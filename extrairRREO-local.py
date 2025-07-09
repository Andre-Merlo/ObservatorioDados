import os
import pandas as pd
import requests
import time
from datetime import datetime
import gc
import zipfile

# === CONFIGURAÇÕES ===
URL_ENTES = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt//entes"
URL_RREO = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo"
OUTPUT_DIR = "csv_por_estado"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def obter_entes():
    try:
        response = requests.get(URL_ENTES, timeout=60)
        response.raise_for_status()
        dados = response.json()
        return pd.DataFrame(dados["items"])
    except Exception as e:
        print(f"❌ Erro ao obter entes: {e}")
        return pd.DataFrame()


def consultar_rreo(cod_ibge, ano, periodo, tipo_demonstrativo):
    params = {
        "an_exercicio": ano,
        "nr_periodo": periodo,
        "co_tipo_demonstrativo": tipo_demonstrativo,
        "id_ente": cod_ibge,
    }
    try:
        response = requests.get(URL_RREO, params=params, timeout=60)
        response.raise_for_status()
        dados = response.json()
        return pd.DataFrame(dados["items"]) if "items" in dados and dados["items"] else pd.DataFrame()
    except:
        return pd.DataFrame()


def consultar_rreo_inteligente(cod_ibge, ano, periodo, esfera, populacao):
    if esfera in ["U", "E", "D"]:
        df = consultar_rreo(cod_ibge, ano, periodo, "RREO")
        if not df.empty:
            df["tipo_demonstrativo"] = "RREO"
        return df
    for tipo in ["RREO", "RREO Simplificado"]:
        df = consultar_rreo(cod_ibge, ano, periodo, tipo)
        if not df.empty:
            df["tipo_demonstrativo"] = tipo
            return df
    return pd.DataFrame()


def salvar_csv_zip(df_concat, nome_base):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_csv = f"{nome_base}_{timestamp}.csv"
    caminho_csv = os.path.join(OUTPUT_DIR, nome_csv)
    df_concat.to_csv(caminho_csv, index=False, sep=";", encoding="utf-8")

    nome_zip = nome_csv.replace(".csv", ".zip")
    caminho_zip = os.path.join(OUTPUT_DIR, nome_zip)
    with zipfile.ZipFile(caminho_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(caminho_csv, arcname=nome_csv)

    os.remove(caminho_csv)
    print(f"✅ Arquivo ZIP salvo: {caminho_zip}")


def executar_extracao(ano, entes_df, esfera, uf_nome=None):
    grupos = [("UNICO", entes_df)] if uf_nome is None else [(uf_nome, entes_df)]
    for i, (uf, grupo) in enumerate(grupos):
        print(f"\n🔄 {i + 1}/{len(grupos)} - UF: {uf} ({len(grupo)} entes)")
        resultados = []
        for _, row in grupo.iterrows():
            cod_ibge = row["cod_ibge"]
            nome_ente = row["ente"]
            esfera_ente = row["esfera"]
            populacao = row.get("populacao", 0) or 0
            for periodo in range(1, 7):
                print(f"📥 {nome_ente} ({cod_ibge}) - {ano} P{periodo}")
                df = consultar_rreo_inteligente(cod_ibge, ano, periodo, esfera_ente, populacao)
                if not df.empty:
                    df["cod_ibge"] = cod_ibge
                    df["ente"] = nome_ente
                    df["ano"] = ano
                    df["periodo"] = periodo
                    resultados.append(df)
                time.sleep(0.2)

        if resultados:
            df_concat = pd.concat(resultados, ignore_index=True)
            nome_base = f"RREO_{uf}_{esfera}_{ano}_P1a6"
            salvar_csv_zip(df_concat, nome_base)
            del df_concat
            gc.collect()
        else:
            print(f"⚠️ Nenhum dado encontrado para {uf}")


def mainold():
    print("📊 Extrator de RREO - Tesouro Nacional")
    ano = int(input("Informe o ano de exercício (ex: 2024): ").strip())
    print("Escolha o tipo de ente (E: Estado, M: Município, U: União, D: DF, C: Por código IBGE)")
    tipo = input("Tipo (E/M/U/D/C): ").strip().upper()

    entes = obter_entes()
    if entes.empty or "esfera" not in entes.columns:
        print("❌ Não foi possível carregar os entes.")
        return

    if tipo in ["E", "U", "D"]:
        entes_filtrados = entes[entes["esfera"] == tipo]
        executar_extracao(ano, entes_filtrados, tipo)

    elif tipo == "M":
        ufs_disponiveis = sorted(entes[entes["esfera"] == "M"]["uf"].unique())
        print("UFs disponíveis para municípios:")
        #print(", ".join(ufs_disponiveis))
        #uf_escolhida = input("Informe a UF desejada (ex: RJ, SP): ").strip().upper()
        #entes_filtrados = entes[(entes["esfera"] == "M") & (entes["uf"] == uf_escolhida)]
        #if entes_filtrados.empty:
        #    print(f"⚠️ Nenhum município encontrado para a UF {uf_escolhida}")
        #    return
        #executar_extracao(ano, entes_filtrados, "M", uf_escolhida)

        print ('MA, MS, MT, PA, PB, PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP, TO')
        entes_grupo=['MA', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']
        for uf_escolhida in entes_grupo:
            entes_filtrados = entes[(entes["esfera"] == "M") & (entes["uf"] == uf_escolhida)]
            executar_extracao(ano, entes_filtrados, "M", uf_escolhida)

    elif tipo == "C":
        codigos = input("Informe os códigos IBGE separados por vírgula: ").split(",")
        codigos = [int(c.strip()) for c in codigos if c.strip().isdigit()]
        entes_filtrados = entes[entes["cod_ibge"].isin(codigos)]
        if entes_filtrados.empty:
            print("⚠️ Nenhum código IBGE encontrado.")
            return
        esfera_cod = entes_filtrados.iloc[0]["esfera"]
        executar_extracao(ano, entes_filtrados, esfera_cod, "Selecionado")

    else:
        print("❌ Tipo inválido. Use E, M, U, D ou C.")


def mainEsfera_E_U_D():
    #Main para extrair todos os anos. Com exceção de Municipal
    print("📊 Extrator de RREO - Tesouro Nacional")
    anoinicial = 2016
    anofinal = 2025
    print("Rodando Estado, União, DF - Todos os anos.")

    entes = obter_entes()
    if entes.empty or "esfera" not in entes.columns:
        print("❌ Não foi possível carregar os entes.")
        return

    for ano in range(anoinicial, anofinal):
        for tipo in ["E", "U", "D"]:
            entes_filtrados = entes[entes["esfera"] == tipo]
            executar_extracao(ano, entes_filtrados, tipo)

def main():
    print("📊 Extrator de RREO - Tesouro Nacional")
    anoinicial = 2020
    anofinal = 2022
    print("Rodando Municipios, por estado - Todos os anos.")

    entes = obter_entes()
    if entes.empty or "esfera" not in entes.columns:
        print("❌ Não foi possível carregar os entes.")
        return
    tipo = "M"
    for ano in range(anoinicial, anofinal):
        print(f"\n🔄Ano {ano}")
        ufs_disponiveis = sorted(entes[entes["esfera"] == "M"]["uf"].unique())
        print("UFs disponíveis para municípios:")
        print(", ".join(ufs_disponiveis))
        for uf_escolhida in ufs_disponiveis:
            entes_filtrados = entes[(entes["esfera"] == "M") & (entes["uf"] == uf_escolhida)]
            executar_extracao(ano, entes_filtrados, "M", uf_escolhida)



if __name__ == "__main__":
    main()
