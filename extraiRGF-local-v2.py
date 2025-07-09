import os
import pandas as pd
import requests
import time
import zipfile
from tqdm import tqdm
from datetime import datetime

# === CONFIGURAÇÕES ===
URL_ENTES = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes"
URL_RGF = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rgf"
OUTPUT_DIR = "csv_rgf_por_ente"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === LISTAS DE VALORES FIXOS ===
esferas = ["M", "E", "U", "C"]
periodicidades = ["Q", "S"]
tipos_demo = ["RGF", "RGF Simplificado"]
poderes = ["E", "L", "J", "M", "D"]

def obter_entes_por_esfera(esfera):
    try:
        response = requests.get(URL_ENTES, timeout=60)
        response.raise_for_status()
        dados = response.json()
        df = pd.DataFrame(dados["items"])
        return df[df["esfera"] == esfera]
    except Exception as e:
        print(f"❌ Erro ao obter entes ({esfera}): {e}")
        return pd.DataFrame()

def consultar_rgf(cod_ibge, ano, periodicidade, periodo, tipo_demo, poder, esfera, anexo=None):
    params = {
        "id_ente": cod_ibge,
        "an_exercicio": ano,
        "in_periodicidade": periodicidade,
        "nr_periodo": periodo,
        "co_tipo_demonstrativo": tipo_demo,
        "co_poder": poder,
        "co_esfera": esfera
    }
    if anexo:
        params["no_anexo"] = anexo

    try:
        response = requests.get(URL_RGF, params=params, timeout=60)
        response.raise_for_status()
        dados = response.json()
        return pd.DataFrame(dados["items"]) if "items" in dados and dados["items"] else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def salvar_csv_zip(df, nome_base):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_csv = f"{nome_base}_{timestamp}.csv"
    caminho_csv = os.path.join(OUTPUT_DIR, nome_csv)
    df.to_csv(caminho_csv, index=False, sep=";", encoding="utf-8")

    nome_zip = nome_csv.replace(".csv", ".zip")
    caminho_zip = os.path.join(OUTPUT_DIR, nome_zip)
    with zipfile.ZipFile(caminho_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(caminho_csv, arcname=nome_csv)

    os.remove(caminho_csv)
    print(f"✅ Arquivo salvo: {caminho_zip}")

def salvar_log_falhas(logs, esfera, uf=None):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    uf_part = f"_{uf}" if uf else ""
    nome_log = f"log_falhas_RGF_{esfera}{uf_part}_{timestamp}.txt"
    caminho_log = os.path.join(OUTPUT_DIR, nome_log)
    with open(caminho_log, "w", encoding="utf-8") as f:
        f.write("\n".join(logs))
    print(f"📄 Log de falhas salvo: {caminho_log}")

def poderes_por_esfera(esfera):
    if esfera == "M":
        return ["E", "L"]  # municípios geralmente têm só executivo e legislativo
    elif esfera == "E":
        return ["E", "L", "J", "M", "D"]
    elif esfera == "U":
        return ["E", "L", "J", "M", "D"]
    elif esfera == "C":  # consórcios públicos normalmente só executivo
        return ["E"]
    else:
        return ["E"]  # fallback

def extrair_para_esfera(ano, esfera, uf_filtro=None):
    entes_df = obter_entes_por_esfera(esfera)
    if entes_df.empty:
        print(f"⚠️ Nenhum ente encontrado para esfera {esfera}")
        return

    lista_poderes = poderes_por_esfera(esfera)

    if uf_filtro and esfera == "M":
        entes_df = entes_df[entes_df["uf"] == uf_filtro]

    agrupamento = entes_df.groupby("uf") if esfera == "M" else [("UNICO", entes_df)]

    for uf, grupo in agrupamento:
        resultados = []
        log_falhas = []

        total_consultas = len(grupo) * len(lista_poderes) * len(periodicidades) * 3
        barra = tqdm(total=total_consultas, desc=f"Processando {esfera} - {uf}", unit="req")

        for _, row in grupo.iterrows():
            cod_ibge = row["cod_ibge"]
            nome_ente = row["ente"]

            for poder in lista_poderes:
                for periodicidade in periodicidades:
                    max_periodo = 3 if periodicidade == "Q" else 2
                    for periodo in range(1, max_periodo + 1):
                        tipo_encontrado = None
                        for tipo_demo in tipos_demo:
                            df = consultar_rgf(cod_ibge, ano, periodicidade, periodo, tipo_demo, poder, esfera)
                            if not df.empty:
                                tipo_encontrado = tipo_demo
                                break

                        if tipo_encontrado:
                            df["cod_ibge"] = cod_ibge
                            df["ente"] = nome_ente
                            df["ano"] = ano
                            df["esfera"] = esfera
                            df["periodicidade"] = periodicidade
                            df["periodo"] = periodo
                            df["tipo_demo"] = tipo_encontrado
                            df["poder"] = poder
                            resultados.append(df)
                        else:
                            log_falhas.append(f"{cod_ibge} - {nome_ente} - {esfera} {poder} {periodicidade} P{periodo}")

                        barra.update(1)
                        time.sleep(0.2)

        barra.close()

        if resultados:
            df_concat = pd.concat(resultados, ignore_index=True)
            nome_base = f"RGF_{esfera}_{uf}_{ano}_completo" if esfera == "M" else f"RGF_{esfera}_{ano}_completo"
            salvar_csv_zip(df_concat, nome_base)
        else:
            print(f"⚠️ Nenhum dado encontrado para {uf} ({esfera})")

        if log_falhas:
            salvar_log_falhas(log_falhas, esfera, uf if esfera == "M" else None)

def main():
    print("📊 Extração COMPLETA RGF - Todas as esferas/poderes/tipos")

    try:
        ano = int(input("Informe o ano de exercício (ex: 2024): ").strip())
    except ValueError:
        print("❌ Ano inválido.")
        return

    print("Deseja filtrar por uma única UF para municípios? (S/N): ", end="")
    filtro = input().strip().upper()
    uf_filtro = None
    if filtro == "S":
        df_municipios = obter_entes_por_esfera("M")
        ufs_disponiveis = sorted(df_municipios["uf"].dropna().unique())
        print("UFs disponíveis:", ", ".join(ufs_disponiveis))
        uf_filtro = input("Informe a sigla da UF desejada (ex: RJ): ").strip().upper()
        if uf_filtro not in ufs_disponiveis:
            print("❌ UF inválida.")
            return

    for esfera in esferas:
        print(f"\n🔍 Iniciando extração para esfera: {esfera}")
        extrair_para_esfera(ano, esfera, uf_filtro if esfera == "M" else None)

    print("✅ Extração RGF finalizada para todas as esferas.")

if __name__ == "__main__":
    main()
