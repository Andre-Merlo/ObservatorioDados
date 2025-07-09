import os
import pandas as pd
import requests
import time
import zipfile
from datetime import datetime

# === CONFIGURAÇÕES ===
URL_ENTES = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes"
URL_RGF = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rgf"
OUTPUT_DIR = "csv_rgf_por_estado"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === LISTAS DE VALORES FIXOS ===
periodicidades = ["Q", "S"]
tipos_demo = ["RGF", "RGF Simplificado"]
poderes = ["E", "L", "J", "M", "D"]

def obter_entes_municipais_por_uf():
    try:
        response = requests.get(URL_ENTES, timeout=60)
        response.raise_for_status()
        dados = response.json()
        df = pd.DataFrame(dados["items"])
        return df[df["esfera"] == "M"].groupby("uf")
    except Exception as e:
        print(f"❌ Erro ao obter entes municipais: {e}")
        return []

def consultar_rgf(cod_ibge, ano, periodicidade, periodo, tipo_demo, poder):
    params = {
        "id_ente": cod_ibge,
        "an_exercicio": ano,
        "in_periodicidade": periodicidade,
        "nr_periodo": periodo,
        "co_tipo_demonstrativo": tipo_demo,
        "co_poder": poder,
        "co_esfera": "M"
    }
    try:
        response = requests.get(URL_RGF, params=params, timeout=60)
        response.raise_for_status()
        dados = response.json()
        return pd.DataFrame(dados["items"]) if "items" in dados and dados["items"] else pd.DataFrame()
    except:
        return pd.DataFrame()

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

def extrair_por_estado(ano):
    grupos_uf = obter_entes_municipais_por_uf()
    for uf, entes_df in grupos_uf:
        print(f"\n🔍 Iniciando extração para UF: {uf} ({len(entes_df)} municípios)")
        resultados = []
        total_consultas = len(entes_df) * len(poderes) * len(periodicidades) * 3
        progresso = 0

        for _, row in entes_df.iterrows():
            cod_ibge = row["cod_ibge"]
            nome_ente = row["ente"]

            for poder in poderes:
                for periodicidade in periodicidades:
                    max_periodo = 3 if periodicidade == "Q" else 2
                    for periodo in range(1, max_periodo + 1):
                        progresso += 1
                        tipo_encontrado = None
                        for tipo_demo in tipos_demo:
                            df = consultar_rgf(cod_ibge, ano, periodicidade, periodo, tipo_demo, poder)
                            if not df.empty:
                                tipo_encontrado = tipo_demo
                                break

                        print(f"[{progresso}/{total_consultas}] 📥 {nome_ente} ({cod_ibge}) - {ano} {uf} {poder} {periodicidade} P{periodo} - {tipo_encontrado or '❌ nenhum'}")

                        if tipo_encontrado:
                            df["cod_ibge"] = cod_ibge
                            df["ente"] = nome_ente
                            df["ano"] = ano
                            df["uf"] = uf
                            df["periodicidade"] = periodicidade
                            df["periodo"] = periodo
                            df["tipo_demo"] = tipo_encontrado
                            df["poder"] = poder
                            resultados.append(df)
                        time.sleep(0.2)

        if resultados:
            df_concat = pd.concat(resultados, ignore_index=True)
            nome_base = f"RGF_M_{uf}_{ano}_completo"
            salvar_csv_zip(df_concat, nome_base)
        else:
            print(f"⚠️ Nenhum dado encontrado para UF {uf}")

def main():
    print("📊 Extração RGF Municípios agrupados por Estado")

    try:
        ano = int(input("Informe o ano de exercício (ex: 2024): ").strip())
    except ValueError:
        print("❌ Ano inválido.")
        return

    extrair_por_estado(ano)
    print("✅ Extração finalizada por estado.")

if __name__ == "__main__":
    main()
