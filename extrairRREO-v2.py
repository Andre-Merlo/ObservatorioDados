import requests
import pandas as pd
import os
import time

# === CONFIGURAÇÃO ===
URL_ENTES = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt//entes"
URL_RREO = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo"

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# === FUNÇÃO: Obter lista de entes ===
def obter_entes():
    print("🔍 Obtendo lista de entes...")
    response = requests.get(URL_ENTES)
    if response.status_code == 200:
        dados = response.json()
        df = pd.DataFrame(dados['items'])
        print(f"✅ {len(df)} entes obtidos.")
        return df
    else:
        raise Exception(f"❌ Erro ao obter entes: {response.status_code}")


# === FUNÇÃO: Filtrar entes ===
def filtrar_entes(df_entes, esfera=None, lista_cod_ibge=None):
    if lista_cod_ibge:
        return df_entes[df_entes['cod_ibge'].isin(lista_cod_ibge)]
    elif esfera:
        return df_entes[df_entes['esfera'] == esfera]
    else:
        return df_entes


# === FUNÇÃO: Consultar RREO ===
def consultar_rreo(cod_ibge, ano, periodo):
    params = {
        "an_exercicio": ano,
        "nr_periodo": periodo,
        "co_tipo_demonstrativo": "RREO",
        "id_ente": cod_ibge
    }
    response = requests.get(URL_RREO, params=params)
    if response.status_code == 200:
        dados = response.json()
        if 'items' in dados:
            return pd.DataFrame(dados['items'])
        else:
            return pd.DataFrame()
    else:
        print(f"⚠️ Erro {response.status_code} para ente {cod_ibge}")
        return pd.DataFrame()


# === FUNÇÃO PRINCIPAL ===
def executar_extracao(ano, esfera=None, lista_cod_ibge=None):
    entes = obter_entes()
    entes_selecionados = filtrar_entes(entes, esfera, lista_cod_ibge)

    print(f"🔢 {len(entes_selecionados)} entes selecionados para extração.\n")

    dfs = []

    for _, row in entes_selecionados.iterrows():
        cod_ibge = row['cod_ibge']
        nome_ente = row['ente']

        for periodo in range(1, 7):  # Bimestres de 1 a 6
            print(f"⬇️ Consultando RREO {ano} - Período {periodo} para {nome_ente} ({cod_ibge})...")

            df_rreo = consultar_rreo(cod_ibge, ano, periodo)

            if not df_rreo.empty:
                df_rreo['cod_ibge'] = cod_ibge
                df_rreo['ente'] = nome_ente
                df_rreo['ano'] = ano
                df_rreo['periodo'] = periodo
                dfs.append(df_rreo)
            else:
                print(f"⚠️ Sem dados para {nome_ente} ({cod_ibge}) no período {periodo}")

            time.sleep(0.5)  # evitar sobrecarga na API

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        nome_arquivo = f"RREO_{esfera or 'personalizado'}_{ano}_P1a6.csv"
        caminho = os.path.join(OUTPUT_DIR, nome_arquivo)
        df_final.to_csv(caminho, index=False, sep=";", encoding="utf-8")
        print(f"\n✅ Arquivo gerado com sucesso: {caminho}")
        return df_final
    else:
        print("\n❌ Nenhum dado encontrado.")
        return None


# === INTERFACE INTERATIVA ===
def menu_interativo():
    print("=== 🔗 Extrator RREO Tesouro Nacional ===")
    ano = input("Digite o ano (ex: 2024): ").strip()

    if not ano.isdigit() or len(ano) != 4:
        print("❌ Ano inválido. Informe um ano com 4 dígitos.")
        return

    ano = int(ano)

    print("\nEscolha uma opção de extração:")
    print("1 - Todos os Estados (E)")
    print("2 - Todos os Municípios (M)")
    print("3 - Ente Federal (U)")
    print("4 - Distrito Federal (D)")
    print("5 - Específico por código IBGE")
    opcao = input("Digite a opção desejada (1-5): ").strip()

    if opcao in ["1", "2", "3", "4"]:
        mapa_opcoes = {"1": "E", "2": "M", "3": "U", "4": "D"}
        esfera = mapa_opcoes[opcao]
        executar_extracao(ano=ano, esfera=esfera)

    elif opcao == "5":
        codigos = input(
            "Digite um ou mais códigos IBGE separados por vírgula: "
        ).strip()
        lista_cod_ibge = [int(c.strip()) for c in codigos.split(",")]
        executar_extracao(ano=ano, lista_cod_ibge=lista_cod_ibge)

    else:
        print("❌ Opção inválida. Tente novamente.")


# === EXECUTAR MENU ===
if __name__ == "__main__":
    menu_interativo()
