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
def consultar_rreo(cod_ibge, ano=2024, periodo=1):
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
def executar_extracao(ano=2024, periodo=1, esfera=None, lista_cod_ibge=None):
    entes = obter_entes()
    entes_selecionados = filtrar_entes(entes, esfera, lista_cod_ibge)

    print(f"🔢 {len(entes_selecionados)} entes selecionados para extração.")

    dfs = []
    for _, row in entes_selecionados.iterrows():
        cod_ibge = row['cod_ibge']
        nome_ente = row['ente']
        print(f"⬇️ Consultando RREO para {nome_ente} ({cod_ibge})...")

        df_rreo = consultar_rreo(cod_ibge, ano, periodo)

        if not df_rreo.empty:
            df_rreo['cod_ibge'] = cod_ibge
            df_rreo['ente'] = nome_ente
            dfs.append(df_rreo)
        else:
            print(f"⚠️ Sem dados para {nome_ente} ({cod_ibge})")

        time.sleep(0.5)  # evitar sobrecarga na API

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        nome_arquivo = f"RREO_{esfera or 'personalizado'}_{ano}_P{periodo}.csv"
        caminho = os.path.join(OUTPUT_DIR, nome_arquivo)
        df_final.to_csv(caminho, index=False, sep=";", encoding="utf-8")
        print(f"✅ Arquivo gerado: {caminho}")
        return df_final
    else:
        print("❌ Nenhum dado encontrado.")
        return None


# === EXEMPLOS DE USO ===

# 🔹 Todos os Estados
# executar_extracao(esfera='E')

# 🔹 Todos os Municípios
# executar_extracao(esfera='M')

# 🔹 Ente Federal
# executar_extracao(esfera='U')

# 🔹 Distrito Federal
# executar_extracao(esfera='D')

# 🔹 Específico por código IBGE (exemplo: Rio de Janeiro - 3304557)
# executar_extracao(lista_cod_ibge=[3304557])

# === INTERFACE INTERATIVA ===
def menu_interativo():
    print("=== 🔗 Extrator RREO Tesouro Nacional ===")
    print("Escolha uma opção de extração:")
    print("1 - Todos os Estados (E)")
    print("2 - Todos os Municípios (M)")
    print("3 - Ente Federal (U)")
    print("4 - Distrito Federal (D)")
    print("5 - Específico por código IBGE")
    opcao = input("Digite a opção desejada (1-5): ").strip()

    if opcao in ["1", "2", "3", "4"]:
        mapa_opcoes = {"1": "E", "2": "M", "3": "U", "4": "D"}
        esfera = mapa_opcoes[opcao]
        executar_extracao(esfera=esfera)

    elif opcao == "5":
        codigos = input(
            "Digite um ou mais códigos IBGE separados por vírgula: "
        ).strip()
        lista_cod_ibge = [int(c.strip()) for c in codigos.split(",")]
        executar_extracao(lista_cod_ibge=lista_cod_ibge)

    else:
        print("❌ Opção inválida. Tente novamente.")


# === EXECUTAR MENU ===
if __name__ == "__main__":
    menu_interativo()