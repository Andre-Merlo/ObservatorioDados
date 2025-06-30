import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime

# === CONFIGURAÇÕES DA API ===
URL_ENTES = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt//entes"
URL_RREO = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo"
OUTPUT_DIR = "csv_por_estado"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === FUNÇÃO: Obter lista de entes ===
@st.cache_data(show_spinner="🔍 Carregando entes...")
def obter_entes():
    try:
        response = requests.get(URL_ENTES, timeout=30)
        response.raise_for_status()
        dados = response.json()
        return pd.DataFrame(dados["items"])
    except Exception as e:
        st.error(f"Erro ao obter entes: {e}")
        return pd.DataFrame()


# === FUNÇÃO: Consultar RREO específico ===
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
        if "items" in dados and dados["items"]:
            return pd.DataFrame(dados["items"])
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# === FUNÇÃO: Consulta com fallback RREO Simplificado ===
def consultar_rreo_inteligente(cod_ibge, ano, periodo, esfera, populacao):
    if esfera in ["U", "E", "D"]:
        tipo = "RREO"
        df = consultar_rreo(cod_ibge, ano, periodo, tipo)
        if not df.empty:
            df["tipo_demonstrativo"] = tipo
        return df

    for tipo in ["RREO", "RREO Simplificado"]:
        df = consultar_rreo(cod_ibge, ano, periodo, tipo)
        if not df.empty:
            df["tipo_demonstrativo"] = tipo
            return df

    return pd.DataFrame()


# === EXECUTAR EXTRAÇÃO MUNICIPAL (TODOS OS ESTADOS) COM SALVAMENTO IMEDIATO ===
def executar_extracao_municipios_uf_estado_a_estado(ano, entes_df):
    grupos = entes_df.groupby("uf")

    for uf, grupo in grupos:
        st.markdown(f"### 🟦 Iniciando extração para UF: `{uf}` ({len(grupo)} municípios)")
        resultados = []
        barra = st.progress(0)
        status_area = st.empty()

        total = len(grupo) * 6
        contador = 0

        for _, row in grupo.iterrows():
            cod_ibge = row["cod_ibge"]
            nome_ente = row["ente"]
            esfera_ente = row["esfera"]
            populacao = row.get("populacao", 0) or 0

            for periodo in range(1, 7):
                status_area.write(f"📥 {nome_ente} ({cod_ibge}) - {ano} P{periodo}")
                df = consultar_rreo_inteligente(cod_ibge, ano, periodo, esfera_ente, populacao)

                if not df.empty:
                    df["cod_ibge"] = cod_ibge
                    df["ente"] = nome_ente
                    df["ano"] = ano
                    df["periodo"] = periodo
                    resultados.append(df)

                contador += 1
                barra.progress(contador / total)
                time.sleep(0.2)

        barra.empty()
        status_area.empty()

        if resultados:
            df_concat = pd.concat(resultados, ignore_index=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"RREO_{uf}_M_{ano}_P1a6_{timestamp}.csv"
            caminho_csv = os.path.join(filename)
            df_concat.to_csv(caminho_csv, index=False, sep=";", encoding="utf-8")
            st.success(f"✅ Arquivo salvo: {caminho_csv}")
            csv_bytes = df_concat.to_csv(index=False, sep=";").encode("utf-8")
            st.download_button(
                label=f"📥 Baixar CSV - {uf}",
                data=csv_bytes,
                file_name=filename,
                mime="text/csv",
                key=f"download_{uf}_{timestamp}"  # chave única para cada botão
            )
        else:
            st.warning(f"⚠️ Nenhum dado encontrado para UF {uf}")


# === EXECUTAR EXTRAÇÃO STREAMLIT (TODOS OS MODOS) ===
def executar_extracao_geral(ano, esfera=None, lista_cod_ibge=None, uf_filtro=None):
    entes = obter_entes()

    if lista_cod_ibge:
        entes_filtrados = entes[entes["cod_ibge"].isin(lista_cod_ibge)]
    elif esfera:
        entes_filtrados = entes[entes["esfera"] == esfera]
        if uf_filtro:
            entes_filtrados = entes_filtrados[entes_filtrados["uf"] == uf_filtro]
    else:
        entes_filtrados = entes

    if entes_filtrados.empty:
        st.warning("Nenhum ente encontrado.")
        return {}

    if esfera == "M" and uf_filtro is None:
        executar_extracao_municipios_uf_estado_a_estado(ano, entes_filtrados)
        return {}

    resultados = []
    barra = st.progress(0)
    status_area = st.empty()
    log_texto = ""
    log_area = st.empty()
    total = len(entes_filtrados) * 6
    contador = 0

    for _, row in entes_filtrados.iterrows():
        cod_ibge = row["cod_ibge"]
        nome_ente = row["ente"]
        esfera_ente = row["esfera"]
        populacao = row.get("populacao", 0) or 0

        for periodo in range(1, 7):
            status_area.write(f"📥 {nome_ente} ({cod_ibge}) - {ano} P{periodo}")
            df = consultar_rreo_inteligente(cod_ibge, ano, periodo, esfera_ente, populacao)

            if not df.empty:
                df["cod_ibge"] = cod_ibge
                df["ente"] = nome_ente
                df["ano"] = ano
                df["periodo"] = periodo
                resultados.append(df)
            else:
                log_texto += f"⚠️ Sem dados para {nome_ente} no período {periodo}\n"

            contador += 1
            barra.progress(contador / total)
            time.sleep(0.2)

    log_area.text_area("📜 Log de execução", value=log_texto, height=200, key="log_area_streamlit")
    barra.empty()
    status_area.empty()

    if resultados:
        df_final = pd.concat(resultados, ignore_index=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if esfera == "M":
            nome_uf = uf_filtro or "Todos"
        elif esfera == "E":
            nome_uf = uf_filtro or "Todos"
        else:
            nome_uf = esfera
        filename = f"RREO_{nome_uf}_{esfera}_{ano}_P1a6_{timestamp}.csv"
        return {filename: df_final}
    else:
        return {}


# === INTERFACE STREAMLIT ===
st.set_page_config(page_title="Extrator RREO", layout="wide")
st.title("📊 Extrator RREO - Tesouro Nacional")

st.sidebar.header("Parâmetros da Extração")
ano = st.sidebar.number_input("Ano", min_value=2010, max_value=2100, value=2024)

tipo = st.sidebar.radio(
    "Seleção de entes:",
    ("Estados (E)", "Municípios (M)", "Federal (U)", "Distrito Federal (D)", "Por código IBGE"),
)

uf_escolhida = None
entes_df_temp = obter_entes()
if tipo in ("Municípios (M)", "Estados (E)"):
    esfera_tipo = "M" if "Municípios" in tipo else "E"
    opcoes_uf = ["Todos"] + sorted(pd.unique(entes_df_temp.query(f"esfera == '{esfera_tipo}'")["uf"].dropna()))
    escolha = st.sidebar.selectbox("UF para extração:", opcoes_uf)
    if escolha != "Todos":
        uf_escolhida = escolha

if tipo == "Por código IBGE":
    entrada = st.sidebar.text_input("Códigos IBGE (separados por vírgula)", value="3304557")
    try:
        codigos_ibge = [int(x.strip()) for x in entrada.split(",") if x.strip().isdigit()]
    except ValueError:
        codigos_ibge = []
    esfera = None
else:
    mapa = {"Estados (E)": "E", "Municípios (M)": "M", "Federal (U)": "U", "Distrito Federal (D)": "D"}
    esfera = mapa[tipo]
    codigos_ibge = None

# Rodapé de autoria
st.sidebar.markdown("---")
st.sidebar.markdown("👤 Construído por **André Merlo**")
st.sidebar.markdown("** Versão - V-1.6 **")

if st.sidebar.button("▶️ Iniciar Extração"):
    st.subheader(f"🔎 Consultando dados de {ano}...")

    resultados = executar_extracao_geral(
        ano=ano,
        esfera=esfera,
        lista_cod_ibge=codigos_ibge,
        uf_filtro=uf_escolhida
    )

    if resultados:
        for nome_arquivo, df in resultados.items():
            st.success(f"✅ Dados extraídos - {len(df)} registros.")
            st.dataframe(df.head(20))
            csv = df.to_csv(index=False, sep=";").encode("utf-8")
            st.download_button(
                label="📥 Baixar CSV",
                data=csv,
                file_name=nome_arquivo,
                mime="text/csv"
            )
    else:
        st.info("🗂 Para todos os municípios, os arquivos foram salvos diretamente por estado na pasta local.")
