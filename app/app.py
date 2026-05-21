"""
Hansard CR — Databricks App
============================
Buscador semántico y RAG sobre el Plenario de la Asamblea Legislativa de Costa Rica.

Estructura:
- Tab "Buscar":   búsqueda híbrida vía Vector Search
- Tab "Sesión":   ver una sesión con su video y transcripción
- Tab "Diputado": stats agregados por diputado
- Tab "Preguntar": chat RAG con citas obligatorias

Variables de entorno (configurar en app.yaml):
- VECTOR_SEARCH_ENDPOINT
- VECTOR_INDEX_NAME
- LLM_ENDPOINT
- CATALOG
"""

import os
import streamlit as st
import pandas as pd
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
from openai import OpenAI

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

ENDPOINT = os.environ.get("VECTOR_SEARCH_ENDPOINT", "hansard-cr-endpoint")
INDEX = os.environ.get("VECTOR_INDEX_NAME", "hansard_cr.gold.intervenciones_idx")
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
CATALOG = os.environ.get("CATALOG", "hansard_cr")

st.set_page_config(
    page_title="Hansard CR",
    page_icon="🏛️",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Clientes (cacheados)
# ---------------------------------------------------------------------------

@st.cache_resource
def get_vsc():
    return VectorSearchClient(disable_notice=True)

@st.cache_resource
def get_index():
    return get_vsc().get_index(endpoint_name=ENDPOINT, index_name=INDEX)

@st.cache_resource
def get_llm():
    w = WorkspaceClient()
    # En Databricks Apps, el SDK toma credenciales automáticamente
    return OpenAI(
        api_key=w.config.token or os.environ.get("DATABRICKS_TOKEN"),
        base_url=f"{w.config.host}/serving-endpoints",
    )

@st.cache_resource
def get_spark():
    # Streamlit corriendo en Databricks Apps puede usar databricks-sql-connector
    # para queries SQL contra el warehouse
    from databricks import sql
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ.get("DATABRICKS_TOKEN"),
    )

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def buscar(query: str, k: int = 10, filtros: dict | None = None) -> list[dict]:
    """Devuelve top-k intervenciones similares a `query`."""
    idx = get_index()
    kwargs = dict(
        query_text=query,
        columns=[
            "intervencion_id", "session_id", "fecha", "diputado",
            "fraccion", "texto", "video_url", "start_sec", "pdf_url", "fuente",
        ],
        num_results=k,
    )
    if filtros:
        kwargs["filters"] = filtros
    res = idx.similarity_search(**kwargs)
    cols = [c["name"] for c in res["manifest"]["columns"]]
    return [dict(zip(cols, row)) for row in res["result"]["data_array"]]

def render_intervencion(item: dict):
    """Renderiza una intervención con link al video/PDF."""
    encabezado = f"**{item['diputado']}**"
    if item.get("fraccion"):
        encabezado += f" *({item['fraccion']})*"
    encabezado += f" — {item['fecha']} — sesión `{item['session_id']}`"
    st.markdown(encabezado)
    st.write(item["texto"])

    cols = st.columns(3)
    if item.get("video_url") and item.get("start_sec") is not None:
        url_t = f"{item['video_url']}&t={int(item['start_sec'])}"
        cols[0].markdown(f"[▶ Ver video min {int(item['start_sec']) // 60}]({url_t})")
    if item.get("pdf_url"):
        cols[1].markdown(f"[📄 Acta oficial]({item['pdf_url']})")
    cols[2].caption(f"Fuente: {item.get('fuente', '?')}")
    st.divider()

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title("🏛️ Hansard CR")
st.caption("Buscador del Plenario de la Asamblea Legislativa de Costa Rica")

tabs = st.tabs(["🔎 Buscar", "📺 Sesión", "👤 Diputado", "💬 Preguntar"])

# --- Tab 1: Buscar -----------------------------------------------------------
with tabs[0]:
    q = st.text_input(
        "Buscá lo que se dijo en el Plenario",
        placeholder="ej. jornada 4x3, CCSS, seguridad ciudadana...",
    )
    col1, col2 = st.columns([3, 1])
    with col2:
        solo_video = st.checkbox("Solo del video reciente", value=False)
    if q:
        filtros = {"fuente": "video"} if solo_video else None
        with st.spinner("Buscando..."):
            resultados = buscar(q, k=10, filtros=filtros)
        if not resultados:
            st.info("Sin resultados. Probá otra búsqueda.")
        else:
            st.success(f"{len(resultados)} resultados")
            for r in resultados:
                render_intervencion(r)

# --- Tab 2: Sesión -----------------------------------------------------------
with tabs[1]:
    st.subheader("Ver una sesión")
    # Lista de sesiones disponibles
    conn = get_spark()
    sesiones = pd.read_sql(f"""
        SELECT DISTINCT session_id, fecha, video_url, fuente
        FROM {CATALOG}.silver.intervenciones
        ORDER BY fecha DESC
        LIMIT 50
    """, conn)
    if sesiones.empty:
        st.warning("No hay sesiones cargadas todavía.")
    else:
        sel = st.selectbox(
            "Sesión",
            options=sesiones["session_id"].tolist(),
            format_func=lambda s: f"{s} — {sesiones[sesiones.session_id==s].iloc[0]['fecha']}",
        )
        row = sesiones[sesiones.session_id == sel].iloc[0]

        col_v, col_t = st.columns([1, 1])
        with col_v:
            if row["video_url"]:
                st.video(row["video_url"])
            else:
                st.info("Sin video para esta sesión (solo acta).")
        with col_t:
            if st.button("🤖 Resumir esta sesión"):
                contenido = pd.read_sql(f"""
                    SELECT diputado, texto FROM {CATALOG}.silver.intervenciones
                    WHERE session_id = '{sel}' LIMIT 30
                """, conn)
                prompt = (
                    "Resumí esta sesión del Plenario en 5 bullets. Cada bullet con el "
                    "nombre del diputado entre paréntesis cuando aplique. Sé conciso.\n\n"
                    + "\n".join(f"- {r['diputado']}: {r['texto'][:300]}"
                                for _, r in contenido.iterrows())
                )
                resp = get_llm().chat.completions.create(
                    model=LLM_ENDPOINT,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=600,
                )
                st.write(resp.choices[0].message.content)

        st.markdown("### Intervenciones")
        intervenciones = pd.read_sql(f"""
            SELECT diputado, texto, start_sec FROM {CATALOG}.silver.intervenciones
            WHERE session_id = '{sel}' ORDER BY orden LIMIT 100
        """, conn)
        for _, i in intervenciones.iterrows():
            with st.expander(f"{i['diputado']} — {i['texto'][:80]}..."):
                st.write(i["texto"])
                if pd.notna(i["start_sec"]):
                    st.caption(f"Min {int(i['start_sec']) // 60}:{int(i['start_sec']) % 60:02d}")

# --- Tab 3: Diputado ---------------------------------------------------------
with tabs[2]:
    st.subheader("Perfil de un diputado")
    conn = get_spark()
    diputados = pd.read_sql(f"""
        SELECT diputado, COUNT(*) AS intervenciones,
               SUM(LENGTH(texto)) AS chars_total
        FROM {CATALOG}.silver.intervenciones
        WHERE diputado IS NOT NULL AND diputado NOT LIKE '%sin identificar%'
        GROUP BY diputado
        HAVING COUNT(*) > 1
        ORDER BY intervenciones DESC
    """, conn)
    if diputados.empty:
        st.warning("No hay datos de diputados todavía.")
    else:
        dip_sel = st.selectbox("Diputado", diputados["diputado"].tolist())
        stats = diputados[diputados.diputado == dip_sel].iloc[0]
        c1, c2 = st.columns(2)
        c1.metric("Intervenciones", int(stats["intervenciones"]))
        c2.metric("Caracteres totales", f"{int(stats['chars_total']):,}")

        st.markdown("#### Comparativa con el resto")
        st.bar_chart(diputados.head(15).set_index("diputado")["intervenciones"])

        st.markdown("#### Últimas intervenciones")
        ultimas = pd.read_sql(f"""
            SELECT fecha, session_id, texto FROM {CATALOG}.silver.intervenciones
            WHERE diputado = '{dip_sel}'
            ORDER BY fecha DESC LIMIT 5
        """, conn)
        for _, u in ultimas.iterrows():
            with st.expander(f"{u['fecha']} — {u['session_id']}"):
                st.write(u["texto"])

# --- Tab 4: Preguntar (RAG con citas) ---------------------------------------
with tabs[3]:
    st.subheader("💬 Preguntale al Plenario")
    st.caption("Respondemos con texto real del Plenario. Cada respuesta cita su fuente.")

    pregunta = st.text_area(
        "Tu pregunta",
        placeholder="ej. ¿Qué se ha dicho sobre la jornada 4x3?",
        height=100,
    )

    if st.button("Preguntar", type="primary") and pregunta:
        with st.spinner("Buscando contexto..."):
            contexto = buscar(pregunta, k=8)

        if not contexto:
            st.error("No encontré contexto suficiente.")
        else:
            # Construir prompt con citas numeradas
            ctx_texto = "\n\n".join(
                f"[{i+1}] {c['diputado']} ({c['fecha']}, sesión {c['session_id']}):\n{c['texto']}"
                for i, c in enumerate(contexto)
            )
            prompt = f"""Eres un analista del Plenario de la Asamblea Legislativa de Costa Rica.
Responde la pregunta usando SOLO la información del contexto.
Cita las fuentes con [n] donde n es el número de la cita.
Si el contexto no alcanza, dilo explícitamente y no inventes.

CONTEXTO:
{ctx_texto}

PREGUNTA: {pregunta}

RESPUESTA (con citas [n] obligatorias):"""

            with st.spinner("Pensando..."):
                resp = get_llm().chat.completions.create(
                    model=LLM_ENDPOINT,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=800,
                    temperature=0.2,
                )
            respuesta = resp.choices[0].message.content
            st.markdown("### Respuesta")
            st.write(respuesta)

            st.markdown("### Fuentes")
            for i, c in enumerate(contexto):
                with st.expander(f"[{i+1}] {c['diputado']} — {c['fecha']}"):
                    st.write(c["texto"])
                    cols = st.columns(2)
                    if c.get("video_url") and c.get("start_sec") is not None:
                        cols[0].markdown(
                            f"[▶ Video]({c['video_url']}&t={int(c['start_sec'])})"
                        )
                    if c.get("pdf_url"):
                        cols[1].markdown(f"[📄 Acta]({c['pdf_url']})")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.sidebar.markdown("### Hansard CR")
st.sidebar.caption(
    "Datos del Plenario de la Asamblea Legislativa de Costa Rica. "
    "Fuentes: actas oficiales (asamblea.go.cr) y canal de YouTube @AsambleaCRC."
)
st.sidebar.caption("Hackathon 2026 · Construido sobre Databricks.")
