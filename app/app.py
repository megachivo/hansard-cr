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

# --- Tab 4: Preguntar (Agente de conocimiento, multi-turno con citas) -------
with tabs[3]:
    st.subheader("💬 Agente del Plenario")
    st.caption(
        "Chat multi-turno con citas obligatorias. Cada respuesta se construye "
        "desde texto real recuperado del Plenario."
    )

    # ------------------------------------------------------------------
    # Estado de la conversación
    # ------------------------------------------------------------------
    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []  # [{role, content, sources?}]

    col_clear, col_sug = st.columns([1, 4])
    with col_clear:
        if st.button("🧹 Limpiar chat", use_container_width=True):
            st.session_state.agent_messages = []
            st.rerun()
    with col_sug:
        st.caption(
            "Sugerencias: *¿Qué se dijo sobre la CCSS?* · "
            "*Comparame las posturas sobre seguridad* · "
            "*Resumime la última sesión*"
        )

    # ------------------------------------------------------------------
    # Render del histórico
    # ------------------------------------------------------------------
    for msg in st.session_state.agent_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sources"):
                with st.expander(f"📚 Fuentes ({len(msg['sources'])})"):
                    for i, c in enumerate(msg["sources"]):
                        st.markdown(
                            f"**[{i+1}] {c['diputado']}**"
                            + (f" *({c['fraccion']})*" if c.get("fraccion") else "")
                            + f" — {c['fecha']} — sesión `{c['session_id']}`"
                        )
                        st.write(c["texto"])
                        link_cols = st.columns(2)
                        if c.get("video_url") and c.get("start_sec") is not None:
                            link_cols[0].markdown(
                                f"[▶ Video min {int(c['start_sec']) // 60}]"
                                f"({c['video_url']}&t={int(c['start_sec'])})"
                            )
                        if c.get("pdf_url"):
                            link_cols[1].markdown(f"[📄 Acta]({c['pdf_url']})")
                        st.divider()

    # ------------------------------------------------------------------
    # Helpers del agente
    # ------------------------------------------------------------------
    def _reescribir_query(historial: list[dict], pregunta: str) -> str:
        """Convierte una pregunta de seguimiento + historial en una query
        de búsqueda independiente (standalone). Si es la primera pregunta,
        la devuelve tal cual."""
        if not historial:
            return pregunta
        previo = "\n".join(
            f"{m['role']}: {m['content'][:400]}" for m in historial[-4:]
        )
        prompt = (
            "Dado el historial de chat y una nueva pregunta de seguimiento, "
            "reescribí la nueva pregunta como una consulta autocontenida en "
            "español, lista para búsqueda semántica. No respondas la pregunta; "
            "solo devolvé la consulta. Si la pregunta ya es autocontenida, "
            "devolvela igual.\n\n"
            f"HISTORIAL:\n{previo}\n\n"
            f"NUEVA PREGUNTA: {pregunta}\n\nCONSULTA:"
        )
        try:
            resp = get_llm().chat.completions.create(
                model=LLM_ENDPOINT,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=120,
                temperature=0.0,
            )
            rewritten = resp.choices[0].message.content.strip().strip('"')
            return rewritten or pregunta
        except Exception:
            return pregunta

    def _construir_prompt_agente(
        historial: list[dict], pregunta: str, contexto: list[dict]
    ) -> list[dict]:
        ctx_texto = "\n\n".join(
            f"[{i+1}] {c['diputado']}"
            + (f" ({c['fraccion']})" if c.get("fraccion") else "")
            + f" — {c['fecha']} — sesión {c['session_id']}:\n{c['texto']}"
            for i, c in enumerate(contexto)
        )
        system = (
            "Sos un analista del Plenario de la Asamblea Legislativa de Costa "
            "Rica. Respondés solo con base en el CONTEXTO provisto. "
            "Toda afirmación factual debe ir acompañada de citas en formato "
            "[n] referidas al número de fuente. Si el contexto no alcanza, "
            "decilo explícitamente y no inventes. Tono neutral, claro y "
            "conciso. Si la pregunta pide comparar posturas, estructurá la "
            "respuesta por diputado o por fracción."
        )
        mensajes = [{"role": "system", "content": system}]
        for m in historial[-6:]:  # ventana corta para no inflar el prompt
            mensajes.append({"role": m["role"], "content": m["content"]})
        mensajes.append(
            {
                "role": "user",
                "content": (
                    f"CONTEXTO RECUPERADO:\n{ctx_texto}\n\n"
                    f"PREGUNTA: {pregunta}\n\n"
                    "Respondé con citas [n] obligatorias."
                ),
            }
        )
        return mensajes

    # ------------------------------------------------------------------
    # Input del usuario
    # ------------------------------------------------------------------
    user_input = st.chat_input("Preguntale al Plenario...")
    if user_input:
        # 1. Pintar el mensaje del usuario inmediatamente
        st.session_state.agent_messages.append(
            {"role": "user", "content": user_input}
        )
        with st.chat_message("user"):
            st.markdown(user_input)

        # 2. Pipeline del agente: rewrite → retrieve → generate
        with st.chat_message("assistant"):
            historial_previo = st.session_state.agent_messages[:-1]

            with st.spinner("Reformulando la consulta..."):
                query = _reescribir_query(historial_previo, user_input)
            if query != user_input:
                st.caption(f"🔍 Buscando: *{query}*")

            with st.spinner("Buscando en el Plenario..."):
                contexto = buscar(query, k=8)

            if not contexto:
                respuesta = (
                    "No encontré nada en el Plenario que responda a esa "
                    "pregunta. Probá reformularla o buscar por un tema más "
                    "específico (CCSS, jornada 4x3, seguridad, etc.)."
                )
                st.markdown(respuesta)
                st.session_state.agent_messages.append(
                    {"role": "assistant", "content": respuesta, "sources": []}
                )
            else:
                mensajes = _construir_prompt_agente(
                    historial_previo, user_input, contexto
                )
                with st.spinner("Pensando..."):
                    resp = get_llm().chat.completions.create(
                        model=LLM_ENDPOINT,
                        messages=mensajes,
                        max_tokens=900,
                        temperature=0.2,
                    )
                respuesta = resp.choices[0].message.content
                st.markdown(respuesta)
                with st.expander(f"📚 Fuentes ({len(contexto)})"):
                    for i, c in enumerate(contexto):
                        st.markdown(
                            f"**[{i+1}] {c['diputado']}**"
                            + (f" *({c['fraccion']})*" if c.get("fraccion") else "")
                            + f" — {c['fecha']} — sesión `{c['session_id']}`"
                        )
                        st.write(c["texto"])
                        link_cols = st.columns(2)
                        if c.get("video_url") and c.get("start_sec") is not None:
                            link_cols[0].markdown(
                                f"[▶ Video min {int(c['start_sec']) // 60}]"
                                f"({c['video_url']}&t={int(c['start_sec'])})"
                            )
                        if c.get("pdf_url"):
                            link_cols[1].markdown(f"[📄 Acta]({c['pdf_url']})")
                        st.divider()

                st.session_state.agent_messages.append(
                    {
                        "role": "assistant",
                        "content": respuesta,
                        "sources": contexto,
                    }
                )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.sidebar.markdown("### Hansard CR")
st.sidebar.caption(
    "Datos del Plenario de la Asamblea Legislativa de Costa Rica. "
    "Fuentes: actas oficiales (asamblea.go.cr) y canal de YouTube @AsambleaCRC."
)
st.sidebar.caption("Hackathon 2026 · Construido sobre Databricks.")
