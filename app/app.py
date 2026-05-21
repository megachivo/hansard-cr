"""
Hansard CR — Databricks App
============================
Buscador semántico, exploración por sesión/diputado y agente de
conocimiento con citas obligatorias sobre el Plenario de la Asamblea
Legislativa de Costa Rica.

Variables de entorno (configurar en app.yaml):
- VECTOR_SEARCH_ENDPOINT
- VECTOR_INDEX_NAME
- LLM_ENDPOINT
- CATALOG
- DATABRICKS_HTTP_PATH  (opcional — sólo para las páginas SQL)
"""

import os
from html import escape

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
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
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Diseño civic CR — paleta basada en la bandera
# ---------------------------------------------------------------------------

CR_BLUE = "#002B7F"
CR_RED = "#CE1126"

# Colores aproximados por fracción
PARTY_COLORS = {
    "PLN": "#2E7D32",
    "Liberación Nacional": "#2E7D32",
    "PUSC": "#1565C0",
    "Unidad Social Cristiana": "#1565C0",
    "FA": "#C62828",
    "Frente Amplio": "#C62828",
    "PLP": "#EF6C00",
    "Liberal Progresista": "#EF6C00",
    "PPSD": "#F9A825",
    "Pueblo Soberano": "#F9A825",
    "NR": "#1976D2",
    "Nueva República": "#1976D2",
}


def party_color(fraccion: str | None) -> str:
    if not fraccion:
        return "#757575"
    return PARTY_COLORS.get(fraccion.strip(), "#455A64")


def party_badge_html(fraccion: str | None) -> str:
    if not fraccion:
        return ""
    color = party_color(fraccion)
    return (
        f'<span class="party-chip" style="background:{color}">'
        f"{escape(fraccion)}</span>"
    )


st.markdown(
    f"""
    <style>
      /* Tipografía */
      html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      }}
      h1, h2, h3 {{
        font-family: 'Source Serif Pro', 'Georgia', serif;
        letter-spacing: -0.01em;
        color: {CR_BLUE};
      }}

      /* Hero */
      .hero {{
        padding: 1.5rem 1.75rem 1.25rem;
        border-radius: 14px;
        background: linear-gradient(135deg, {CR_BLUE} 0%, #06408a 60%, {CR_RED} 130%);
        color: white;
        margin-bottom: 1.25rem;
        box-shadow: 0 6px 24px rgba(0,43,127,0.18);
      }}
      .hero h1 {{
        color: white;
        margin: 0 0 0.25rem;
        font-size: 2.1rem;
        letter-spacing: -0.02em;
      }}
      .hero .tag {{
        opacity: 0.92;
        font-size: 1.0rem;
      }}
      .hero .flag-bar {{
        display: flex; gap: 4px; height: 6px;
        margin-top: 0.9rem; border-radius: 4px; overflow: hidden;
      }}
      .hero .flag-bar > div {{ flex: 1; }}

      /* Chips de fracción */
      .party-chip {{
        display: inline-block;
        padding: 2px 10px;
        border-radius: 999px;
        color: white;
        font-size: 0.78rem;
        font-weight: 600;
        letter-spacing: 0.02em;
        margin-left: 6px;
        vertical-align: middle;
      }}

      /* Card de intervención */
      .interv-card {{
        background: white;
        border: 1px solid #E5E7EB;
        border-left: 4px solid {CR_BLUE};
        border-radius: 10px;
        padding: 14px 16px;
        margin-bottom: 12px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
      }}
      .interv-card .meta {{
        font-size: 0.85rem;
        color: #5b6b7a;
        margin-bottom: 6px;
      }}
      .interv-card .who {{
        font-weight: 700;
        color: {CR_BLUE};
        font-size: 1.02rem;
      }}
      .interv-card .text {{
        margin: 6px 0 10px;
        line-height: 1.5;
        color: #1f2937;
      }}
      .interv-card .links a {{
        font-size: 0.85rem;
        margin-right: 14px;
        text-decoration: none;
        color: {CR_BLUE};
        font-weight: 600;
      }}
      .interv-card .links a:hover {{ color: {CR_RED}; }}

      /* Suggestion chips */
      .stButton > button[kind="secondary"] {{
        border-radius: 999px;
        font-size: 0.85rem;
        padding: 4px 14px;
      }}

      /* Citation pill dentro de respuesta del agente */
      .cite {{
        display: inline-block;
        background: {CR_BLUE};
        color: white;
        padding: 0 6px;
        border-radius: 999px;
        font-size: 0.72rem;
        font-weight: 600;
        margin: 0 1px;
        vertical-align: super;
      }}

      /* Sidebar branding */
      [data-testid="stSidebar"] {{
        background: #F8FAFC;
        border-right: 1px solid #E5E7EB;
      }}
      [data-testid="stSidebar"] h2 {{
        font-size: 1.1rem;
      }}
    </style>
    """,
    unsafe_allow_html=True,
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
    return OpenAI(
        api_key=w.config.token or os.environ.get("DATABRICKS_TOKEN"),
        base_url=f"{w.config.host}/serving-endpoints",
    )


@st.cache_resource(show_spinner=False)
def get_spark():
    """SQL warehouse connection. Devuelve None si no está configurado."""
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    host = os.environ.get("DATABRICKS_HOST")
    if not http_path or not host:
        return None
    try:
        from databricks import sql
        return sql.connect(
            server_hostname=host.replace("https://", "").rstrip("/"),
            http_path=http_path,
            access_token=os.environ.get("DATABRICKS_TOKEN"),
        )
    except Exception:
        return None


def sql_df(query: str) -> pd.DataFrame | None:
    """Corre una query contra el warehouse; None si no hay conexión o falla."""
    conn = get_spark()
    if conn is None:
        return None
    try:
        return pd.read_sql(query, conn)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Helpers de búsqueda y render
# ---------------------------------------------------------------------------

def buscar(query: str, k: int = 10, filtros: dict | None = None) -> list[dict]:
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
    try:
        res = idx.similarity_search(**kwargs)
    except Exception as e:
        st.warning(f"Vector Search no disponible: {e}")
        return []
    cols = [c["name"] for c in res["manifest"]["columns"]]
    return [dict(zip(cols, row)) for row in res["result"]["data_array"]]


def render_intervencion_card(item: dict, cite_n: int | None = None):
    """Renderiza una intervención como card con badge de fracción."""
    badge = party_badge_html(item.get("fraccion"))
    cite = f'<span class="cite">{cite_n}</span> ' if cite_n is not None else ""
    diputado = escape(str(item.get("diputado") or "Sin identificar"))
    fecha = escape(str(item.get("fecha") or ""))
    session_id = escape(str(item.get("session_id") or ""))
    fuente = escape(str(item.get("fuente") or "?"))
    texto = escape(str(item.get("texto") or ""))

    links = []
    if item.get("video_url") and item.get("start_sec") is not None:
        try:
            secs = int(item["start_sec"])
            mins = secs // 60
            links.append(
                f'<a href="{item["video_url"]}&t={secs}" target="_blank">'
                f"▶ Video · min {mins}</a>"
            )
        except (TypeError, ValueError):
            pass
    if item.get("pdf_url"):
        links.append(f'<a href="{item["pdf_url"]}" target="_blank">📄 Acta</a>')
    links_html = (
        f'<div class="links">{" ".join(links)}'
        f'<span style="color:#94a3b8;font-size:0.78rem;float:right">'
        f"fuente: {fuente}</span></div>"
    )

    st.markdown(
        f"""
        <div class="interv-card">
          <div class="meta">{cite}{fecha} · sesión <code>{session_id}</code></div>
          <div class="who">{diputado}{badge}</div>
          <div class="text">{texto}</div>
          {links_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Hero
# ---------------------------------------------------------------------------

def render_hero():
    st.markdown(
        """
        <div class="hero">
          <h1>🏛️ Hansard CR</h1>
          <div class="tag">El Plenario de la Asamblea Legislativa de Costa Rica,
          buscable y conversable. Cada respuesta cita su fuente.</div>
          <div class="flag-bar">
            <div style="background:#002B7F"></div>
            <div style="background:white"></div>
            <div style="background:#CE1126"></div>
            <div style="background:white"></div>
            <div style="background:#002B7F"></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Métricas (silenciosas si no hay warehouse)
    metrics = sql_df(
        f"""
        SELECT
          (SELECT COUNT(DISTINCT session_id) FROM {CATALOG}.silver.intervenciones) AS sesiones,
          (SELECT COUNT(*) FROM {CATALOG}.silver.intervenciones) AS intervenciones,
          (SELECT COUNT(DISTINCT diputado) FROM {CATALOG}.silver.intervenciones
              WHERE diputado IS NOT NULL) AS diputados
        """
    )
    if metrics is not None and not metrics.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("📅 Sesiones", int(metrics.iloc[0]["sesiones"] or 0))
        c2.metric("💬 Intervenciones", f"{int(metrics.iloc[0]['intervenciones'] or 0):,}")
        c3.metric("👥 Diputados", int(metrics.iloc[0]["diputados"] or 0))


# ---------------------------------------------------------------------------
# Página: Agente (landing)
# ---------------------------------------------------------------------------

SUGERENCIAS = [
    "¿Qué se dijo sobre la CCSS esta semana?",
    "Comparame las posturas sobre seguridad ciudadana",
    "Resumime las últimas discusiones sobre la jornada 4x3",
    "¿Qué propone Frente Amplio en educación?",
]


def _reescribir_query(historial: list[dict], pregunta: str) -> str:
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
        "Rica. Respondés solo con base en el CONTEXTO provisto. Toda "
        "afirmación factual debe ir acompañada de citas en formato [n] "
        "referidas al número de fuente. Si el contexto no alcanza, decilo "
        "explícitamente y no inventes. Tono neutral, claro y conciso. Si la "
        "pregunta pide comparar posturas, estructurá la respuesta por "
        "diputado o por fracción."
    )
    mensajes = [{"role": "system", "content": system}]
    for m in historial[-6:]:
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


def page_agente():
    st.markdown("## 💬 Agente del Plenario")
    st.caption(
        "Chat multi-turno con citas obligatorias. Cada respuesta se "
        "construye desde texto real recuperado del Plenario."
    )

    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []
    if "pending_input" not in st.session_state:
        st.session_state.pending_input = None

    # Sugerencias en chips
    if not st.session_state.agent_messages:
        st.markdown("**Probá una de estas preguntas:**")
        chip_cols = st.columns(len(SUGERENCIAS))
        for col, sug in zip(chip_cols, SUGERENCIAS):
            if col.button(sug, key=f"chip_{sug}", use_container_width=True):
                st.session_state.pending_input = sug
                st.rerun()

    # Render del histórico
    for msg in st.session_state.agent_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sources"):
                with st.expander(f"📚 Fuentes ({len(msg['sources'])})"):
                    for i, c in enumerate(msg["sources"]):
                        render_intervencion_card(c, cite_n=i + 1)

    # Input — manejo de chip o chat_input
    user_input = st.chat_input("Preguntale al Plenario...")
    if st.session_state.pending_input and not user_input:
        user_input = st.session_state.pending_input
        st.session_state.pending_input = None

    if user_input:
        st.session_state.agent_messages.append(
            {"role": "user", "content": user_input}
        )
        with st.chat_message("user"):
            st.markdown(user_input)

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
                        render_intervencion_card(c, cite_n=i + 1)

                st.session_state.agent_messages.append(
                    {
                        "role": "assistant",
                        "content": respuesta,
                        "sources": contexto,
                    }
                )


# ---------------------------------------------------------------------------
# Página: Buscar
# ---------------------------------------------------------------------------

def page_buscar():
    st.markdown("## 🔎 Buscador semántico")
    st.caption("Buscá lo que se dijo en el Plenario. Resultados con video y acta.")

    q = st.text_input(
        "Tu búsqueda",
        placeholder="ej. jornada 4x3, CCSS, seguridad ciudadana...",
        label_visibility="collapsed",
    )
    c1, c2, c3 = st.columns([2, 1, 1])
    with c2:
        solo_video = st.checkbox("Solo videos", value=False)
    with c3:
        k = st.slider("Resultados", 5, 25, 10, label_visibility="collapsed")

    if q:
        filtros = {"fuente": "video"} if solo_video else None
        with st.spinner("Buscando..."):
            resultados = buscar(q, k=k, filtros=filtros)
        if not resultados:
            st.info("Sin resultados. Probá otra búsqueda.")
        else:
            st.success(f"{len(resultados)} resultados para *{q}*")
            for r in resultados:
                render_intervencion_card(r)


# ---------------------------------------------------------------------------
# Página: Sesión
# ---------------------------------------------------------------------------

def page_sesion():
    st.markdown("## 📺 Explorar una sesión")
    sesiones = sql_df(
        f"""
        SELECT DISTINCT session_id, fecha, video_url, fuente
        FROM {CATALOG}.silver.intervenciones
        ORDER BY fecha DESC LIMIT 50
        """
    )
    if sesiones is None or sesiones.empty:
        st.warning(
            "No hay sesiones cargadas todavía. Corré `daily_pipeline` para "
            "poblar la tabla."
        )
        return

    sel = st.selectbox(
        "Sesión",
        options=sesiones["session_id"].tolist(),
        format_func=lambda s: (
            f"{s} — {sesiones[sesiones.session_id == s].iloc[0]['fecha']}"
        ),
    )
    row = sesiones[sesiones.session_id == sel].iloc[0]

    col_v, col_t = st.columns([3, 2])
    with col_v:
        if row["video_url"]:
            st.video(row["video_url"])
        else:
            st.info("Sin video para esta sesión (solo acta).")
    with col_t:
        st.markdown("### Resumen express")
        if st.button("🤖 Resumir esta sesión", use_container_width=True):
            contenido = sql_df(
                f"""
                SELECT diputado, texto FROM {CATALOG}.silver.intervenciones
                WHERE session_id = '{sel}' LIMIT 30
                """
            )
            if contenido is None or contenido.empty:
                st.warning("No hay intervenciones cargadas para esta sesión.")
            else:
                prompt = (
                    "Resumí esta sesión del Plenario en 5 bullets. Cada bullet "
                    "con el nombre del diputado entre paréntesis cuando "
                    "aplique. Sé conciso.\n\n"
                    + "\n".join(
                        f"- {r['diputado']}: {r['texto'][:300]}"
                        for _, r in contenido.iterrows()
                    )
                )
                with st.spinner("Resumiendo..."):
                    resp = get_llm().chat.completions.create(
                        model=LLM_ENDPOINT,
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=600,
                    )
                st.write(resp.choices[0].message.content)

    st.markdown("### Intervenciones")
    intervenciones = sql_df(
        f"""
        SELECT diputado, fraccion, texto, start_sec, fecha, session_id,
               video_url, fuente
        FROM {CATALOG}.silver.intervenciones
        WHERE session_id = '{sel}' ORDER BY orden LIMIT 100
        """
    )
    if intervenciones is None or intervenciones.empty:
        st.info("Sin intervenciones cargadas para esta sesión.")
    else:
        for _, i in intervenciones.iterrows():
            render_intervencion_card(i.to_dict())


# ---------------------------------------------------------------------------
# Página: Diputado
# ---------------------------------------------------------------------------

def page_diputado():
    st.markdown("## 👤 Perfil de diputado")
    diputados = sql_df(
        f"""
        SELECT diputado, ANY_VALUE(fraccion) AS fraccion,
               COUNT(*) AS intervenciones,
               SUM(LENGTH(texto)) AS chars_total
        FROM {CATALOG}.silver.intervenciones
        WHERE diputado IS NOT NULL AND diputado NOT LIKE '%sin identificar%'
        GROUP BY diputado HAVING COUNT(*) > 1
        ORDER BY intervenciones DESC
        """
    )
    if diputados is None or diputados.empty:
        st.warning("No hay datos de diputados todavía.")
        return

    dip_sel = st.selectbox("Diputado", diputados["diputado"].tolist())
    stats = diputados[diputados.diputado == dip_sel].iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Intervenciones", int(stats["intervenciones"]))
    c2.metric("Caracteres totales", f"{int(stats['chars_total']):,}")
    c3.markdown(
        f"**Fracción**<br>{party_badge_html(stats.get('fraccion'))}",
        unsafe_allow_html=True,
    )

    st.markdown("#### Top 15 más activos")
    st.bar_chart(diputados.head(15).set_index("diputado")["intervenciones"])

    st.markdown("#### Últimas intervenciones")
    ultimas = sql_df(
        f"""
        SELECT fecha, session_id, texto, diputado, fraccion, start_sec,
               video_url, fuente
        FROM {CATALOG}.silver.intervenciones
        WHERE diputado = '{dip_sel}'
        ORDER BY fecha DESC LIMIT 5
        """
    )
    if ultimas is not None and not ultimas.empty:
        for _, u in ultimas.iterrows():
            render_intervencion_card(u.to_dict())


# ---------------------------------------------------------------------------
# Sidebar nav
# ---------------------------------------------------------------------------

PAGES = {
    "💬 Agente": page_agente,
    "🔎 Buscar": page_buscar,
    "📺 Sesión": page_sesion,
    "👤 Diputado": page_diputado,
}

with st.sidebar:
    st.markdown(
        f"""
        <div style="padding:8px 0 14px;">
          <div style="font-family:'Source Serif Pro',Georgia,serif;
                      font-size:1.4rem; font-weight:700; color:{CR_BLUE};">
            🏛️ Hansard CR
          </div>
          <div style="font-size:0.82rem; color:#64748b;">
            Plenario · buscador + agente
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    page = st.radio("Navegación", list(PAGES.keys()), label_visibility="collapsed")

    st.divider()
    if page == "💬 Agente" and st.button("🧹 Limpiar chat", use_container_width=True):
        st.session_state.agent_messages = []
        st.rerun()

    st.divider()
    st.caption("**Fuentes**")
    st.caption("Actas oficiales · asamblea.go.cr")
    st.caption("Canal oficial · @AsambleaCRC")
    st.caption("")
    st.caption("Hackathon 2026 · Construido sobre Databricks.")


# ---------------------------------------------------------------------------
# Render principal
# ---------------------------------------------------------------------------

render_hero()
PAGES[page]()
