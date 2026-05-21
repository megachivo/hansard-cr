"""
Hansard CR — Databricks App
============================
Buscador semántico, exploración por sesión/diputado y agente de
conocimiento (chat flotante) sobre el Plenario de la Asamblea
Legislativa de Costa Rica. Cada respuesta cita su fuente.

Variables de entorno (configurar en app.yaml):
- AGENT_ENDPOINT_URL  (URL /invocations del Knowledge Assistant — el
  agente del chat lateral llama directo a este endpoint)
- VECTOR_SEARCH_ENDPOINT, VECTOR_INDEX_NAME  (sólo para la página "Buscar")
- LLM_ENDPOINT  (usado por "Resumir esta sesión")
- CATALOG, SCHEMA_SILVER, SCHEMA_GOLD  (queries SQL desde el App)
- DATABRICKS_HTTP_PATH  (opcional — sólo para las páginas SQL)
"""

import os
from html import escape

import pandas as pd
import requests
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
# Knowledge Assistant (RAG-as-a-service) endpoint. Maneja retrieval +
# generación internamente, así que el app sólo le manda los mensajes.
AGENT_ENDPOINT_URL = os.environ.get(
    "AGENT_ENDPOINT_URL",
    "https://adb-7405613420378213.13.azuredatabricks.net/serving-endpoints/"
    "ka-232fb7fd-endpoint/invocations",
)
CATALOG = os.environ.get("CATALOG", "hansard_cr")
SCHEMA_SILVER = os.environ.get("SCHEMA_SILVER", "silver")
SCHEMA_GOLD = os.environ.get("SCHEMA_GOLD", "gold")
SILVER_INTERVENCIONES = f"{CATALOG}.{SCHEMA_SILVER}.intervenciones"
GOLD_UNIFIED = f"{CATALOG}.{SCHEMA_GOLD}.intervenciones_unified"

st.set_page_config(
    page_title="Hansard CR",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Paleta CR
# ---------------------------------------------------------------------------

CR_BLUE = "#002B7F"
CR_RED = "#CE1126"

PARTY_COLORS = {
    "PLN": "#2E7D32", "Liberación Nacional": "#2E7D32",
    "PUSC": "#1565C0", "Unidad Social Cristiana": "#1565C0",
    "FA": "#C62828", "Frente Amplio": "#C62828",
    "PLP": "#EF6C00", "Liberal Progresista": "#EF6C00",
    "PPSD": "#F9A825", "Pueblo Soberano": "#F9A825",
    "NR": "#1976D2", "Nueva República": "#1976D2",
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


def avatar_html(name: str | None, fraccion: str | None) -> str:
    parts = [p for p in (name or "").replace("Diputado", "").replace("Diputada", "").split() if p]
    initials = "".join(p[0] for p in parts[:2]).upper() or "?"
    color = party_color(fraccion)
    return (
        f'<div class="avatar" style="background:{color}">{escape(initials)}</div>'
    )


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

st.markdown(
    f"""
    <style>
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
        padding: 1.6rem 1.9rem 1.3rem;
        border-radius: 16px;
        background:
          radial-gradient(circle at 90% -20%, rgba(255,255,255,0.18), transparent 50%),
          linear-gradient(135deg, {CR_BLUE} 0%, #06408a 55%, {CR_RED} 140%);
        color: white;
        margin-bottom: 1.25rem;
        box-shadow: 0 10px 30px rgba(0,43,127,0.20);
        position: relative;
        overflow: hidden;
      }}
      .hero::after {{
        content: "🏛️";
        position: absolute;
        right: -8px; bottom: -36px;
        font-size: 8rem; opacity: 0.07;
        pointer-events: none;
      }}
      .hero h1 {{
        color: white; margin: 0 0 0.25rem;
        font-size: 2.2rem; letter-spacing: -0.02em;
      }}
      .hero .tag {{ opacity: 0.94; font-size: 1.02rem; max-width: 720px; }}
      .hero .flag-bar {{
        display: flex; gap: 3px; height: 6px;
        margin-top: 1rem; border-radius: 4px; overflow: hidden;
      }}
      .hero .flag-bar > div {{ flex: 1; }}

      /* Suggestion cards */
      .sugcard {{
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 14px 16px;
        cursor: pointer;
        transition: all 0.15s ease;
        height: 100%;
      }}
      .sugcard:hover {{
        border-color: {CR_BLUE};
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(0,43,127,0.10);
      }}
      .sugcard .ico {{ font-size: 1.4rem; }}

      /* Chips */
      .party-chip {{
        display: inline-block; padding: 2px 10px; border-radius: 999px;
        color: white; font-size: 0.78rem; font-weight: 600;
        letter-spacing: 0.02em; margin-left: 6px; vertical-align: middle;
      }}

      /* Avatar */
      .avatar {{
        display: inline-flex; align-items: center; justify-content: center;
        width: 36px; height: 36px; border-radius: 999px;
        color: white; font-weight: 700; font-size: 0.85rem;
        margin-right: 10px; vertical-align: middle;
        flex-shrink: 0;
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
      .interv-card .head {{
        display: flex; align-items: center; margin-bottom: 8px;
      }}
      .interv-card .who {{
        font-weight: 700; color: {CR_BLUE};
        font-size: 1.02rem;
      }}
      .interv-card .meta {{
        font-size: 0.82rem; color: #5b6b7a;
      }}
      .interv-card .text {{
        margin: 4px 0 10px; line-height: 1.55; color: #1f2937;
      }}
      .interv-card .links a {{
        font-size: 0.85rem; margin-right: 14px;
        text-decoration: none; color: {CR_BLUE}; font-weight: 600;
      }}
      .interv-card .links a:hover {{ color: {CR_RED}; }}

      /* Citation pill */
      .cite {{
        display: inline-block; background: {CR_BLUE};
        color: white; padding: 0 6px; border-radius: 999px;
        font-size: 0.72rem; font-weight: 600;
        margin: 0 1px; vertical-align: super;
      }}

      /* Sidebar — más ancho para que la conversación con el agente quepa */
      [data-testid="stSidebar"] {{
        background: #F8FAFC;
        border-right: 1px solid #E5E7EB;
        min-width: 360px !important;
        max-width: 420px !important;
      }}
      [data-testid="stSidebar"] h2 {{ font-size: 1.1rem; }}

      /* Floating agent — el contenedor que envuelve todo es el de
         streamlit-float; le damos look de panel/burbuja según estado. */
      .agent-panel {{
        background: white;
        border-radius: 18px;
        padding: 14px 16px;
        border-top: 4px solid {CR_BLUE};
        box-shadow: 0 18px 50px rgba(0,0,0,0.22);
      }}
      .agent-panel .head {{
        display: flex; justify-content: space-between;
        align-items: center; padding-bottom: 8px;
        border-bottom: 1px solid #eef0f3; margin-bottom: 8px;
      }}
      .agent-panel .head .title {{
        color: {CR_BLUE}; font-weight: 700;
        font-family: 'Source Serif Pro', Georgia, serif;
      }}
      .agent-bubble button {{
        background: linear-gradient(135deg, {CR_BLUE} 0%, {CR_RED} 140%) !important;
        color: white !important;
        border: none !important;
        border-radius: 999px !important;
        padding: 12px 18px !important;
        font-weight: 700 !important;
        box-shadow: 0 10px 26px rgba(0,43,127,0.32) !important;
      }}
      .agent-bubble button:hover {{
        transform: translateY(-1px);
        box-shadow: 0 12px 30px rgba(0,43,127,0.45) !important;
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
    host = os.environ.get("DATABRICKS_HOST")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    if not http_path:
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
        if warehouse_id:
            http_path = f"/sql/1.0/warehouses/{warehouse_id}"
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
    conn = get_spark()
    if conn is None:
        return None
    try:
        return pd.read_sql(query, conn)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Búsqueda y renders
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


def render_intervencion_card(item: dict, cite_n: int | None = None, compact: bool = False):
    avatar = avatar_html(item.get("diputado"), item.get("fraccion"))
    badge = party_badge_html(item.get("fraccion"))
    cite = f'<span class="cite">{cite_n}</span> ' if cite_n is not None else ""
    diputado = escape(str(item.get("diputado") or "Sin identificar"))
    fecha = escape(str(item.get("fecha") or ""))
    session_id = escape(str(item.get("session_id") or ""))
    fuente = escape(str(item.get("fuente") or "?"))
    texto = escape(str(item.get("texto") or ""))
    if compact and len(texto) > 220:
        texto = texto[:220] + "…"

    links = []
    if item.get("video_url") and item.get("start_sec") is not None:
        try:
            secs = int(item["start_sec"])
            links.append(
                f'<a href="{item["video_url"]}&t={secs}" target="_blank">'
                f"▶ Video · min {secs // 60}</a>"
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
          <div class="head">
            {avatar}
            <div>
              <div class="who">{cite}{diputado}{badge}</div>
              <div class="meta">{fecha} · sesión <code>{session_id}</code></div>
            </div>
          </div>
          <div class="text">{texto}</div>
          {links_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Agente — pipeline
# ---------------------------------------------------------------------------

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
            max_tokens=120, temperature=0.0,
        )
        return resp.choices[0].message.content.strip().strip('"') or pregunta
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


def _agent_auth_token() -> str | None:
    """Pick the cleanest token available for calling the agent endpoint."""
    try:
        return WorkspaceClient().config.token
    except Exception:
        return os.environ.get("DATABRICKS_TOKEN")


def _normalize_agent_sources(raw) -> list[dict]:
    """Conformar fuentes del Knowledge Assistant al formato que esperan
    nuestras tarjetas (`render_intervencion_card`)."""
    if not raw:
        return []
    out = []
    for s in raw:
        if not isinstance(s, dict):
            continue
        meta = s.get("metadata") or s.get("doc_metadata") or {}

        def pick(*keys):
            for k in keys:
                if s.get(k) not in (None, ""):
                    return s.get(k)
                if meta.get(k) not in (None, ""):
                    return meta.get(k)
            return None

        texto = pick("texto", "page_content", "content", "text") or ""
        out.append({
            "diputado": pick("diputado") or "Sin identificar",
            "fraccion": pick("fraccion"),
            "fecha": pick("fecha") or "",
            "session_id": pick("session_id") or "",
            "video_url": pick("video_url"),
            "start_sec": pick("start_sec"),
            "pdf_url": pick("pdf_url"),
            "fuente": pick("fuente") or "agent",
            "texto": texto,
        })
    return out


def _extract_agent_payload(data) -> tuple[str, list[dict]]:
    """Robustly pull (answer, sources) from the endpoint response.

    Acepta varias formas: OpenAI chat completions, MLflow predictions,
    custom outputs con retrievals, etc."""
    if not isinstance(data, dict):
        return str(data), []

    answer = ""
    # OpenAI chat completions
    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message") or {}
        answer = msg.get("content") or ""

    # MLflow dataframe_records / dataframe_split predictions
    if not answer and "predictions" in data:
        pred = data["predictions"]
        if isinstance(pred, list) and pred:
            pred = pred[0]
        if isinstance(pred, dict):
            answer = pred.get("content") or pred.get("output") or pred.get("text") or ""
        elif isinstance(pred, str):
            answer = pred

    if not answer:
        answer = data.get("output") or data.get("answer") or data.get("response") or ""
    if not answer:
        answer = str(data)

    # Sources can live in several places
    sources_raw = (
        (data.get("custom_outputs") or {}).get("retrievals")
        or (data.get("custom_outputs") or {}).get("sources")
        or data.get("retrievals")
        or data.get("sources")
        or data.get("context")
    )
    return answer, _normalize_agent_sources(sources_raw)


def run_agent_turn(user_input: str):
    """Una vuelta del agente: POST a la URL del Knowledge Assistant."""
    st.session_state.agent_messages.append(
        {"role": "user", "content": user_input}
    )

    messages = [
        {"role": m["role"], "content": m["content"]}
        for m in st.session_state.agent_messages[-12:]
    ]

    token = _agent_auth_token()
    if not token:
        st.session_state.agent_messages.append({
            "role": "assistant",
            "content": "⚠️ No hay credenciales para llamar al agente "
                       "(falta DATABRICKS_TOKEN).",
            "sources": [],
        })
        return

    try:
        resp = requests.post(
            AGENT_ENDPOINT_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"messages": messages},
            timeout=90,
        )
        resp.raise_for_status()
        respuesta, sources = _extract_agent_payload(resp.json())
    except requests.HTTPError as e:
        body = e.response.text[:400] if e.response is not None else ""
        respuesta = f"⚠️ El agente respondió con error: {e}\n\n```\n{body}\n```"
        sources = []
    except Exception as e:
        respuesta = f"⚠️ No pude llamar al agente: {e}"
        sources = []

    st.session_state.agent_messages.append(
        {"role": "assistant", "content": respuesta, "sources": sources}
    )


# ---------------------------------------------------------------------------
# Floating agent widget
# ---------------------------------------------------------------------------

def render_sidebar_agent():
    """Render the agent inside the left sidebar.

    Closed: just a 'Abrir agente' button. The user can browse the main
    page without distraction.
    Open: chat history (scrollable) + input form. Stays in the sidebar
    so the main page content remains fully visible to the right.
    """
    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []
    if "chat_open" not in st.session_state:
        st.session_state.chat_open = False

    chat_open = st.session_state.chat_open

    if not chat_open:
        if st.button("💬 Abrir agente",
                     key="open_agent", use_container_width=True,
                     type="primary"):
            st.session_state.chat_open = True
            st.rerun()
        return

    # Header
    head_col1, head_col2, head_col3 = st.columns([4, 1, 1])
    head_col1.markdown(
        f'<div style="color:{CR_BLUE};font-weight:700;'
        'font-family:Source Serif Pro,Georgia,serif;'
        'padding-top:6px;">💬 Agente</div>',
        unsafe_allow_html=True,
    )
    if head_col2.button("🧹", key="clear_agent", help="Limpiar conversación"):
        st.session_state.agent_messages = []
        st.rerun()
    if head_col3.button("✕", key="close_agent", help="Cerrar"):
        st.session_state.chat_open = False
        st.rerun()

    if not st.session_state.agent_messages:
        st.caption(
            "Preguntá lo que se ha dicho en el Plenario. "
            "Cada respuesta cita su fuente."
        )

    # Pending input from sugerencia chips
    pending = st.session_state.pop("pending_input", None)

    # History (scrollable)
    history = st.container(height=360)
    with history:
        for msg in st.session_state.agent_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
                if msg.get("sources"):
                    with st.expander(f"📚 {len(msg['sources'])} fuentes"):
                        for i, c in enumerate(msg["sources"]):
                            render_intervencion_card(
                                c, cite_n=i + 1, compact=True
                            )

    # Input form
    with st.form("agent_form", clear_on_submit=True):
        user_input = st.text_input(
            "Pregunta",
            placeholder="Preguntale al Plenario...",
            label_visibility="collapsed",
        )
        submitted = st.form_submit_button("Enviar →", use_container_width=True)

    text = pending or (user_input if submitted else None)
    if text:
        with st.spinner("Pensando..."):
            run_agent_turn(text)
        st.rerun()


# ---------------------------------------------------------------------------
# Hero
# ---------------------------------------------------------------------------

def render_hero():
    st.markdown(
        """
        <div class="hero">
          <h1>🏛️ Hansard CR</h1>
          <div class="tag">El Plenario de la Asamblea Legislativa de Costa
          Rica, buscable y conversable. Cada respuesta cita su fuente.</div>
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


def render_metrics():
    metrics = sql_df(
        f"""
        SELECT
          (SELECT COUNT(DISTINCT session_id) FROM {SILVER_INTERVENCIONES}) AS sesiones,
          (SELECT COUNT(*) FROM {SILVER_INTERVENCIONES}) AS intervenciones,
          (SELECT COUNT(DISTINCT diputado) FROM {SILVER_INTERVENCIONES}
              WHERE diputado IS NOT NULL) AS diputados
        """
    )
    if metrics is None or metrics.empty:
        return
    c1, c2, c3 = st.columns(3)
    c1.metric("📅 Sesiones", int(metrics.iloc[0]["sesiones"] or 0))
    c2.metric("💬 Intervenciones",
              f"{int(metrics.iloc[0]['intervenciones'] or 0):,}")
    c3.metric("👥 Diputados", int(metrics.iloc[0]["diputados"] or 0))


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

SUGERENCIAS = [
    ("🏥", "CCSS",
     "¿Qué se ha dicho sobre la situación de la CCSS?"),
    ("🛡️", "Seguridad",
     "Comparame las posturas de las fracciones sobre seguridad ciudadana"),
    ("⏱️", "Jornada 4x3",
     "Resumime las últimas discusiones sobre la jornada laboral 4x3"),
    ("📚", "Educación",
     "¿Qué propuestas hay sobre presupuesto en educación?"),
]


def _open_chat_with(query: str):
    st.session_state.chat_open = True
    st.session_state.pending_input = query
    st.rerun()


def page_inicio():
    render_metrics()
    st.markdown("## 💡 Probá una pregunta")
    st.caption(
        "Tocá una tarjeta para abrir el agente con la pregunta lista. "
        "El agente busca en el Plenario y responde con citas."
    )

    cols = st.columns(len(SUGERENCIAS))
    for col, (ico, titulo, pregunta) in zip(cols, SUGERENCIAS):
        with col:
            st.markdown(
                f"""
                <div class="sugcard">
                  <div class="ico">{ico}</div>
                  <div style="font-weight:700; margin:6px 0 4px;">{escape(titulo)}</div>
                  <div style="color:#5b6b7a; font-size:0.88rem;">{escape(pregunta)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button("Preguntar", key=f"sug_{titulo}", use_container_width=True):
                _open_chat_with(pregunta)

    # Sesiones recientes
    st.markdown("## 🗓️ Sesiones recientes")
    recientes = sql_df(
        f"""
        SELECT session_id, fecha, COUNT(*) AS intervenciones,
               ANY_VALUE(video_url) AS video_url, ANY_VALUE(fuente) AS fuente
        FROM {SILVER_INTERVENCIONES}
        GROUP BY session_id, fecha
        ORDER BY fecha DESC LIMIT 5
        """
    )
    if recientes is None or recientes.empty:
        st.info(
            "Sin sesiones cargadas todavía. Corré "
            "`databricks bundle run vector_search_bootstrap` para sembrar "
            "datos placeholder."
        )
        return

    for _, s in recientes.iterrows():
        c1, c2, c3 = st.columns([2, 1, 1])
        c1.markdown(
            f"**{s['fecha']}** · sesión `{s['session_id']}`  \n"
            f"<span style='color:#5b6b7a'>fuente: {s['fuente']} · "
            f"{int(s['intervenciones'])} intervenciones</span>",
            unsafe_allow_html=True,
        )
        if s["video_url"]:
            c2.link_button("▶ Video", s["video_url"], use_container_width=True)
        if c3.button("Resumir 🤖", key=f"res_{s['session_id']}",
                     use_container_width=True):
            _open_chat_with(f"Resumime la sesión {s['session_id']}")


def page_buscar():
    st.markdown("## 🔎 Buscador semántico")
    st.caption("Buscá lo que se dijo en el Plenario. Resultados con video y acta.")

    q = st.text_input(
        "Tu búsqueda",
        placeholder="ej. jornada 4x3, CCSS, seguridad ciudadana...",
        label_visibility="collapsed",
    )
    c1, c2 = st.columns([3, 1])
    with c2:
        solo_video = st.checkbox("Solo videos", value=False)
    k = c1.slider("Resultados", 5, 25, 10, label_visibility="collapsed")

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


def page_sesion():
    st.markdown("## 📺 Explorar una sesión")
    sesiones = sql_df(
        f"""
        SELECT DISTINCT session_id, fecha, video_url, fuente
        FROM {SILVER_INTERVENCIONES}
        ORDER BY fecha DESC LIMIT 50
        """
    )
    if sesiones is None or sesiones.empty:
        st.warning(
            "No hay sesiones cargadas todavía. Corré `daily_pipeline` o "
            "`vector_search_bootstrap` para poblar la tabla."
        )
        return

    sel = st.selectbox(
        "Sesión", options=sesiones["session_id"].tolist(),
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
        st.markdown("### Acciones")
        if st.button("🤖 Resumir esta sesión", use_container_width=True):
            _open_chat_with(f"Resumime la sesión {sel}")
        if st.button("💬 Preguntar sobre esta sesión", use_container_width=True):
            _open_chat_with(
                f"¿Qué se discutió en la sesión {sel} del {row['fecha']}?"
            )

    st.markdown("### Intervenciones")
    intervenciones = sql_df(
        f"""
        SELECT diputado, fraccion, texto, start_sec, fecha, session_id,
               video_url, fuente
        FROM {SILVER_INTERVENCIONES}
        WHERE session_id = '{sel}' ORDER BY orden LIMIT 100
        """
    )
    if intervenciones is None or intervenciones.empty:
        st.info("Sin intervenciones cargadas para esta sesión.")
    else:
        for _, i in intervenciones.iterrows():
            render_intervencion_card(i.to_dict())


def page_diputado():
    st.markdown("## 👤 Perfil de diputado")
    diputados = sql_df(
        f"""
        SELECT diputado, ANY_VALUE(fraccion) AS fraccion,
               COUNT(*) AS intervenciones,
               SUM(LENGTH(texto)) AS chars_total
        FROM {SILVER_INTERVENCIONES}
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

    if st.button(f"💬 Preguntar al agente sobre {dip_sel}",
                 use_container_width=False):
        _open_chat_with(f"¿Cuáles son las posturas de {dip_sel}?")

    st.markdown("#### Top 15 más activos")
    st.bar_chart(diputados.head(15).set_index("diputado")["intervenciones"])

    st.markdown("#### Últimas intervenciones")
    ultimas = sql_df(
        f"""
        SELECT fecha, session_id, texto, diputado, fraccion, start_sec,
               video_url, fuente
        FROM {SILVER_INTERVENCIONES}
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
    "🏠 Inicio": page_inicio,
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
    page = st.radio(
        "Navegación", list(PAGES.keys()), label_visibility="collapsed"
    )

    st.divider()
    render_sidebar_agent()

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
