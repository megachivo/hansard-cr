# 03 — Plan de 4 horas (equipo de 3)

## Pre-requisitos antes de empezar

- [ ] Workspace Databricks con permisos para crear catálogos, endpoints, apps
- [ ] Databricks Apps habilitado en Previews
- [ ] Foundation Model APIs activo en la región
- [ ] CLI instalado: `databricks --version`
- [ ] (Recomendado) `databricks bundle init` para Asset Bundle base

## Reparto

| Rol | Quién | Foco |
|-----|-------|------|
| **A — Datos** | Persona A | Notebooks de ingesta, esquema Unity Catalog |
| **B — IA/Backend** | Persona B | Vector Search, Model Serving, RAG |
| **C — App** | Persona C | Streamlit, deploy del App, UX |

---

## Hora 0 — Setup paralelo (30 min)

### Persona A
- `databricks catalogs create hansard_cr`
- `databricks schemas create bronze --catalog-name hansard_cr` (y silver, gold)
- Crear Volume `hansard_cr.bronze.raw_files` para PDFs y audio
- Compute: cluster small (single node) para notebooks ligeros, identificar cluster GPU disponible

### Persona B
- Verificar Foundation Model APIs: `curl` a un endpoint pay-per-token
  ```bash
  curl -X POST $DATABRICKS_HOST/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations \
    -H "Authorization: Bearer $DATABRICKS_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"messages":[{"role":"user","content":"hola"}]}'
  ```
- Crear Vector Search endpoint: `hansard-cr-endpoint` (tarda 5–15 min en provisionar — **lanzarlo YA**)
- Confirmar qué modelo LLM usar (Claude si está, Llama si no)

### Persona C
- `databricks bundle init` con template Streamlit
- Editar `app.yaml` y `app.py` con Hello World
- `databricks bundle deploy --target dev`
- **Verificar que la URL del App carga.** Si esto falla, todo lo demás no importa.

---

## Horas 0.5 – 2 — Ingesta y datos en Delta (90 min)

### Persona A — `notebooks/01_scrape_actas.py`
- Descargar índice de actas de `https://www.asamblea.go.cr/glcp/actas/forms/plenario.aspx`
- Filtrar ~20 PDFs más recientes
- Descargar a Volume `bronze.raw_files/actas/`
- Extraer texto con `pdfplumber`
- Segmentar por diputado con regex:
  - Patrón principal: `^([A-ZÁÉÍÓÚÑ]+ (?:DIPUTAD[AO]|PRESIDENT[AE]) [A-ZÁÉÍÓÚÑ ]+):`
  - Patrón alternativo: `^EL DIPUTADO ([A-ZÁÉÍÓÚÑ ]+):` / `^LA DIPUTADA ([A-ZÁÉÍÓÚÑ ]+):`
  - Patrón presidencia: `^EL PRESIDENTE ([A-ZÁÉÍÓÚÑ ]+):` / `^LA PRESIDENTA`
- Escribir a `bronze.actas_raw` y `silver.intervenciones`
- **Atajo:** si <80% del PDF parsea bien, descartá esa sesión y seguí. 15 buenas > 50 malas.

### Persona B — `notebooks/02_transcribe_youtube.py`
- Lista hardcodeada de 3–5 video IDs recientes del canal (sacar de la página del canal)
- `yt-dlp -x --audio-format mp3` → guardar en Volume
- `faster-whisper` con `large-v3`, idioma `es`
- **Limitar a primeros 60 min** de cada video (`--end_time 3600`)
- Output: `silver.transcripciones` con chunks de ~30 seg

### Persona C — Streamlit base
- 4 tabs/páginas: Buscar / Sesión / Diputado / Preguntar
- Datos mockeados (lista hardcodeada con 5 intervenciones de ejemplo)
- `requirements.txt` con: `streamlit`, `databricks-sdk`, `databricks-vectorsearch`, `openai`, `pandas`
- Confirmar que el re-deploy es rápido (<2 min)

---

## Horas 2 – 3 — IA y búsqueda (60 min)

### Persona A
- Termina ingesta
- Crea `gold.intervenciones_unified` como **vista** que une `silver.intervenciones` (de actas) + intervenciones derivadas de `silver.transcripciones` (provisionalmente con `diputado='desconocido'` si no hay diarization)
- Pobla `gold.diputados` con CSV manual de los 57 diputados activos (lista pública)

### Persona B — `notebooks/03_unify_and_embed.py`
- Habilitar Change Data Feed en `gold.intervenciones_unified`:
  ```sql
  ALTER TABLE hansard_cr.gold.intervenciones_unified
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
  ```
- Crear index:
  ```python
  from databricks.vector_search.client import VectorSearchClient
  vsc = VectorSearchClient()
  vsc.create_delta_sync_index(
      endpoint_name="hansard-cr-endpoint",
      source_table_name="hansard_cr.gold.intervenciones_unified",
      index_name="hansard_cr.gold.intervenciones_idx",
      primary_key="intervencion_id",
      embedding_source_column="texto",
      embedding_model_endpoint_name="databricks-gte-large-en",
      pipeline_type="TRIGGERED",
  )
  ```
- Disparar primer sync. Esperar a que termine (5–10 min).
- (Si sobra tiempo) Registrar Whisper como endpoint custom — *opcional, no bloquea demo*

### Persona C
- Conectar `app.py` real a Vector Search
- Conectar a LLM via OpenAI SDK apuntando a Databricks Serving:
  ```python
  from openai import OpenAI
  client = OpenAI(
      api_key=os.environ["DATABRICKS_TOKEN"],
      base_url=f"{os.environ['DATABRICKS_HOST']}/serving-endpoints"
  )
  ```
- Implementar RAG en tab "Preguntar"
- Embeber video YouTube en tab "Sesión" con `st.video(url)` y param `t=start_sec`

---

## Hora 3 – 3.5 — Integración y pulido (30 min)

- Test end-to-end de las 3 queries de demo
- Página de diputado: query SQL sobre `silver.intervenciones`:
  ```sql
  SELECT diputado, COUNT(*) AS intervenciones, SUM(LENGTH(texto)) AS palabras
  FROM hansard_cr.silver.intervenciones
  WHERE fecha >= current_date() - INTERVAL 90 DAYS
  GROUP BY diputado
  ORDER BY intervenciones DESC
  ```
- Gráfico con `st.bar_chart`

---

## Hora 3.5 – 4 — Demo prep (30 min)

- Re-deploy del App (`databricks bundle deploy --target dev`)
- Probar las 3 queries de demo **2 veces seguidas**
- **Screenshots de respaldo** de cada query funcionando, en local
- Ensayar pitch completo de 5 min
- Cargar tab "Preguntar" con la pregunta de demo **ya escrita** para que solo sea darle Enter

---

## Si vas atrasado: qué cortar

Orden de corte (de menos doloroso a más):
1. Página de diputado con gráficos → reemplazar por lista simple
2. Whisper como endpoint custom → dejarlo corriendo en notebook
3. Tab "Sesión" con video sincronizado → reemplazar por link externo a YouTube
4. Transcripciones de YouTube → solo actas (perdés el ángulo "fresco" pero mantenés el demo)
5. (Último recurso) RAG → solo buscador, sin chat

**Nunca cortes:** Vector Search + buscador básico. Es el core.
