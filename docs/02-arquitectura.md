# 02 — Arquitectura técnica

## Diagrama

```
┌──────────────────────────────────────────────────────────────────────┐
│                     UNITY CATALOG (gobierno único)                    │
│   bronze.sesiones_raw  →  silver.intervenciones  →  gold.search_idx  │
└────────────┬─────────────────────────────────────┬───────────────────┘
             │                                      │
             ▼                                      ▼
┌────────────────────────────┐         ┌──────────────────────────────┐
│  LAKEFLOW JOB (GPU cluster)│         │  MODEL SERVING ENDPOINTS      │
│  Notebook 1: scrape actas  │         │  • whisper-cr (custom MLflow) │
│  Notebook 2: yt-dlp+Whisper│         │  • claude-sonnet (external)   │
│  Notebook 3: embeddings    │         │  • bge / gte (FMAPI)          │
│  Notebook 4: NER+segment   │         │                              │
│  Corre 1x al día           │         │  Expuestos como REST          │
└────────────┬───────────────┘         └──────────────┬───────────────┘
             │                                        │
             └──────────────┬─────────────────────────┘
                            ▼
              ┌─────────────────────────────┐
              │   VECTOR SEARCH INDEX       │
              │   (managed, sobre Delta)    │
              │   gold.intervenciones_idx   │
              └──────────────┬──────────────┘
                             ▼
              ┌─────────────────────────────┐
              │      DATABRICKS APP          │
              │      (Streamlit, 2 vCPU)     │
              │                              │
              │  • Buscador semántico        │
              │  • Vista de sesión + video YT│
              │  • Chat con citas (RAG)      │
              │  • Perfil de diputado        │
              └─────────────────────────────┘
```

## Esquema de tablas en Unity Catalog

### `hansard_cr.bronze.actas_raw`
| col | tipo | nota |
|-----|------|------|
| pdf_url | STRING | URL original del PDF |
| session_id | STRING | ej. `ord-096-2025` |
| fecha | DATE | de la sesión |
| pdf_bytes | BINARY | opcional, PDF crudo |
| texto_crudo | STRING | output de pdfplumber |
| ingested_at | TIMESTAMP | |

### `hansard_cr.bronze.youtube_raw`
| col | tipo | nota |
|-----|------|------|
| video_id | STRING | id de YouTube |
| video_url | STRING | URL canónica |
| titulo | STRING | título original |
| fecha | DATE | parseada del título |
| session_id | STRING | derivado |
| duracion_seg | INT | |
| audio_path | STRING | path en Volume |
| ingested_at | TIMESTAMP | |

### `hansard_cr.silver.intervenciones`
Una fila por intervención individual de un diputado.
| col | tipo | nota |
|-----|------|------|
| intervencion_id | STRING | uuid |
| session_id | STRING | FK a sesión |
| fecha | DATE | |
| fuente | STRING | `acta` \| `video` |
| diputado | STRING | nombre normalizado |
| fraccion | STRING | partido |
| texto | STRING | contenido |
| orden | INT | secuencia dentro de la sesión |
| start_sec | INT | nullable, solo si fuente=video |
| video_url | STRING | nullable |
| pdf_url | STRING | nullable |

### `hansard_cr.silver.transcripciones`
Chunks crudos del Whisper antes de asociar a diputado.
| col | tipo | nota |
|-----|------|------|
| video_id | STRING | |
| start_sec | INT | |
| end_sec | INT | |
| texto | STRING | |
| confidence | FLOAT | |

### `hansard_cr.gold.intervenciones_unified`
Vista (o tabla) sobre la que se construye el Vector Search index. Mismo esquema que `silver.intervenciones` pero con `embedding` opcional si no usamos managed embeddings.

### `hansard_cr.gold.diputados`
| col | tipo | nota |
|-----|------|------|
| diputado_id | STRING | |
| nombre | STRING | |
| fraccion | STRING | |
| provincia | STRING | |
| activo | BOOLEAN | |

## Endpoints de Model Serving

| Endpoint | Tipo | Uso |
|----------|------|-----|
| `databricks-claude-sonnet-4` o `databricks-meta-llama-3-3-70b-instruct` | Foundation Model API | RAG, resúmenes |
| `databricks-gte-large-en` o `databricks-bge-large-en` | Foundation Model API | Embeddings (multilingüe aceptable) |
| `hansard-whisper` (opcional) | Custom MLflow pyfunc | Transcripción de audio |

**Recomendación de modelo para embeddings en español:** si querés calidad superior, registrá `intfloat/multilingual-e5-large` como endpoint propio. Si querés velocidad, usá el `databricks-gte-large-en` que aguanta español decente.

## Vector Search index

```python
index_name = "hansard_cr.gold.intervenciones_idx"
endpoint_name = "hansard-cr-endpoint"
source_table = "hansard_cr.gold.intervenciones_unified"
primary_key = "intervencion_id"
embedding_source_column = "texto"
embedding_model_endpoint = "databricks-gte-large-en"
pipeline_type = "TRIGGERED"  # o "CONTINUOUS" si querés sync automático
```

## Decisiones técnicas defendibles

| Decisión | Por qué |
|----------|---------|
| Vector Search managed en vez de pgvector | Sync automático con Delta, sin infra extra |
| Whisper en Job (no en App) | App tiene 2 vCPU; Whisper pide GPU |
| Streamlit en vez de React | 4 horas, prioridad demo, framework natural de Databricks Apps |
| `faster-whisper` con `large-v3` | Mejor relación calidad/velocidad para español tico |
| Foundation Model APIs (no API key externa) | Gobernanza, billing centralizado, sin secretos en el repo |
| Citas obligatorias en RAG | Anti-alucinación by design, no por buena fe |

## Por qué Databricks específicamente

- **Unity Catalog** = gobierno y lineage de las actas desde día 1.
- **Vector Search managed** = sin montar pgvector/Weaviate; sync automático con Delta.
- **Model Serving** = endpoints REST con autoscale para Whisper y LLM, sin gestionar infra.
- **Databricks Apps** = deploy en 1 comando, autenticación OAuth incluida.
- **Asset Bundles** = todo el proyecto (notebooks + app + job) versionado y reproducible. Si la Asamblea quisiera adoptarlo, son 3 comandos.
