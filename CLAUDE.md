# Hansard CR — Buscador del Plenario de la Asamblea Legislativa de Costa Rica

> Este archivo orienta a Claude Code. Leelo primero antes de cualquier tarea.

## Qué es esto

Hackathon project (4 horas, 3 personas) para construir un **buscador semántico + dashboard** sobre las sesiones del Plenario de la Asamblea Legislativa de Costa Rica. Combina:

- **Actas oficiales** (PDFs de SharePoint)
- **Transcripciones de video** (canal oficial de YouTube vía Whisper)
- **LLM con citas obligatorias** (Claude/Llama vía Databricks Model Serving)

Todo el stack corre en **Databricks**: Delta + Unity Catalog + Vector Search + Model Serving + Databricks Apps.

## Estructura del repo

```
hansard-cr/
├── CLAUDE.md                    ← este archivo
├── docs/
│   ├── 01-contexto.md           ← fuentes de datos, decisiones, riesgos
│   ├── 02-arquitectura.md       ← diagrama y stack
│   ├── 03-plan-4-horas.md       ← cronograma por persona
│   ├── 04-pitch.md              ← cómo presentar a jueces
│   └── 05-fuentes-urls.md       ← URLs canónicas de scraping
├── notebooks/
│   ├── 01_scrape_actas.py       ← Databricks notebook (PDF → Delta)
│   ├── 02_transcribe_youtube.py ← yt-dlp + Whisper → Delta
│   ├── 03_unify_and_embed.py    ← union + Vector Search index
│   └── 04_register_whisper.py   ← (opcional) Whisper como endpoint
├── app/
│   ├── app.py                   ← Streamlit (Databricks App)
│   ├── app.yaml                 ← config del Databricks App
│   └── requirements.txt
├── jobs/
│   └── daily_pipeline.json      ← Lakeflow Job que orquesta los 3 notebooks
└── scripts/
    └── deploy.sh                ← `databricks bundle deploy` helper
```

## Reglas de trabajo para Claude Code

1. **Leé el doc relevante antes de codear.** Si la tarea es scraping → `docs/05-fuentes-urls.md`. Si es arquitectura → `docs/02-arquitectura.md`.
2. **No inventés URLs ni esquemas de tablas.** Están en los docs. Si falta algo, preguntá antes de adivinar.
3. **Mantené Unity Catalog como fuente de verdad.** Todas las tablas viven en `hansard_cr.{bronze,silver,gold}.*`.
4. **Citas obligatorias en cualquier output de LLM.** El RAG nunca responde sin pasar contexto recuperado, y siempre muestra fuentes.
5. **Optimizá para demo, no para producción exhaustiva.** 15 sesiones bien parseadas > 200 sucias. 60 min de transcripción por video > video entero.
6. **Tope de Databricks Apps: 2 vCPU, 6 GB RAM, archivos ≤10 MB.** Modelos pesados van en endpoints, no en el App.

## Comandos útiles

```bash
# Validar el bundle antes de deploy
databricks bundle validate

# Deploy del App + notebooks + job
databricks bundle deploy --target dev

# Correr el job manualmente
databricks bundle run daily_pipeline

# Tail de logs del App
databricks apps logs hansard-cr-app
```

## Estado actual y próximos pasos

Editá esta sección a medida que avanzás:

- [ ] Hora 0: workspace listo, App "Hello World" desplegado, Foundation Model APIs verificadas
- [ ] Hora 1: notebook `01_scrape_actas.py` poblando `silver.intervenciones`
- [ ] Hora 1: notebook `02_transcribe_youtube.py` poblando `silver.transcripciones`
- [ ] Hora 2: Vector Search index creado sobre `gold.intervenciones_unified`
- [ ] Hora 2.5: Streamlit conectado a Vector Search + LLM endpoint
- [ ] Hora 3: 3 queries de demo funcionando end-to-end
- [ ] Hora 3.5: gráfico de perfil de diputado
- [ ] Hora 4: re-deploy, demo prep, screenshots de respaldo

## Las 3 queries que deben funcionar en el demo (no negociables)

1. *"¿Qué se dijo sobre la CCSS esta semana?"* — buscador con resultados frescos del video.
2. *"Resumime la sesión del [fecha]"* — RAG con citas.
3. *"Comparame las posturas de [Diputado X] y [Diputada Y] sobre seguridad"* — la query "wow".

Si alguna de estas se rompe, **dejá todo y arreglala antes de seguir**.
