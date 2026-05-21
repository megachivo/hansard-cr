# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Unificación y creación del Vector Search index
# MAGIC
# MAGIC **Objetivo:**
# MAGIC 1. Crear `gold.intervenciones_unified` con CDF habilitado.
# MAGIC 2. Crear el Vector Search endpoint y el index managed.
# MAGIC 3. Disparar el primer sync.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# Widgets para que el bundle pueda inyectar los nombres reales de los
# schemas (en `mode: development` los schemas tienen prefijo de usuario).
dbutils.widgets.text("catalog", "hansard_cr")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold", "gold")
dbutils.widgets.text("endpoint_name", "hansard-cr-endpoint")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")
ENDPOINT = dbutils.widgets.get("endpoint_name")
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint")

UNIFIED_TABLE = f"{CATALOG}.{SCHEMA_GOLD}.intervenciones_unified"
SILVER_TABLE = f"{CATALOG}.{SCHEMA_SILVER}.intervenciones"
INDEX = f"{CATALOG}.{SCHEMA_GOLD}.intervenciones_idx"
SOURCE_TABLE = UNIFIED_TABLE
print(f"catalog={CATALOG} silver={SCHEMA_SILVER} gold={SCHEMA_GOLD}")
print(f"source={SOURCE_TABLE} index={INDEX} endpoint={ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Crear vista/tabla `gold.intervenciones_unified`

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {UNIFIED_TABLE} (
  intervencion_id STRING NOT NULL,
  session_id STRING,
  fecha DATE,
  fuente STRING,
  diputado STRING,
  fraccion STRING,
  texto STRING,
  orden INT,
  start_sec INT,
  video_url STRING,
  pdf_url STRING,
  CONSTRAINT pk_intervenciones PRIMARY KEY (intervencion_id)
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# Poblar desde silver
silver_exists = spark.catalog.tableExists(SILVER_TABLE)
silver_count = (
    spark.table(SILVER_TABLE).count() if silver_exists else 0
)

if silver_count > 0:
    spark.sql(f"""
    INSERT OVERWRITE {UNIFIED_TABLE}
    SELECT
      intervencion_id,
      session_id,
      fecha,
      fuente,
      diputado,
      fraccion,
      texto,
      orden,
      start_sec,
      video_url,
      pdf_url
    FROM {SILVER_TABLE}
    WHERE LENGTH(texto) > 50
    """)
    print(f"Cargado desde {SILVER_TABLE} ({silver_count} filas en silver).")
else:
    # ------------------------------------------------------------------
    # Placeholder seed: deja la tabla utilizable por el App aun antes
    # de que terminen los pipelines de scraping/transcripción. Estas
    # filas se sobreescriben en la próxima corrida con datos reales.
    # ------------------------------------------------------------------
    from datetime import date

    seed = [
        (
            "seed-001", "ord-001-2026", date(2026, 5, 14), "video",
            "Diputada Pilar Cisneros", "PPSD",
            "La situación de la CCSS es insostenible. Los tiempos de espera "
            "para una cita de especialidad superan los seis meses en varias "
            "regiones del país. Necesitamos una reforma estructural ya.",
            1, 320,
            "https://www.youtube.com/watch?v=demo01", None,
        ),
        (
            "seed-002", "ord-001-2026", date(2026, 5, 14), "video",
            "Diputado Antonio Ortega", "Frente Amplio",
            "Coincido en que la CCSS necesita más recursos, pero la solución "
            "no es desfinanciarla con exoneraciones. Aquí proponemos un "
            "fortalecimiento del primer nivel de atención.",
            2, 540,
            "https://www.youtube.com/watch?v=demo01", None,
        ),
        (
            "seed-003", "ord-001-2026", date(2026, 5, 14), "acta",
            "Diputado Daniel Vargas", "Liberación Nacional",
            "Sobre seguridad ciudadana, los homicidios crecieron 38% en dos "
            "años. La estrategia actual no está funcionando. Proponemos "
            "duplicar las unidades de Fuerza Pública en cantones críticos.",
            3, None,
            None, "https://www.asamblea.go.cr/actas/2026-05-14.pdf",
        ),
        (
            "seed-004", "ord-002-2026", date(2026, 5, 15), "video",
            "Diputada Sofía Guillén", "Frente Amplio",
            "La jornada laboral de cuatro días por tres no debe imponerse "
            "sobre el trabajador. Esta propuesta, en su forma actual, "
            "vulnera derechos consolidados desde el código de 1943.",
            1, 180,
            "https://www.youtube.com/watch?v=demo02", None,
        ),
        (
            "seed-005", "ord-002-2026", date(2026, 5, 15), "video",
            "Diputado Eli Feinzaig", "Liberal Progresista",
            "La jornada 4x3 es una oportunidad para sectores específicos "
            "como turismo y manufactura. Con salvaguardas adecuadas, puede "
            "convivir con el código laboral existente.",
            2, 360,
            "https://www.youtube.com/watch?v=demo02", None,
        ),
        (
            "seed-006", "ord-002-2026", date(2026, 5, 15), "video",
            "Diputada Pilar Cisneros", "PPSD",
            "Sobre seguridad, lo que necesitamos no son más policías sino "
            "policías mejor entrenados, con inteligencia, y con fiscales "
            "que persigan los delitos en lugar de archivarlos.",
            3, 720,
            "https://www.youtube.com/watch?v=demo02", None,
        ),
        (
            "seed-007", "ord-003-2026", date(2026, 5, 16), "acta",
            "Diputado Antonio Ortega", "Frente Amplio",
            "El presupuesto extraordinario destinado al MEP es insuficiente. "
            "La caída en pruebas estandarizadas requiere inversión en "
            "infraestructura, formación docente y conectividad rural.",
            1, None,
            None, "https://www.asamblea.go.cr/actas/2026-05-16.pdf",
        ),
        (
            "seed-008", "ord-003-2026", date(2026, 5, 16), "video",
            "Diputado Daniel Vargas", "Liberación Nacional",
            "Educación pública es nuestra mejor herramienta contra la "
            "desigualdad. Apoyamos el presupuesto extraordinario, pero pedimos "
            "auditoría obligatoria de los recursos del FEES.",
            2, 240,
            "https://www.youtube.com/watch?v=demo03", None,
        ),
    ]

    schema = (
        "intervencion_id STRING, session_id STRING, fecha DATE, fuente STRING, "
        "diputado STRING, fraccion STRING, texto STRING, orden INT, "
        "start_sec INT, video_url STRING, pdf_url STRING"
    )
    seed_df = spark.createDataFrame(seed, schema=schema)
    (seed_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(UNIFIED_TABLE))
    print(
        f"Silver vacío o ausente: poblado {len(seed)} filas de placeholder "
        "en {UNIFIED_TABLE} para que el Vector Search y el App tengan algo "
        "que mostrar."
    )

print(spark.sql(f"SELECT COUNT(*) FROM {UNIFIED_TABLE}").collect()[0][0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Crear Vector Search endpoint (si no existe)
# MAGIC
# MAGIC **Tarda 5-15 min en provisionar.** Lanzarlo lo antes posible.

# COMMAND ----------

vsc = VectorSearchClient(disable_notice=True)

# Crear endpoint si no existe
existing = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if ENDPOINT not in existing:
    print(f"Creando endpoint {ENDPOINT}...")
    vsc.create_endpoint(name=ENDPOINT, endpoint_type="STANDARD")
    # Esto bloquea hasta que está READY
    vsc.wait_for_endpoint(name=ENDPOINT, timeout=900)
    print("Endpoint listo.")
else:
    print(f"Endpoint {ENDPOINT} ya existe.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Crear Delta Sync Index

# COMMAND ----------

existing_indexes = [i["name"] for i in vsc.list_indexes(name=ENDPOINT).get("vector_indexes", [])]

if INDEX not in existing_indexes:
    print(f"Creando index {INDEX}...")
    vsc.create_delta_sync_index(
        endpoint_name=ENDPOINT,
        source_table_name=SOURCE_TABLE,
        index_name=INDEX,
        primary_key="intervencion_id",
        embedding_source_column="texto",
        embedding_model_endpoint_name=EMBEDDING_ENDPOINT,
        pipeline_type="TRIGGERED",
    )
    print("Index creado, disparando primer sync...")
else:
    print(f"Index {INDEX} ya existe.")

idx = vsc.get_index(endpoint_name=ENDPOINT, index_name=INDEX)
idx.sync()
print("Sync disparado. Esperar ~5-10 min para que termine.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Test del index

# COMMAND ----------

import time
time.sleep(30)  # darle un momento al sync

idx = vsc.get_index(endpoint_name=ENDPOINT, index_name=INDEX)
results = idx.similarity_search(
    query_text="seguridad ciudadana",
    columns=["intervencion_id", "diputado", "fecha", "texto", "video_url", "start_sec"],
    num_results=5,
)

for row in results["result"]["data_array"]:
    print(row)
    print("---")
