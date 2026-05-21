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

CATALOG = "hansard_cr"
ENDPOINT = "hansard-cr-endpoint"
INDEX = f"{CATALOG}.gold.intervenciones_idx"
SOURCE_TABLE = f"{CATALOG}.gold.intervenciones_unified"
EMBEDDING_ENDPOINT = "databricks-gte-large-en"  # cambiar a multilingual-e5 si está disponible

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Crear vista/tabla `gold.intervenciones_unified`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hansard_cr.gold.intervenciones_unified (
# MAGIC   intervencion_id STRING NOT NULL,
# MAGIC   session_id STRING,
# MAGIC   fecha DATE,
# MAGIC   fuente STRING,
# MAGIC   diputado STRING,
# MAGIC   fraccion STRING,
# MAGIC   texto STRING,
# MAGIC   orden INT,
# MAGIC   start_sec INT,
# MAGIC   video_url STRING,
# MAGIC   pdf_url STRING,
# MAGIC   CONSTRAINT pk_intervenciones PRIMARY KEY (intervencion_id)
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# Poblar desde silver
spark.sql(f"""
INSERT OVERWRITE {CATALOG}.gold.intervenciones_unified
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
FROM {CATALOG}.silver.intervenciones
WHERE LENGTH(texto) > 50
""")

print(spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.gold.intervenciones_unified").collect()[0][0])

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
