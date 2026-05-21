# Databricks notebook source
# MAGIC %md
# MAGIC # 02c — Desplegar `system.ai.whisper_large_v3` como Model Serving endpoint
# MAGIC
# MAGIC **Por qué:** la transcripción en GPU local (cluster con `device="cuda"`)
# MAGIC requiere job-cluster con GPU. Más limpio: usar el modelo Marketplace
# MAGIC `system.ai.whisper_large_v3` y servirlo vía endpoint con scale-to-zero.
# MAGIC
# MAGIC **Tiempo:** ~15–20 min de provisión la primera vez. Idempotente: si el
# MAGIC endpoint ya existe, no lo recrea.
# MAGIC
# MAGIC **Output:** endpoint `hansard-whisper` (GPU_SMALL) consumido por
# MAGIC `02_transcribe_youtube.py`.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    ServingModelWorkloadType,
)

# COMMAND ----------

dbutils.widgets.text("endpoint_name", "hansard-whisper")
dbutils.widgets.text("model_full_name", "system.ai.whisper_large_v3")
dbutils.widgets.text("model_version", "3")

ENDPOINT = dbutils.widgets.get("endpoint_name")
MODEL_FULL_NAME = dbutils.widgets.get("model_full_name")
MODEL_VERSION = dbutils.widgets.get("model_version")

print(f"endpoint={ENDPOINT} model={MODEL_FULL_NAME} v{MODEL_VERSION}")

# COMMAND ----------

w = WorkspaceClient()

existing = {e.name for e in w.serving_endpoints.list()}
if ENDPOINT in existing:
    print(f"Endpoint {ENDPOINT} ya existe — nada que crear.")
else:
    print(f"Creando endpoint {ENDPOINT}...")
    w.serving_endpoints.create(
        name=ENDPOINT,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=MODEL_FULL_NAME,
                    entity_version=MODEL_VERSION,
                    workload_size="Small",
                    workload_type=ServingModelWorkloadType.GPU_SMALL,
                    scale_to_zero_enabled=True,
                )
            ],
        ),
    )
    print("Solicitud de creación enviada. Provisión tarda 15-20 min.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Esperar a que esté READY
# MAGIC
# MAGIC Bloquea hasta que el endpoint esté listo para queries (o time-out a los
# MAGIC 30 min). Si revientas la celda y la vuelves a correr, sigue esperando.

# COMMAND ----------

ep = w.serving_endpoints.wait_get_serving_endpoint_not_updating(
    name=ENDPOINT,
)
print(f"Estado: {ep.state.ready} | config_update={ep.state.config_update}")
