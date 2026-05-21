# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — (OPCIONAL) Registrar Whisper como Model Serving endpoint
# MAGIC
# MAGIC **NO ES BLOQUEANTE PARA EL DEMO.** Solo hacer esto si sobran 30 min al final.
# MAGIC
# MAGIC **Objetivo:** envolver `faster-whisper` en un MLflow pyfunc y desplegarlo como
# MAGIC Model Serving endpoint con autoscale GPU. Permite que el App o cualquier
# MAGIC otra app llame a Whisper vía REST.

# COMMAND ----------

# MAGIC %pip install mlflow faster-whisper --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import mlflow.pyfunc
from mlflow.models import infer_signature

CATALOG = "hansard_cr"
MODEL_NAME = f"{CATALOG}.gold.hansard_whisper"
ENDPOINT_NAME = "hansard-whisper"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir el wrapper pyfunc

# COMMAND ----------

class WhisperWrapper(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        from faster_whisper import WhisperModel
        self.model = WhisperModel("large-v3", device="cuda", compute_type="float16")

    def predict(self, context, model_input):
        """
        model_input: DataFrame con columna 'audio_b64' (audio mp3 en base64)
                     o 'audio_url' (URL a descargar)
        Devuelve: DataFrame con cols [start_sec, end_sec, texto, confidence]
        """
        import base64
        import tempfile
        import pandas as pd

        rows = []
        for _, row in model_input.iterrows():
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
                if "audio_b64" in row and row["audio_b64"]:
                    f.write(base64.b64decode(row["audio_b64"]))
                elif "audio_url" in row and row["audio_url"]:
                    import requests
                    f.write(requests.get(row["audio_url"], timeout=120).content)
                else:
                    continue
                tmp_path = f.name

            segs, _ = self.model.transcribe(tmp_path, language="es", vad_filter=True)
            for s in segs:
                rows.append({
                    "start_sec": int(s.start),
                    "end_sec": int(s.end),
                    "texto": s.text.strip(),
                    "confidence": float(s.avg_logprob),
                })

        return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registrar el modelo en Unity Catalog

# COMMAND ----------

import pandas as pd
mlflow.set_registry_uri("databricks-uc")

sample_input = pd.DataFrame([{"audio_url": "https://example.com/audio.mp3"}])
sample_output = pd.DataFrame([{
    "start_sec": 0, "end_sec": 5, "texto": "ejemplo", "confidence": -0.5
}])

with mlflow.start_run() as run:
    mlflow.pyfunc.log_model(
        artifact_path="whisper_model",
        python_model=WhisperWrapper(),
        registered_model_name=MODEL_NAME,
        signature=infer_signature(sample_input, sample_output),
        pip_requirements=[
            "faster-whisper",
            "requests",
            "pandas",
        ],
    )

print(f"Modelo registrado en {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear el endpoint
# MAGIC
# MAGIC **Importante:** requiere workload type GPU. Verificar disponibilidad en la región.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput, ServingModelWorkloadType
)

w = WorkspaceClient()

# Obtener última versión del modelo
client = mlflow.tracking.MlflowClient()
latest = client.get_model_version_by_alias(MODEL_NAME, "@latest") if False else \
         max(client.search_model_versions(f"name='{MODEL_NAME}'"), key=lambda mv: int(mv.version))

w.serving_endpoints.create(
    name=ENDPOINT_NAME,
    config=EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name=MODEL_NAME,
                entity_version=latest.version,
                workload_size="Small",
                workload_type=ServingModelWorkloadType.GPU_SMALL,
                scale_to_zero_enabled=True,
            )
        ],
    ),
)

print(f"Endpoint {ENDPOINT_NAME} en creación. Tarda ~15-20 min con GPU.")
