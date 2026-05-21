# Databricks notebook source
# MAGIC %md
# MAGIC # 02b — Transcripción de audios de YouTube
# MAGIC
# MAGIC **Objetivo:** leer `bronze.youtube_raw` (poblada por `02_pull_youtube_audio`),
# MAGIC transcribir los `mp3` del volume con `faster-whisper`, y escribir
# MAGIC `silver.transcripciones` + `silver.intervenciones` (sin diarization).
# MAGIC
# MAGIC **Pre-requisito:** correr `02_pull_youtube_audio` primero — este notebook
# MAGIC NO descarga audio.
# MAGIC
# MAGIC **Cluster:** GPU (T4/A10) con DBR ML 15.x+.
# MAGIC
# MAGIC **Tiempo estimado:** ~10 min por hora de audio en T4 con `large-v3`.

# COMMAND ----------

# MAGIC %pip install faster-whisper --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "hansard_cr")
dbutils.widgets.text("modelo_whisper", "large-v3")
dbutils.widgets.text("ventana_intervencion_seg", "180")

CATALOG = dbutils.widgets.get("catalog")
MODELO = dbutils.widgets.get("modelo_whisper")
VENTANA = int(dbutils.widgets.get("ventana_intervencion_seg"))

TABLE_YT = f"{CATALOG}.bronze.youtube_raw"
TABLE_TRANS = f"{CATALOG}.silver.transcripciones"
TABLE_INTERS = f"{CATALOG}.silver.intervenciones"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Seleccionar videos pendientes

# COMMAND ----------

videos = spark.table(TABLE_YT).select(
    "video_id", "video_url", "session_id", "fecha", "audio_path"
)

if spark.catalog.tableExists(TABLE_TRANS):
    ya_transcritos = (
        spark.table(TABLE_TRANS).select("video_id").distinct()
    )
    pendientes = videos.join(ya_transcritos, on="video_id", how="left_anti")
else:
    pendientes = videos

pendientes_list = pendientes.collect()
print(f"Videos pendientes de transcribir: {len(pendientes_list)}")
for v in pendientes_list[:10]:
    print(f"  {v.video_id}  {v.audio_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Cargar Whisper y transcribir

# COMMAND ----------

from faster_whisper import WhisperModel

modelo = WhisperModel(MODELO, device="cuda", compute_type="float16")


def transcribir(audio_path: str, video_id: str) -> list[dict]:
    segmentos, _info = modelo.transcribe(
        audio_path,
        language="es",
        vad_filter=True,
        beam_size=5,
    )
    return [
        {
            "video_id": video_id,
            "start_sec": int(seg.start),
            "end_sec": int(seg.end),
            "texto": seg.text.strip(),
            "confidence": float(seg.avg_logprob),
        }
        for seg in segmentos
    ]


todos_chunks: list[dict] = []
for v in pendientes_list:
    print(f"Transcribiendo {v.video_id}...")
    try:
        chunks = transcribir(v.audio_path, v.video_id)
        todos_chunks.extend(chunks)
        print(f"  → {len(chunks)} segmentos")
    except Exception as e:
        print(f"  FAIL {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Append a `silver.transcripciones`

# COMMAND ----------

trans_schema = StructType([
    StructField("video_id", StringType()),
    StructField("start_sec", IntegerType()),
    StructField("end_sec", IntegerType()),
    StructField("texto", StringType()),
    StructField("confidence", FloatType()),
])

if todos_chunks:
    df_trans = spark.createDataFrame(todos_chunks, schema=trans_schema)
    (df_trans.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TABLE_TRANS))
    print(f"{TABLE_TRANS}: +{df_trans.count()} chunks")
    display(df_trans.limit(10))
else:
    print("Sin chunks nuevos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Agrupar chunks en intervenciones de ~`VENTANA` seg
# MAGIC
# MAGIC Sin diarization para el demo. Cada bloque queda etiquetado como
# MAGIC `diputado='(de video, sin identificar)'`.

# COMMAND ----------

def merge_chunks(
    chunks: list[dict], session_id: str, fecha, video_url: str, ventana_seg: int
) -> list[dict]:
    if not chunks:
        return []
    chunks = sorted(chunks, key=lambda c: c["start_sec"])

    def nuevo(c, orden):
        return {
            "intervencion_id": str(uuid.uuid4()),
            "session_id": session_id,
            "fecha": fecha,
            "fuente": "video",
            "diputado": "(de video, sin identificar)",
            "fraccion": None,
            "texto": c["texto"],
            "orden": orden,
            "start_sec": c["start_sec"],
            "video_url": video_url,
            "pdf_url": None,
            "_inicio": c["start_sec"],
        }

    grupos: list[dict] = []
    actual = nuevo(chunks[0], 0)
    for c in chunks[1:]:
        if c["start_sec"] - actual["_inicio"] < ventana_seg:
            actual["texto"] += " " + c["texto"]
        else:
            grupos.append({k: v for k, v in actual.items() if not k.startswith("_")})
            actual = nuevo(c, len(grupos))
    grupos.append({k: v for k, v in actual.items() if not k.startswith("_")})
    return grupos


intervenciones: list[dict] = []
for v in pendientes_list:
    chunks_v = [c for c in todos_chunks if c["video_id"] == v.video_id]
    intervenciones.extend(
        merge_chunks(chunks_v, v.session_id, v.fecha, v.video_url, VENTANA)
    )

print(f"Intervenciones derivadas de video: {len(intervenciones)}")

# COMMAND ----------

silver_schema = StructType([
    StructField("intervencion_id", StringType()),
    StructField("session_id", StringType()),
    StructField("fecha", DateType()),
    StructField("fuente", StringType()),
    StructField("diputado", StringType()),
    StructField("fraccion", StringType()),
    StructField("texto", StringType()),
    StructField("orden", IntegerType()),
    StructField("start_sec", IntegerType()),
    StructField("video_url", StringType()),
    StructField("pdf_url", StringType()),
])

if intervenciones:
    df_inters = spark.createDataFrame(intervenciones, schema=silver_schema)
    (df_inters.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TABLE_INTERS))
    print(f"{TABLE_INTERS}: +{df_inters.count()} filas")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fuente, COUNT(*) FROM hansard_cr.silver.intervenciones GROUP BY fuente;
