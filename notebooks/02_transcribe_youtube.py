# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Transcripción de audios vía Model Serving
# MAGIC
# MAGIC **Objetivo:** leer `bronze.youtube_raw`, partir cada mp3 en chunks de 30s
# MAGIC con ffmpeg, mandar cada chunk al endpoint `hansard-whisper` (modelo
# MAGIC `system.ai.whisper_large_v3` v3 detrás de GPU_SMALL serving) y escribir
# MAGIC `silver.transcripciones` + `silver.intervenciones`.
# MAGIC
# MAGIC **Por qué no `device="cuda"` local:** evita pedir GPU al job cluster.
# MAGIC El endpoint corre en GPU compartido, scale-to-zero. El notebook puede
# MAGIC vivir en serverless o cluster CPU sin problemas.
# MAGIC
# MAGIC **Pre-requisitos:**
# MAGIC - `02_pull_youtube_audio` (o `02b_register_uploaded_audio`) corrió y
# MAGIC   `bronze.youtube_raw` tiene filas con `audio_path` apuntando a mp3 en el
# MAGIC   UC Volume.
# MAGIC - `02c_deploy_whisper_endpoint` corrió y el endpoint `hansard-whisper`
# MAGIC   está READY.

# COMMAND ----------

# MAGIC %pip install --quiet imageio-ffmpeg databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import base64
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import imageio_ffmpeg
from databricks.sdk import WorkspaceClient
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
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("endpoint_name", "hansard-whisper")
dbutils.widgets.text("chunk_seg", "30")
dbutils.widgets.text("ventana_intervencion_seg", "180")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
ENDPOINT = dbutils.widgets.get("endpoint_name")
CHUNK_SEG = int(dbutils.widgets.get("chunk_seg"))
VENTANA = int(dbutils.widgets.get("ventana_intervencion_seg"))

TABLE_YT = f"{CATALOG}.{SCHEMA_BRONZE}.youtube_raw"
TABLE_TRANS = f"{CATALOG}.{SCHEMA_SILVER}.transcripciones"
TABLE_INTERS = f"{CATALOG}.{SCHEMA_SILVER}.intervenciones"

FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()
print(f"ffmpeg: {FFMPEG}")
print(f"endpoint: {ENDPOINT}")
print(f"tables: {TABLE_YT}, {TABLE_TRANS}, {TABLE_INTERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Seleccionar videos pendientes

# COMMAND ----------

videos = spark.table(TABLE_YT).select(
    "video_id", "video_url", "session_id", "fecha", "audio_path"
)

if spark.catalog.tableExists(TABLE_TRANS):
    ya_transcritos = spark.table(TABLE_TRANS).select("video_id").distinct()
    pendientes = videos.join(ya_transcritos, on="video_id", how="left_anti")
else:
    pendientes = videos

pendientes_list = pendientes.collect()
print(f"Videos pendientes: {len(pendientes_list)}")
for v in pendientes_list[:10]:
    print(f"  {v.video_id}  {v.audio_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Funciones helper: chunking + endpoint query

# COMMAND ----------

w = WorkspaceClient()


def chunk_mp3(audio_path: str, chunk_seg: int, out_dir: Path) -> list[Path]:
    """Split mp3 into N-second segments without re-encoding."""
    out_dir.mkdir(parents=True, exist_ok=True)
    pattern = str(out_dir / "chunk_%05d.mp3")
    cmd = [
        FFMPEG,
        "-y",
        "-i", audio_path,
        "-f", "segment",
        "-segment_time", str(chunk_seg),
        "-c", "copy",
        "-reset_timestamps", "1",
        pattern,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(
            f"ffmpeg segment failed (exit {res.returncode}): {res.stderr.strip()}"
        )
    return sorted(out_dir.glob("chunk_*.mp3"))


def query_whisper(audio_bytes: bytes, retries: int = 3) -> str:
    """Send raw mp3 bytes to the Whisper serving endpoint."""
    b64 = base64.b64encode(audio_bytes).decode("utf-8")
    last_exc: Exception | None = None
    for i in range(retries):
        try:
            resp = w.serving_endpoints.query(
                name=ENDPOINT,
                inputs=[b64],
            )
            # MLflow transformers ASR returns predictions as list of strings.
            preds = resp.predictions
            if isinstance(preds, list) and preds:
                first = preds[0]
                if isinstance(first, str):
                    return first
                if isinstance(first, dict):
                    # Some flavors return {"text": "..."} per row.
                    return first.get("text") or first.get("output") or ""
            return ""
        except Exception as e:
            last_exc = e
            wait = 2 ** i
            print(f"    query retry {i + 1}/{retries} after {wait}s ({e})")
            time.sleep(wait)
    raise RuntimeError(f"whisper endpoint failed after {retries} retries: {last_exc}")


def transcribir_video(audio_path: str, video_id: str) -> list[dict]:
    if not os.path.exists(audio_path):
        print(f"  SKIP {video_id} mp3 missing: {audio_path}")
        return []
    with tempfile.TemporaryDirectory(prefix=f"wh_{video_id}_") as tmp:
        tmp_dir = Path(tmp)
        chunks = chunk_mp3(audio_path, CHUNK_SEG, tmp_dir)
        rows: list[dict] = []
        for idx, chunk_path in enumerate(chunks):
            start_sec = idx * CHUNK_SEG
            end_sec = start_sec + CHUNK_SEG
            try:
                text = query_whisper(chunk_path.read_bytes()).strip()
            except Exception as e:
                print(f"    chunk {idx} fallo: {e}")
                continue
            if not text:
                continue
            rows.append({
                "video_id": video_id,
                "start_sec": start_sec,
                "end_sec": end_sec,
                "texto": text,
                "confidence": None,  # endpoint no expone log-prob por segmento
            })
            if idx % 20 == 0:
                print(f"    chunk {idx}/{len(chunks)} ok ({len(text)} chars)")
        return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Transcribir cada video pendiente

# COMMAND ----------

todos_chunks: list[dict] = []
for v in pendientes_list:
    print(f"Transcribiendo {v.video_id} ({v.audio_path})...")
    t0 = time.time()
    try:
        rows = transcribir_video(v.audio_path, v.video_id)
        todos_chunks.extend(rows)
        dt = time.time() - t0
        print(f"  → {len(rows)} chunks en {dt:.1f}s")
    except Exception as e:
        print(f"  FAIL {type(e).__name__}: {e}")

print(f"\nTotal chunks: {len(todos_chunks)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Append a `silver.transcripciones`

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
# MAGIC ## Paso 5 — Agrupar chunks en intervenciones de ~`VENTANA` seg
# MAGIC
# MAGIC Sin diarization. Cada bloque queda etiquetado como
# MAGIC `diputado='(de video, sin identificar)'` — el Vector Search lo va a
# MAGIC indexar igual y la UI muestra fuente "video" con `start_sec` para
# MAGIC linkear al timestamp del YouTube.

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

if spark.catalog.tableExists(TABLE_INTERS):
    display(
        spark.sql(
            f"SELECT fuente, COUNT(*) AS n FROM {TABLE_INTERS} GROUP BY fuente"
        )
    )
