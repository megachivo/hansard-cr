# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Transcripción de videos del canal de YouTube
# MAGIC
# MAGIC **Objetivo:** descargar audio de 3-5 sesiones recientes del canal `@AsambleaCRC`,
# MAGIC transcribir con `faster-whisper`, y poblar `silver.transcripciones`.
# MAGIC
# MAGIC **Requisitos de cluster:**
# MAGIC - GPU (T4 o A10 alcanza)
# MAGIC - Databricks Runtime 15.x ML o superior
# MAGIC
# MAGIC **Tiempo estimado:** 30-45 min para 5 videos limitados a 60 min cada uno.

# COMMAND ----------

# MAGIC %pip install yt-dlp faster-whisper --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import re
import subprocess
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType

CATALOG = "hansard_cr"
VOLUME_AUDIO = f"/Volumes/{CATALOG}/bronze/raw_files/audio"
LIMITE_SEG = 3600  # primeros 60 min de cada video

# Seed manual: videos recientes a procesar
# (verificar al inicio del hackathon que estos IDs estén vigentes)
VIDEO_IDS = [
    "6RvmcG2CzqQ",  # ordinaria #96, 03 feb 2025
    "82f-NpJeKeE",  # extraordinaria #52, 03 abr 2025
    # añadir 2-3 más al iniciar el hackathon
]

os.makedirs(VOLUME_AUDIO, exist_ok=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Descargar audio + metadata

# COMMAND ----------

def descargar_audio_yt(video_id: str, dest_dir: str, limite_seg: int = 3600) -> dict:
    """
    Descarga audio del video como mp3 cortado en los primeros `limite_seg` segundos.

    Devuelve dict con: video_id, video_url, titulo, audio_path, duracion_seg
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    audio_path = f"{dest_dir}/{video_id}.mp3"

    # Descargar audio completo primero (yt-dlp no corta nativamente)
    cmd = [
        "yt-dlp",
        "-x", "--audio-format", "mp3",
        "--audio-quality", "5",
        "-o", audio_path.replace(".mp3", ".%(ext)s"),
        "--print-json",
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    import json
    meta = json.loads(result.stdout.split("\n")[0])

    # Cortar a `limite_seg` con ffmpeg
    if meta["duration"] > limite_seg:
        cortado = audio_path.replace(".mp3", "_cut.mp3")
        subprocess.run(
            ["ffmpeg", "-y", "-i", audio_path, "-t", str(limite_seg),
             "-c", "copy", cortado],
            check=True, capture_output=True,
        )
        os.replace(cortado, audio_path)

    return {
        "video_id": video_id,
        "video_url": url,
        "titulo": meta["title"],
        "audio_path": audio_path,
        "duracion_seg": min(meta["duration"], limite_seg),
    }

# COMMAND ----------

PATTERN_TITULO = re.compile(
    r"sesión\s+(?P<tipo>ordinaria|extraordinaria|solemne)"
    r"(?:\s+#(?P<numero>\d+))?,?\s+"
    r"(?:lunes|martes|miércoles|jueves|viernes|sábado|domingo)?\s*"
    r"(?P<dia>\d+)\s+(?:de\s+)?(?P<mes>\w+)\s+(?:de\s+)?(?P<anio>\d{4})",
    re.IGNORECASE,
)
MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "setiembre": 9, "septiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}

def parsear_titulo(titulo: str) -> tuple[str | None, date | None]:
    """
    Extrae session_id y fecha del título del video.
    'Plenario Legislativo, sesión ordinaria #96, 03 febrero 2025' →
        ('ord-096-2025', date(2025,2,3))
    """
    m = PATTERN_TITULO.search(titulo)
    if not m:
        return None, None
    tipo = {"ordinaria": "ord", "extraordinaria": "ext", "solemne": "sol"}[m["tipo"].lower()]
    numero = m["numero"] or "000"
    mes = MESES.get(m["mes"].lower())
    if not mes:
        return None, None
    fecha = date(int(m["anio"]), mes, int(m["dia"]))
    session_id = f"{tipo}-{int(numero):03d}-{m['anio']}"
    return session_id, fecha

# COMMAND ----------

metadatos = []
for vid in VIDEO_IDS:
    try:
        meta = descargar_audio_yt(vid, VOLUME_AUDIO, LIMITE_SEG)
        session_id, fecha = parsear_titulo(meta["titulo"])
        meta["session_id"] = session_id
        meta["fecha"] = fecha
        meta["ingested_at"] = datetime.utcnow()
        metadatos.append(meta)
        print(f"OK: {vid} — {meta['titulo']} → {session_id}")
    except Exception as e:
        print(f"FAIL: {vid} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Escribir bronze.youtube_raw

# COMMAND ----------

yt_schema = StructType([
    StructField("video_id", StringType()),
    StructField("video_url", StringType()),
    StructField("titulo", StringType()),
    StructField("fecha", DateType()),
    StructField("session_id", StringType()),
    StructField("duracion_seg", IntegerType()),
    StructField("audio_path", StringType()),
    StructField("ingested_at", TimestampType()),
])

df_yt = spark.createDataFrame(metadatos, schema=yt_schema)
(df_yt.write.mode("append").saveAsTable(f"{CATALOG}.bronze.youtube_raw"))
display(df_yt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Transcribir con faster-whisper

# COMMAND ----------

from faster_whisper import WhisperModel

# large-v3 da mejor calidad para español; medium si la GPU es chica
modelo = WhisperModel("large-v3", device="cuda", compute_type="float16")

def transcribir(audio_path: str, video_id: str) -> list[dict]:
    """Devuelve lista de chunks {video_id, start_sec, end_sec, texto, confidence}."""
    segmentos, info = modelo.transcribe(
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

# COMMAND ----------

todos_chunks = []
for meta in metadatos:
    print(f"Transcribiendo {meta['video_id']}...")
    chunks = transcribir(meta["audio_path"], meta["video_id"])
    todos_chunks.extend(chunks)
    print(f"  → {len(chunks)} segmentos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Escribir silver.transcripciones

# COMMAND ----------

trans_schema = StructType([
    StructField("video_id", StringType()),
    StructField("start_sec", IntegerType()),
    StructField("end_sec", IntegerType()),
    StructField("texto", StringType()),
    StructField("confidence", FloatType()),
])

df_trans = spark.createDataFrame(todos_chunks, schema=trans_schema)
(df_trans.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.transcripciones"))

print(f"silver.transcripciones: {df_trans.count()} chunks")
display(df_trans.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Mergear chunks en "intervenciones" (sin diarization)
# MAGIC
# MAGIC Para el demo, agrupamos chunks contiguos en bloques de ~2-3 min y los marcamos
# MAGIC con diputado='(de video, sin identificar)'. La feature de diarization se deja
# MAGIC para fase 2.

# COMMAND ----------

import uuid

def merge_chunks_en_intervenciones(
    chunks: list[dict], session_id: str, fecha, video_url: str,
    ventana_seg: int = 180
) -> list[dict]:
    """Agrupa chunks consecutivos en intervenciones de ~ventana_seg segundos."""
    if not chunks:
        return []
    chunks = sorted(chunks, key=lambda c: c["start_sec"])
    grupos = []
    actual = {
        "intervencion_id": str(uuid.uuid4()),
        "session_id": session_id,
        "fecha": fecha,
        "fuente": "video",
        "diputado": "(de video, sin identificar)",
        "fraccion": None,
        "texto": chunks[0]["texto"],
        "orden": 0,
        "start_sec": chunks[0]["start_sec"],
        "video_url": video_url,
        "pdf_url": None,
        "_inicio": chunks[0]["start_sec"],
    }
    for c in chunks[1:]:
        if c["start_sec"] - actual["_inicio"] < ventana_seg:
            actual["texto"] += " " + c["texto"]
        else:
            grupos.append({k: v for k, v in actual.items() if not k.startswith("_")})
            actual = {
                "intervencion_id": str(uuid.uuid4()),
                "session_id": session_id,
                "fecha": fecha,
                "fuente": "video",
                "diputado": "(de video, sin identificar)",
                "fraccion": None,
                "texto": c["texto"],
                "orden": len(grupos),
                "start_sec": c["start_sec"],
                "video_url": video_url,
                "pdf_url": None,
                "_inicio": c["start_sec"],
            }
    grupos.append({k: v for k, v in actual.items() if not k.startswith("_")})
    return grupos

# COMMAND ----------

intervenciones_video = []
for meta in metadatos:
    chunks_video = [c for c in todos_chunks if c["video_id"] == meta["video_id"]]
    intervenciones_video.extend(merge_chunks_en_intervenciones(
        chunks_video, meta["session_id"], meta["fecha"], meta["video_url"]
    ))

print(f"Intervenciones derivadas de video: {len(intervenciones_video)}")

# Append a silver.intervenciones
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

df_video_inters = spark.createDataFrame(intervenciones_video, schema=silver_schema)
(df_video_inters.write.mode("append").saveAsTable(f"{CATALOG}.silver.intervenciones"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fuente, COUNT(*) FROM hansard_cr.silver.intervenciones GROUP BY fuente;
