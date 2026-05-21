# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Pull de audio desde el canal de YouTube
# MAGIC
# MAGIC **Objetivo:** descubrir videos recientes del canal `@AsambleaCRC`, descargar
# MAGIC el audio como `mp3`, guardarlo en el UC Volume `bronze.raw_files/audio/` y
# MAGIC registrar la metadata en `bronze.youtube_raw`.
# MAGIC
# MAGIC **Este notebook NO transcribe.** La transcripción vive en `03_transcribe_youtube`.
# MAGIC
# MAGIC **El volume y el schema los crea el Asset Bundle (`databricks.yml`).** Este
# MAGIC notebook asume que ya existen — si fallan los writes, corré
# MAGIC `databricks bundle deploy` primero.
# MAGIC
# MAGIC **Cluster recomendado:** un single-node CPU pequeño alcanza (no se transcribe
# MAGIC acá). `ffmpeg` viene preinstalado en DBR ML; en runtimes "estándar" lo
# MAGIC instalamos vía apt.
# MAGIC
# MAGIC **Fuentes y patrones:** `docs/05-fuentes-urls.md`.

# COMMAND ----------

# MAGIC %pip install -U yt-dlp --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sh which ffmpeg || (apt-get update -qq && apt-get install -y -qq ffmpeg)

# COMMAND ----------

import json
import os
import re
import subprocess
import time
from datetime import date, datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros (widgets)

# COMMAND ----------

dbutils.widgets.text("catalog", "hansard_cr")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("volume", "raw_files")
dbutils.widgets.text("channel_url", "https://www.youtube.com/@AsambleaCRC/videos")
dbutils.widgets.text("max_videos", "10")
dbutils.widgets.text("limite_seg", "3600")
dbutils.widgets.text("min_duracion_seg", "3600")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
CHANNEL_URL = dbutils.widgets.get("channel_url")
MAX_VIDEOS = int(dbutils.widgets.get("max_videos"))
LIMITE_SEG = int(dbutils.widgets.get("limite_seg"))
MIN_DURACION_SEG = int(dbutils.widgets.get("min_duracion_seg"))

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
AUDIO_DIR = f"{VOLUME_ROOT}/audio"
TABLE_YT = f"{CATALOG}.{SCHEMA}.youtube_raw"

# `os.makedirs` walks up to `/Volumes/{cat}/{schema}` and fails with EOPNOTSUPP
# on serverless FUSE. `dbutils.fs.mkdirs` resolves through the UC abstraction.
dbutils.fs.mkdirs(AUDIO_DIR)
print(f"Audio dir: {AUDIO_DIR}")
print(f"Tabla destino: {TABLE_YT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Descubrir IDs de video desde el canal
# MAGIC
# MAGIC Usamos `yt-dlp --flat-playlist` que no descarga audio, sólo lista entradas.
# MAGIC Filtramos por duración ≥ `MIN_DURACION_SEG` (default 1h) — las sesiones del
# MAGIC plenario duran horas; comisiones, clips y resúmenes caen por debajo.

# COMMAND ----------

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _run_ytdlp(extra_args: list[str], url: str) -> subprocess.CompletedProcess:
    """Wrap yt-dlp call so stderr is surfaced on failure (otherwise serverless
    swallows it as a bare CalledProcessError)."""
    cmd = [
        "yt-dlp",
        "--user-agent", USER_AGENT,
        "--no-warnings",
        *extra_args,
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"yt-dlp failed (exit {result.returncode})\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}\n"
            f"stdout: {result.stdout.strip()[:500]}"
        )
    return result


def listar_videos_canal(url: str, max_n: int) -> list[dict]:
    """Devuelve [{video_id, titulo, duration_seg}] desde una URL de canal/playlist."""
    result = _run_ytdlp(
        ["--flat-playlist", "--dump-json", "--playlist-end", str(max_n)],
        url,
    )
    entradas = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        d = json.loads(line)
        entradas.append({
            "video_id": d.get("id"),
            "titulo": d.get("title", ""),
            "duration_seg": int(d.get("duration") or 0),
        })
    return entradas


PATTERN_TITULO = re.compile(
    r"sesi[oó]n\s+(?P<tipo>ordinaria|extraordinaria|solemne)"
    r"(?:\s+#?(?P<numero>\d+))?,?\s+"
    r"(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)?\s*"
    r"(?P<dia>\d+)\s+(?:de\s+)?(?P<mes>\w+)\s+(?:de\s+)?(?P<anio>\d{4})",
    re.IGNORECASE,
)
MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "setiembre": 9, "septiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


def parsear_titulo(titulo: str) -> tuple[str | None, date | None]:
    m = PATTERN_TITULO.search(titulo)
    if not m:
        return None, None
    tipo = {"ordinaria": "ord", "extraordinaria": "ext", "solemne": "sol"}[m["tipo"].lower()]
    mes = MESES.get(m["mes"].lower())
    if not mes:
        return None, None
    fecha = date(int(m["anio"]), mes, int(m["dia"]))
    session_id = f"{tipo}-{int(m['numero'] or 0):03d}-{m['anio']}"
    return session_id, fecha


candidatos = listar_videos_canal(CHANNEL_URL, MAX_VIDEOS)
print(f"Candidatos del canal: {len(candidatos)}")

candidatos = [c for c in candidatos if c["duration_seg"] >= MIN_DURACION_SEG]
print(f"Tras filtrar duración ≥{MIN_DURACION_SEG}s: {len(candidatos)}")

for c in candidatos[:10]:
    print(f"  {c['video_id']}  {c['titulo'][:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Saltar videos ya descargados (idempotencia)

# COMMAND ----------

ya_ingestados: set[str] = set()
if spark.catalog.tableExists(TABLE_YT):
    ya_ingestados = {
        r["video_id"]
        for r in spark.table(TABLE_YT).select("video_id").distinct().collect()
    }
print(f"Ya en {TABLE_YT}: {len(ya_ingestados)}")

pendientes = [c for c in candidatos if c["video_id"] not in ya_ingestados]
print(f"Pendientes a descargar: {len(pendientes)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Descargar mp3 y cortar a `LIMITE_SEG`

# COMMAND ----------

def descargar_audio(video_id: str, dest_dir: str, limite_seg: int) -> dict:
    """Descarga audio como mp3, lo corta a `limite_seg` y devuelve metadata."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    out_template = f"{dest_dir}/{video_id}.%(ext)s"
    audio_path = f"{dest_dir}/{video_id}.mp3"

    result = _run_ytdlp(
        [
            "-x",
            "--audio-format", "mp3",
            "--audio-quality", "5",
            "--no-playlist",
            "-o", out_template,
            "--print-json",
        ],
        url,
    )
    meta = json.loads(result.stdout.strip().split("\n")[-1])

    if meta.get("duration", 0) > limite_seg:
        cortado = f"{dest_dir}/{video_id}_cut.mp3"
        subprocess.run(
            ["ffmpeg", "-y", "-i", audio_path, "-t", str(limite_seg),
             "-c", "copy", cortado],
            check=True, capture_output=True,
        )
        os.replace(cortado, audio_path)

    return {
        "video_id": video_id,
        "video_url": url,
        "titulo": meta.get("title", ""),
        "audio_path": audio_path,
        "duracion_seg": min(int(meta.get("duration") or 0), limite_seg),
    }


metadatos: list[dict] = []
for c in pendientes:
    try:
        m = descargar_audio(c["video_id"], AUDIO_DIR, LIMITE_SEG)
        session_id, fecha = parsear_titulo(m["titulo"])
        m["session_id"] = session_id
        m["fecha"] = fecha
        m["ingested_at"] = datetime.utcnow()
        metadatos.append(m)
        print(f"OK  {c['video_id']}  {m['titulo'][:70]}  → {session_id}")
        time.sleep(5)  # rate limit (ver docs/05-fuentes-urls.md)
    except subprocess.CalledProcessError as e:
        print(f"FAIL {c['video_id']}  stderr: {e.stderr[:200]}")
    except Exception as e:
        print(f"FAIL {c['video_id']}  {type(e).__name__}: {e}")

print(f"\nDescargados: {len(metadatos)} / {len(pendientes)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Append a `bronze.youtube_raw`

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

if metadatos:
    df_yt = spark.createDataFrame(metadatos, schema=yt_schema)
    (df_yt.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TABLE_YT))
    print(f"Appended {df_yt.count()} filas a {TABLE_YT}")
    display(df_yt)
else:
    print("Nada nuevo para escribir.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen

# COMMAND ----------

if spark.catalog.tableExists(TABLE_YT):
    display(
        spark.table(TABLE_YT)
            .orderBy(F.col("ingested_at").desc())
            .limit(20)
    )
