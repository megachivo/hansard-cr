# Databricks notebook source
# MAGIC %md
# MAGIC # 02b — Registrar audios subidos manualmente
# MAGIC
# MAGIC **Para qué:** YouTube gatea descargas desde IPs de datacenter (egress de
# MAGIC Databricks serverless), así que `02_pull_youtube_audio` falla en CI. El
# MAGIC workaround es bajar los `mp3` localmente (`scripts/pull_audio_local.py`)
# MAGIC y subirlos al UC Volume manualmente. Este notebook hace el último paso:
# MAGIC poblar `bronze.youtube_raw` para que `02_transcribe_youtube` los vea.
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `mp3`s en `/Volumes/{cat}/{schema}/{vol}/audio/<video_id>.mp3`
# MAGIC - `manifest.csv` en la misma carpeta con columnas:
# MAGIC   `video_id,duration_seg,titulo` (lo genera el script local).
# MAGIC
# MAGIC **Idempotente:** salta video_ids ya presentes en `youtube_raw`.

# COMMAND ----------

import os
import re
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

dbutils.widgets.text("catalog", "hansard_cr")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("volume", "raw_files")
dbutils.widgets.text("manifest_filename", "manifest.csv")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
MANIFEST = dbutils.widgets.get("manifest_filename")

AUDIO_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/audio"
MANIFEST_PATH = f"{AUDIO_DIR}/{MANIFEST}"
TABLE_YT = f"{CATALOG}.{SCHEMA}.youtube_raw"

print(f"Manifest: {MANIFEST_PATH}")
print(f"Tabla destino: {TABLE_YT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Leer manifest

# COMMAND ----------

# Spark CSV con escape para títulos entre comillas dobles.
df_manifest = (
    spark.read
    .option("header", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(MANIFEST_PATH)
    .withColumn("duracion_seg", F.col("duration_seg").cast("int"))
    .drop("duration_seg")
)
display(df_manifest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Parsear título → `session_id`, `fecha`
# MAGIC
# MAGIC Misma lógica que `02_pull_youtube_audio.py`. Si el parseo falla
# MAGIC (título no estándar), session_id y fecha quedan `None` — la fila aún se
# MAGIC inserta para que la transcripción la procese.

# COMMAND ----------

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


def parsear_titulo(titulo):
    if not titulo:
        return None, None
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


filas = []
ingested_at = datetime.utcnow()
for r in df_manifest.collect():
    session_id, fecha = parsear_titulo(r["titulo"])
    audio_path = f"{AUDIO_DIR}/{r['video_id']}.mp3"
    if not os.path.exists(audio_path):
        print(f"SKIP {r['video_id']}  mp3 no está en {audio_path}")
        continue
    filas.append({
        "video_id": r["video_id"],
        "video_url": f"https://www.youtube.com/watch?v={r['video_id']}",
        "titulo": r["titulo"],
        "fecha": fecha,
        "session_id": session_id,
        "duracion_seg": int(r["duracion_seg"]),
        "audio_path": audio_path,
        "ingested_at": ingested_at,
    })
    print(f"OK   {r['video_id']}  {(r['titulo'] or '')[:70]}  → {session_id}")

print(f"\nFilas listas: {len(filas)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Filtrar ya ingestados y appendear

# COMMAND ----------

ya_ingestados = set()
if spark.catalog.tableExists(TABLE_YT):
    ya_ingestados = {
        r["video_id"]
        for r in spark.table(TABLE_YT).select("video_id").distinct().collect()
    }

nuevas = [f for f in filas if f["video_id"] not in ya_ingestados]
print(f"Ya en {TABLE_YT}: {len(ya_ingestados)}")
print(f"Nuevas a insertar: {len(nuevas)}")

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

if nuevas:
    df_nuevas = spark.createDataFrame(nuevas, schema=yt_schema)
    (df_nuevas.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TABLE_YT))
    print(f"Appended {df_nuevas.count()} filas a {TABLE_YT}")
    display(df_nuevas)
else:
    print("Nada nuevo para escribir.")

# COMMAND ----------

if spark.catalog.tableExists(TABLE_YT):
    display(
        spark.table(TABLE_YT)
            .orderBy(F.col("ingested_at").desc())
            .limit(20)
    )
