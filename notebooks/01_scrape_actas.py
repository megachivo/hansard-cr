# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Scrape de actas del Plenario
# MAGIC
# MAGIC **Objetivo:** descargar últimas ~20 actas del Plenario, extraer texto, segmentar por intervención, escribir a `bronze.actas_raw` y `silver.intervenciones`.
# MAGIC
# MAGIC **Fuente:** ver `docs/05-fuentes-urls.md`
# MAGIC
# MAGIC **Tiempo estimado:** 45-60 min (incluye descarga)

# COMMAND ----------

# MAGIC %pip install pdfplumber requests beautifulsoup4 lxml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Imports y constantes
import re
import time
import uuid
import requests
import pdfplumber
from datetime import datetime
from io import BytesIO
from bs4 import BeautifulSoup
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType

CATALOG = "hansard_cr"
INDICE_ACTAS_URL = "https://www.asamblea.go.cr/glcp/actas/forms/plenario.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-CR,es;q=0.9",
}
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_files/actas"
MAX_PDFS = 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Descargar índice y extraer URLs de PDFs

# COMMAND ----------

def listar_pdfs_disponibles(indice_url: str, limite: int = 20) -> list[dict]:
    """
    Scrapea el SharePoint de actas y devuelve [{pdf_url, fecha, session_id}, ...]

    TODO Claude Code: implementar parsing del SharePoint listing.
    El listing es un ASPX con tabla; cada fila tiene un link a un .pdf.
    Estrategia:
      1. requests.get(indice_url, headers=HEADERS)
      2. BeautifulSoup para extraer todos los <a> con href.endswith('.pdf')
      3. Inferir fecha desde texto del link o nombre del archivo
      4. Generar session_id formato: 'ord-NNN-YYYY' o similar
      5. Ordenar por fecha desc, retornar los primeros `limite`
    """
    pass

pdfs = listar_pdfs_disponibles(INDICE_ACTAS_URL, MAX_PDFS)
print(f"Encontrados {len(pdfs)} PDFs")
for p in pdfs[:5]:
    print(p)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Descargar PDFs al Volume y a bronze

# COMMAND ----------

def descargar_pdf(pdf_url: str, dest_dir: str) -> tuple[str, bytes]:
    """Descarga un PDF y lo guarda en el Volume. Devuelve (path_local, bytes)."""
    r = requests.get(pdf_url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    filename = pdf_url.split("/")[-1].replace("%20", "_")
    dest_path = f"{dest_dir}/{filename}"
    with open(dest_path, "wb") as f:
        f.write(r.content)
    return dest_path, r.content

# Crear Volume si no existe
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_files")

rows_bronze = []
for p in pdfs:
    try:
        local_path, pdf_bytes = descargar_pdf(p["pdf_url"], VOLUME_PATH)
        # Extraer texto crudo
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            texto = "\n".join((page.extract_text() or "") for page in pdf.pages)
        rows_bronze.append({
            "pdf_url": p["pdf_url"],
            "session_id": p["session_id"],
            "fecha": p["fecha"],
            "texto_crudo": texto,
            "ingested_at": datetime.utcnow(),
        })
        print(f"OK: {p['session_id']} — {len(texto)} chars")
        time.sleep(2)
    except Exception as e:
        print(f"FAIL: {p.get('session_id')} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Escribir a `bronze.actas_raw`

# COMMAND ----------

bronze_schema = StructType([
    StructField("pdf_url", StringType()),
    StructField("session_id", StringType()),
    StructField("fecha", DateType()),
    StructField("texto_crudo", StringType()),
    StructField("ingested_at", TimestampType()),
])

df_bronze = spark.createDataFrame(rows_bronze, schema=bronze_schema)
(df_bronze.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.actas_raw"))

print(f"bronze.actas_raw: {df_bronze.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Segmentar por intervención
# MAGIC
# MAGIC Regex sobre el texto crudo. Cada vez que aparece un patrón tipo
# MAGIC `EL DIPUTADO X:` o `LA DIPUTADA Y:`, abrimos una nueva intervención.

# COMMAND ----------

# Regex que captura el inicio de una intervención
PATRON_INTERVENCION = re.compile(
    r"^\s*(?:EL|LA)\s+"
    r"(?P<rol>DIPUTAD[OA]|PRESIDENT[EA]|SECRETARI[OA])\s+"
    r"(?P<nombre>[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]+?):\s*",
    re.MULTILINE,
)

def segmentar_intervenciones(texto: str, session_id: str, fecha, pdf_url: str) -> list[dict]:
    """
    Divide el texto del acta en intervenciones individuales.

    TODO Claude Code:
    1. Encontrar todos los matches de PATRON_INTERVENCION (con sus posiciones)
    2. Por cada match, el texto de la intervención va desde el final del match
       hasta el inicio del siguiente match
    3. Limpiar texto (strip, eliminar saltos múltiples)
    4. Filtrar intervenciones <50 caracteres (probable falso positivo)
    5. Devolver lista de dicts con esquema de silver.intervenciones
    """
    pass

# COMMAND ----------

todas_intervenciones = []
for row in rows_bronze:
    inters = segmentar_intervenciones(
        row["texto_crudo"], row["session_id"], row["fecha"], row["pdf_url"]
    )
    todas_intervenciones.extend(inters)

print(f"Total intervenciones segmentadas: {len(todas_intervenciones)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Escribir a `silver.intervenciones`

# COMMAND ----------

silver_schema = StructType([
    StructField("intervencion_id", StringType()),
    StructField("session_id", StringType()),
    StructField("fecha", DateType()),
    StructField("fuente", StringType()),  # 'acta'
    StructField("diputado", StringType()),
    StructField("fraccion", StringType()),
    StructField("texto", StringType()),
    StructField("orden", IntegerType()),
    StructField("start_sec", IntegerType()),  # NULL para fuente=acta
    StructField("video_url", StringType()),
    StructField("pdf_url", StringType()),
])

df_silver = spark.createDataFrame(todas_intervenciones, schema=silver_schema)
(df_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.intervenciones"))

print(f"silver.intervenciones: {df_silver.count()} filas")
display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fuente, COUNT(*) AS n, COUNT(DISTINCT diputado) AS diputados_unicos
# MAGIC FROM hansard_cr.silver.intervenciones
# MAGIC GROUP BY fuente;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT diputado, COUNT(*) AS n
# MAGIC FROM hansard_cr.silver.intervenciones
# MAGIC GROUP BY diputado
# MAGIC ORDER BY n DESC
# MAGIC LIMIT 15;
