# Databricks notebook source
# MAGIC %md
# MAGIC # E7.1 · Ingesta a Bronze
# MAGIC **Cierra con:** M6 Big Data (batch) + M7 Integración (incremental/streaming)
# MAGIC
# MAGIC Principio de la capa bronze: **fidelidad al origen** — sin limpiar, sin filtrar,
# MAGIC pero SÍ con metadata de auditoría (archivo origen, timestamp de ingesta).
# MAGIC
# MAGIC Dos modos:
# MAGIC - **Batch full** (mínimo obligatorio): `read_files` sobre el Volume.
# MAGIC - **Incremental con Auto Loader** (obligatorio para al menos UNA entidad, se ve en M7):
# MAGIC   demuestra ingesta idempotente — si suben un archivo nuevo al Volume, solo procesa lo nuevo.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Este pipeline es el SBB que implementa los bloques de ingesta y almacenamiento definidos en el documento.

# COMMAND ----------

RAW = "/Volumes/workspace/yelp_bronze/raw"
ENTIDADES = ["business", "user", "tip", "checkin"]  # review va con Auto Loader abajo

# COMMAND ----------

# DBTITLE 1: Modo batch — 4 entidades
from pyspark.sql import functions as F

for e in ENTIDADES:
    (spark.read.json(f"{RAW}/{e}.json.gz")
        .withColumn("_archivo_origen", F.input_file_name())
        .withColumn("_fecha_ingesta", F.current_timestamp())
        .write.mode("overwrite")
        .saveAsTable(f"workspace.yelp_bronze.brz_{e}"))
    print(f"✔ brz_{e}: {spark.table(f'workspace.yelp_bronze.brz_{e}').count():,} filas")

# COMMAND ----------

# DBTITLE 1: Modo incremental — Auto Loader para review (la entidad de mayor volumen)
# ✏️ TODO (M7): completar los parámetros marcados y explicar en markdown por qué
#               Auto Loader es idempotente (schema tracking + checkpoint + exactly-once).
CHECKPOINT = "/Volumes/workspace/yelp_bronze/raw/_checkpoints/review"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .load(f"{RAW}/review*.json.gz")
    .withColumn("_archivo_origen", F.col("_metadata.file_path"))
    .withColumn("_fecha_ingesta", F.current_timestamp())
 .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)          # micro-batch y termina: amigable con la cuota serverless
    .toTable("workspace.yelp_bronze.brz_review")
).awaitTermination()

print(f"✔ brz_review: {spark.table('workspace.yelp_bronze.brz_review').count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Experimento obligatorio (evidencia para el informe)
# MAGIC 1. Ejecutar este notebook completo → anotar conteos.
# MAGIC 2. Volver a ejecutar SOLO la celda de Auto Loader **sin subir archivos nuevos** → debe procesar 0 registros.
# MAGIC 3. Subir un archivo `review_2.json.gz` pequeño al Volume y re-ejecutar → solo procesa el nuevo.
# MAGIC > ✏️ TODO — Documentar el experimento con capturas: es la prueba de ingesta incremental.
# MAGIC
# MAGIC ### Evidencias de la carga y de las tablas Bronze
# MAGIC
# MAGIC La siguiente evidencia documenta la carga inicial en modo batch de las cuatro entidades
# MAGIC (`business`, `user`, `tip` y `checkin`). La imagen muestra el punto de ejecución del
# MAGIC notebook y los archivos `.json.gz` utilizados como origen.
# MAGIC
# MAGIC ![Carga batch de las cuatro entidades](../recursos/E7/01-bronze-ingesta/4_entidades.png)
# MAGIC
# MAGIC ![Archivos JSON comprimidos utilizados para la carga](../recursos/E7/01-bronze-ingesta/archivos_gz.png)
# MAGIC
# MAGIC La primera captura corresponde a la ejecución inicial del notebook de ingesta. En ella
# MAGIC se verifica la creación y carga de las tablas Bronze a partir de los archivos de origen.
# MAGIC
# MAGIC ![Primera ejecución de la carga](../recursos/E7/01-bronze-ingesta/1_carga.jpeg)
# MAGIC
# MAGIC La segunda captura recoge la ejecución posterior del experimento de ingesta incremental
# MAGIC con Auto Loader para `review`, utilizada para comprobar el comportamiento idempotente
# MAGIC del checkpoint cuando no se incorporan registros ya procesados.
# MAGIC
# MAGIC ![Ejecución posterior del experimento de carga incremental](../recursos/E7/01-bronze-ingesta/2_carga.png)
# MAGIC
# MAGIC La vista general siguiente permite comprobar que las tablas se encuentran disponibles en
# MAGIC el esquema `workspace.yelp_bronze` después de la ejecución del pipeline.
# MAGIC
# MAGIC ![Vista general de las tablas Bronze](../recursos/E7/01-bronze-ingesta/tablas.png)
# MAGIC
# MAGIC Finalmente, se incluyen las evidencias individuales de las cinco tablas Bronze creadas.
# MAGIC Estas tablas conservan los datos del origen y añaden las columnas de auditoría
# MAGIC `_archivo_origen` y `_fecha_ingesta`.
# MAGIC
# MAGIC **Tabla `brz_business`**
# MAGIC
# MAGIC ![Tabla Bronze de negocios](../recursos/E7/01-bronze-ingesta/brz_business.png)
# MAGIC
# MAGIC **Tabla `brz_checkin`**
# MAGIC
# MAGIC ![Tabla Bronze de check-ins](../recursos/E7/01-bronze-ingesta/brz_checkin.png)
# MAGIC
# MAGIC **Tabla `brz_review`**
# MAGIC
# MAGIC ![Tabla Bronze de reseñas](../recursos/E7/01-bronze-ingesta/brz_review.png)
# MAGIC
# MAGIC **Tabla `brz_tip`**
# MAGIC
# MAGIC ![Tabla Bronze de tips](../recursos/E7/01-bronze-ingesta/brz_tip.png)
# MAGIC
# MAGIC **Tabla `brz_user`**
# MAGIC
# MAGIC ![Tabla Bronze de usuarios](../recursos/E7/01-bronze-ingesta/brz_user.png)
# MAGIC
# MAGIC ### Definition of Done (E7.1)

# MAGIC - [ ] 5 tablas bronze pobladas con columnas de auditoría.
# MAGIC - [ ] Experimento de idempotencia documentado.
# MAGIC - [ ] Decisión batch vs streaming justificada por entidad (tabla en markdown).
