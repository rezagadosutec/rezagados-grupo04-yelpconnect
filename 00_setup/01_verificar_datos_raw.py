# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Verificación de datos crudos
# MAGIC Valida que los 5 archivos del Yelp Open Dataset (muestreados) estén en el Volume
# MAGIC y hace una primera exploración de esquemas. **No transforma nada** — eso es trabajo
# MAGIC del pipeline (E7).

# COMMAND ----------

RAW = "/Volumes/workspace/yelp_bronze/raw"
ESPERADOS = ["business", "review", "user", "tip", "checkin"]

archivos = [f.name for f in dbutils.fs.ls(RAW)]
print("Archivos en el Volume:", archivos)

faltantes = [e for e in ESPERADOS if not any(e in a for a in archivos)]
assert not faltantes, f"❌ Faltan archivos: {faltantes}. Suban la muestra al Volume."
print("✔ Los 5 datasets están presentes")

# COMMAND ----------

# DBTITLE 1: Exploración rápida de esquemas y volúmenes
for e in ESPERADOS:
    df = spark.read.json(f"{RAW}/{e}.json.gz")
    print(f"\n=== {e} · {df.count():,} filas ===")
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC > ✏️ **TODO (grupo):** anotar aquí en markdown 3 observaciones sobre los datos crudos
# MAGIC > (campos anidados, tipos sospechosos, posibles problemas de calidad). Estas
# MAGIC > observaciones alimentan E3 (modelado) y E5 (reglas de calidad).
# MAGIC
# MAGIC > ✏️ **Observaciones sobre los datos crudos:**
# MAGIC > 1. `business` contiene estructuras anidadas (`attributes` y `hours`). Para el modelado
# MAGIC >    será necesario decidir si se normalizan en tablas/columnas separadas o si se
# MAGIC >    conservan como estructuras; además, `categories` llega como una cadena con varias
# MAGIC >    categorías, por lo que puede requerir una tabla puente.
# MAGIC > 2. Hay varios campos potencialmente sospechosos por su representación: los atributos
# MAGIC >    de `business` son `string` aunque contienen valores booleanos, listas o rangos, y
# MAGIC >    `checkin.date` es una cadena con fechas concatenadas. También conviene convertir
# MAGIC >    `date`/`yelping_since` a tipos de fecha y validar rangos y formatos.
# MAGIC > 3. Deben controlarse problemas de calidad e integridad: valores nulos en campos clave,
# MAGIC >    duplicados de identificadores, referencias de `business_id`/`user_id` sin pareja,
# MAGIC >    estrellas fuera del rango esperado (1–5) y conteos negativos. Las reseñas y tips
# MAGIC >    contienen texto libre, que requiere considerar PII indirecta y reglas de limpieza.

