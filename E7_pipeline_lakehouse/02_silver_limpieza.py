# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: limpieza y conformación
# MAGIC **Cierra con:** M6 Big Data
# MAGIC
# MAGIC Silver materializa el **modelo físico de E3**: mismos nombres, mismos tipos,
# MAGIC mismas constraints. Las transformaciones deben atacar los dolores del caso:
# MAGIC deduplicación de reseñas (Marketing), normalización de check-ins (Ops Comerciales).
# MAGIC
# MAGIC Ejemplo resuelto: `slv_business`. El grupo completa las demás entidades.

# COMMAND ----------

# DBTITLE 1: Preparación y utilidades de carga
from pyspark.sql import functions as F, Window

SILVER = "workspace.yelp_silver"

# E3 ya creó las tablas con COMMENT y PK/FK. TRUNCATE + INSERT conserva ese metadato.
def cargar(tabla, df):
    destino = f"{SILVER}.{tabla}"
    spark.sql(f"TRUNCATE TABLE {destino}")
    df.select(*spark.table(destino).columns).write.mode("append").insertInto(destino)
    print(f"✔ {tabla}: {df.count():,} filas")

def normalizar_texto(columna):
    return F.trim(F.regexp_replace(F.col(columna), r"\s+", " "))

brz_business = spark.table("workspace.yelp_bronze.brz_business")
brz_user = spark.table("workspace.yelp_bronze.brz_user")
brz_review = spark.table("workspace.yelp_bronze.brz_review")
brz_checkin = spark.table("workspace.yelp_bronze.brz_checkin")

# COMMAND ----------

# DBTITLE 2: slv_business, slv_category, slv_business_category y slv_business_hours
# Criterio de negocio: se conserva la versión más reciente por business_id.
w_business = Window.partitionBy("business_id").orderBy(F.col("_fecha_ingesta").desc_nulls_last())
business_base = (brz_business
    .withColumn("_rn", F.row_number().over(w_business))
    .filter((F.col("_rn") == 1) & F.col("business_id").isNotNull())
    .select(
        "business_id", normalizar_texto("name").alias("nombre"),
        normalizar_texto("address").alias("direccion"),
        F.initcap(normalizar_texto("city")).alias("ciudad"),
        F.upper(F.trim(F.col("state"))).alias("estado"),
        F.trim(F.col("postal_code")).alias("codigo_postal"),
        F.col("latitude").cast("double").alias("latitud"),
        F.col("longitude").cast("double").alias("longitud"),
        F.col("stars").cast("double").alias("estrellas"),
        F.col("review_count").cast("int").alias("n_resenas"),
        (F.col("is_open").cast("int") == 1).alias("esta_abierto"),
        F.current_timestamp().alias("fecha_carga"),
        F.split(F.coalesce(F.col("categories"), F.lit("")), r"\s*,\s*").alias("categorias")
    ))

cargar("slv_business", business_base.drop("categorias"))

categorias = (business_base.select(F.explode("categorias").alias("categoria"))
    .withColumn("categoria", normalizar_texto("categoria"))
    .filter(F.col("categoria") != "")
    .dropDuplicates(["categoria"])
    .withColumn("categoria_key", F.abs(F.xxhash64(F.lower("categoria"))).cast("long"))
    .select("categoria_key", "categoria")
    .withColumn("fecha_carga", F.current_timestamp()))
cargar("slv_category", categorias)

bridge = (business_base.select("business_id", F.explode("categorias").alias("categoria"))
    .withColumn("categoria", normalizar_texto("categoria"))
    .filter(F.col("categoria") != "")
    .join(categorias.select("categoria", "categoria_key"), "categoria", "left")
    .filter(F.col("categoria_key").isNotNull())
    .withColumn("fecha_carga", F.current_timestamp())
    .select("business_id", "categoria_key", "categoria", "fecha_carga")
    .dropDuplicates(["business_id", "categoria_key"]))
cargar("slv_business_category", bridge)

# La fuente trae hours como struct con las claves Monday...Sunday.
dias = [(1, "Monday"), (2, "Tuesday"), (3, "Wednesday"), (4, "Thursday"),
        (5, "Friday"), (6, "Saturday"), (7, "Sunday")]
horas = (brz_business
    .filter(F.col("business_id").isNotNull())
    .withColumn("_rn", F.row_number().over(w_business))
    .filter(F.col("_rn") == 1)
    .select("business_id", *[F.col("hours").getField(d).alias(d) for _, d in dias]))
por_dia = F.array(*[F.struct(F.lit(n).alias("dia_semana"), F.col(d).alias("intervalo")) for n, d in dias])
horas = (horas.select("business_id", F.explode(por_dia).alias("h"))
    .select("business_id", "h.dia_semana", "h.intervalo")
    .filter(F.col("intervalo").isNotNull()))
horas = (horas.withColumn("hora_apertura", F.split("intervalo", "-").getItem(0).cast("string"))
    .withColumn("hora_cierre", F.split("intervalo", "-").getItem(1).cast("string"))
    .withColumn("es_24_horas", (F.col("hora_apertura") == "00:00") & (F.col("hora_cierre") == "00:00"))
    .withColumn("_ap", F.to_timestamp(F.concat(F.lit("2000-01-01 "), "hora_apertura")))
    .withColumn("_ci", F.to_timestamp(F.concat(F.lit("2000-01-01 "), "hora_cierre")))
    .withColumn("cruza_medianoche", F.col("_ci") < F.col("_ap"))
    .withColumn("fecha_carga", F.current_timestamp())
    .select("business_id", "dia_semana", "hora_apertura", "hora_cierre", "es_24_horas", "cruza_medianoche", "fecha_carga"))
cargar("slv_business_hours", horas)

# COMMAND ----------

# DBTITLE 3: slv_user
usuarios = (brz_user.filter(F.col("user_id").isNotNull())
    .withColumn("_rn", F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("_fecha_ingesta").desc_nulls_last())))
    .filter(F.col("_rn") == 1)
    .select(
        "user_id", normalizar_texto("name").alias("nombre"),
        F.to_timestamp("yelping_since").cast("date").alias("fecha_alta"),
        F.col("review_count").cast("int").alias("n_resenas"),
        F.col("useful").cast("int").alias("votos_utiles"),
        F.col("funny").cast("int").alias("votos_divertidos"),
        F.col("cool").cast("int").alias("votos_geniales"),
        F.col("fans").cast("int").alias("n_fans"),
        F.col("average_stars").cast("double").alias("estrellas_promedio"),
        F.when(F.col("elite").isNull() | (F.trim("elite") == ""), F.array().cast("array<string>")
               ).otherwise(F.split(F.regexp_replace("elite", r"\s*,\s*", ","), ",")).alias("elite"),
        F.current_timestamp().alias("fecha_carga")))
cargar("slv_user", usuarios)

# COMMAND ----------

# DBTITLE 4: slv_review y slv_review_quarantine
# DQ-001, DQ-002, DQ-003, DQ-011 y DQ-013:
# las filas inválidas, duplicadas o huérfanas se conservan en cuarentena.
negocios = spark.table(f"{SILVER}.slv_business").select("business_id").distinct()
usuarios_ok = spark.table(f"{SILVER}.slv_user").select("user_id").distinct()
reviews_raw = (brz_review
    .withColumn("review_id", F.trim("review_id"))
    .withColumn("user_id", F.trim("user_id"))
    .withColumn("business_id", F.trim("business_id"))
    .withColumn("estrellas", F.col("stars").cast("double").cast("int"))
    .withColumn("fecha_resena", F.to_timestamp("date"))
    .withColumn("texto", F.trim("text"))
    .withColumn("votos_utiles", F.col("useful").cast("int"))
    .withColumn("votos_divertidos", F.col("funny").cast("int"))
    .withColumn("votos_geniales", F.col("cool").cast("int"))
    .withColumn("_rn_id", F.row_number().over(Window.partitionBy("review_id").orderBy(F.col("_fecha_ingesta").desc_nulls_last())))
    .withColumn("_rn_logico", F.row_number().over(Window.partitionBy("user_id", "business_id", "texto").orderBy(F.col("_fecha_ingesta").desc_nulls_last()))))

reasons = (reviews_raw
    .withColumn("_motivo", F.when(F.col("review_id").isNull() | (F.col("review_id") == ""), "review_id nulo o vacío")
        .when(F.col("_rn_id") > 1, "review_id duplicado")
        .when((F.col("estrellas").isNull()) | (F.col("estrellas") < 1) | (F.col("estrellas") > 5), "estrellas fuera de 1 a 5")
        .when(F.col("fecha_resena").isNull(), "fecha_resena inválida")
        .when(F.col("texto").isNull() | (F.col("texto") == ""), "texto nulo o vacío")
        .when(F.col("_rn_logico") > 1, "duplicado lógico user_id + business_id + texto")
        .when(F.col("business_id").isNull(), "business_id nulo")
        .when(F.col("user_id").isNull(), "user_id nulo")
        .otherwise(F.lit(None).cast("string"))))
reasons = (reasons.join(negocios.withColumn("_business_ok", F.lit(1)), "business_id", "left")
    .join(usuarios_ok.withColumn("_user_ok", F.lit(1)), "user_id", "left")
    .withColumn("_motivo", F.when(F.col("_motivo").isNotNull(), F.col("_motivo"))
        .when(F.col("_business_ok").isNull(), "business_id no existe en slv_business")
        .when(F.col("_user_ok").isNull(), "user_id no existe en slv_user")
        .otherwise(F.lit(None).cast("string"))))

cuarentena = (reasons.filter(F.col("_motivo").isNotNull())
    .select("review_id", "user_id", "business_id", "estrellas", "fecha_resena",
            F.col("_motivo").alias("motivo_cuarentena"))
    .withColumn("fecha_cuarentena", F.current_timestamp())
    .withColumn("fecha_carga", F.current_timestamp()))
cargar("slv_review_quarantine", cuarentena)

validas = (reasons.filter(F.col("_motivo").isNull())
    .select("review_id", "user_id", "business_id", "estrellas", "fecha_resena", "texto",
            "votos_utiles", "votos_divertidos", "votos_geniales")
    .withColumn("fecha_carga", F.current_timestamp()))
cargar("slv_review", validas)

# COMMAND ----------

# DBTITLE 5: slv_checkin
# DQ-006, DQ-007 y DQ-012: se explota date a un evento por fila,
# se normaliza el timestamp y se excluyen referencias huérfanas.
checkins = (brz_checkin
    .select("business_id", F.explode(F.split(F.coalesce(F.col("date"), F.lit("")), r"\s*,\s*")).alias("fecha_texto"))
    .withColumn("business_id", F.trim("business_id"))
    .withColumn("fecha_hora_checkin", F.to_timestamp(F.trim("fecha_texto")))
    .filter(F.col("fecha_hora_checkin").isNotNull())
    .join(negocios, "business_id", "left_semi")
    .withColumn("checkin_event_id", F.sha2(F.concat_ws("|", "business_id", F.date_format("fecha_hora_checkin", "yyyy-MM-dd HH:mm:ss")), 256))
    .withColumn("fecha_key", F.date_format("fecha_hora_checkin", "yyyyMMdd").cast("int"))
    .withColumn("hora", F.hour("fecha_hora_checkin"))
    .withColumn("franja_horaria", F.when(F.col("hora") < 6, "MADRUGADA")
        .when(F.col("hora") < 12, "MANANA").when(F.col("hora") < 18, "TARDE").otherwise("NOCHE"))
    .withColumn("dia_semana", F.date_format("fecha_hora_checkin", "EEEE"))
    .withColumn("es_fin_semana", F.dayofweek("fecha_hora_checkin").isin([1, 7]))
    .withColumn("fecha_carga", F.current_timestamp())
    .select("checkin_event_id", "business_id", "fecha_hora_checkin", "fecha_key", "hora",
            "franja_horaria", "dia_semana", "es_fin_semana", "fecha_carga")
    .dropDuplicates(["checkin_event_id"]))
cargar("slv_checkin", checkins)


# DBTITLE 6: Gate de calidad — el pipeline falla si la calidad crítica no pasa
# Ejecuta E5 y corta el pipeline si una regla de criticidad alta falla.
resultado = dbutils.notebook.run("../E5_calidad_datos/01_reglas_calidad", 600)

fallas_criticas = spark.sql("""
  SELECT count(*) c
  FROM workspace.yelp_gov.dq_resultados res
  JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
  WHERE r.criticidad = 'alta' AND NOT res.paso
    AND res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
""").first().c
assert fallas_criticas == 0, f"❌ {fallas_criticas} reglas críticas fallaron — pipeline detenido (así se ve la calidad como GATE, no como reporte)"
print("✔ Gate de calidad superado")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.2)
# MAGIC - [ ] Todas las entidades silver pobladas conforme al DDL de E3.
# MAGIC - [ ] Deduplicación de reseñas con criterio documentado + tabla de cuarentena operativa.
# MAGIC - [ ] Check-ins explotados a grano evento.
# MAGIC - [ ] Gate de calidad integrado y funcionando.
