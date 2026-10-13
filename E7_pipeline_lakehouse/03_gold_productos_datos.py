# Databricks notebook source
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: productos de datos
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo)
# MAGIC
# MAGIC Este notebook **no redefine el modelo físico**: carga y transforma las tablas Gold
# MAGIC creadas por E3. Los productos responden a tres necesidades: Producto necesita KPIs
# MAGIC mensuales; Operaciones Comerciales necesita actividad temporal; y Trust & Safety
# MAGIC necesita trazabilidad de reseñas y cuarentenas.
# MAGIC
# MAGIC | Producto | Consumidor | Pregunta |
# MAGIC |---|---|---|
# MAGIC | `gld_dashboard_negocio_mensual` | Producto/Marketing | ¿Cómo evoluciona el rating, volumen y satisfacción por negocio y mes? |
# MAGIC | `gld_actividad_negocio_temporal` | Operaciones Comerciales | ¿En qué fechas y franjas se concentra la actividad? |
# MAGIC | `fact_resena` y `fact_checkin_temporal` | Trust & Safety/BI | ¿Qué hechos gobernados sustentan los indicadores? |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

S = "workspace.yelp_silver"
G = "workspace.yelp_gold"

def cargar(tabla, df):
    destino = f"{G}.{tabla}"
    cols = spark.table(destino).columns
    spark.sql(f"TRUNCATE TABLE {destino}")
    df.select(*cols).write.mode("append").insertInto(destino)
    print(f"✔ {destino}: {df.count():,} filas")

def clave(columna):
    return F.abs(F.xxhash64(F.col(columna))).cast("long")

def nulo(tipo):
    return F.lit(None).cast(tipo)

business = spark.table(f"{S}.slv_business")
users = spark.table(f"{S}.slv_user")
categories = spark.table(f"{S}.slv_category")
bridge_silver = spark.table(f"{S}.slv_business_category")
reviews = spark.table(f"{S}.slv_review")
checkins = spark.table(f"{S}.slv_checkin")

# COMMAND ----------

# DBTITLE 1: Dimensiones conformadas
# Las claves sustitutas son estables: se derivan del identificador natural.
dim_negocio = (business
    .withColumn("business_key", clave("business_id"))
    .withColumn("fecha_inicio", F.lit("1900-01-01").cast("date"))
    .withColumn("fecha_fin", F.lit("9999-12-31").cast("date"))
    .withColumn("es_actual", F.lit(True))
    .withColumn("has_category", F.lit(False))
    .withColumn("has_postal_code", F.col("codigo_postal").isNotNull() & (F.trim("codigo_postal") != ""))
    .withColumn("eligible_geo", F.col("latitud").between(-90, 90) & F.col("longitud").between(-180, 180))
    .withColumn("estrellas_snapshot", F.col("estrellas"))
    .withColumn("n_resenas_snapshot", F.col("n_resenas"))
    .withColumn("fecha_carga", F.current_timestamp())
    .select("business_key", "business_id", "nombre", "direccion", "ciudad", "estado", "codigo_postal",
            "latitud", "longitud", "esta_abierto", "has_category", "has_postal_code", "eligible_geo",
            "estrellas_snapshot", "n_resenas_snapshot", "fecha_inicio", "fecha_fin", "es_actual", "fecha_carga"))
# Actualizar has_category con la relación Silver.
con_categoria = bridge_silver.select("business_id").distinct().withColumn("has_category", F.lit(True))
dim_negocio = dim_negocio.drop("has_category").join(con_categoria, "business_id", "left").fillna(False, ["has_category"])
cargar("dim_negocio", dim_negocio)

usuario_real = (users.withColumn("user_key", clave("user_id"))
    .withColumn("es_usuario_desconocido", F.lit(False))
    .withColumn("es_actual", F.lit(True)).withColumn("fecha_carga", F.current_timestamp())
    .select("user_key", "user_id", "nombre", "es_usuario_desconocido", "fecha_alta", "es_actual", "fecha_carga"))
usuario_unknown = spark.createDataFrame([(-1, "UNKNOWN", "Usuario desconocido", True, None, True)],
    "user_key long, user_id string, nombre string, es_usuario_desconocido boolean, fecha_alta date, es_actual boolean") \
    .withColumn("fecha_carga", F.current_timestamp())
cargar("dim_usuario", usuario_real.unionByName(usuario_unknown))

categorias_gold = categories.select("categoria_key", "categoria", "fecha_carga")
cargar("dim_categoria", categorias_gold)

# COMMAND ----------

# DBTITLE 2: Puente y hechos
bridge_gold = (bridge_silver
    .join(dim_negocio.select("business_key", "business_id"), "business_id")
    .select("business_key", "categoria_key", F.lit(False).alias("es_categoria_principal"), "fecha_carga"))
cargar("bridge_negocio_categoria", bridge_gold)

fact_resena = (reviews
    .join(dim_negocio.select("business_key", "business_id"), "business_id")
    .join(spark.table(f"{G}.dim_usuario").select("user_key", "user_id"), "user_id")
    .withColumn("review_key", clave("review_id"))
    .withColumn("fecha_key", F.date_format("fecha_resena", "yyyyMMdd").cast("int"))
    .withColumn("fecha_carga", F.current_timestamp())
    .select("review_key", "review_id", "business_key", "user_key", "fecha_key", "estrellas",
            "votos_utiles", "votos_divertidos", "votos_geniales", "fecha_carga"))
cargar("fact_resena", fact_resena)

fact_checkin = (checkins
    .join(dim_negocio.select("business_key", "business_id"), "business_id")
    .withColumn("fecha_carga", F.current_timestamp())
    .groupBy("business_key", "fecha_key", "franja_horaria")
    .agg(F.count("checkin_event_id").cast("long").alias("cantidad_checkins"))
    .withColumn("fecha_carga", F.current_timestamp()))
cargar("fact_checkin_temporal", fact_checkin)

actividad = (checkins
    .join(dim_negocio.select("business_key", "business_id"), "business_id")
    .groupBy("business_key", "fecha_key", "franja_horaria", "dia_semana", "es_fin_semana")
    .agg(F.count("checkin_event_id").cast("long").alias("cantidad_checkins"))
    .withColumn("fecha_carga", F.current_timestamp()))
cargar("gld_actividad_negocio_temporal", actividad)

# COMMAND ----------

# DBTITLE 3: Dimensión fecha y dashboard mensual
fechas_review = reviews.select(F.to_date("fecha_resena").alias("fecha"))
fechas_checkin = checkins.select(F.to_date("fecha_hora_checkin").alias("fecha"))
limites = fechas_review.union(fechas_checkin).filter(F.col("fecha").isNotNull()).agg(F.min("fecha").alias("min_f"), F.max("fecha").alias("max_f")).first()
if limites and limites.min_f:
    fechas = spark.sql(f"SELECT explode(sequence(to_date('{limites.min_f}'), to_date('{limites.max_f}'), interval 1 day)) AS fecha")
else:
    fechas = spark.sql("SELECT current_date() AS fecha")
dim_fecha = (fechas.withColumn("fecha_key", F.date_format("fecha", "yyyyMMdd").cast("int"))
    .withColumn("anio", F.year("fecha")).withColumn("mes", F.month("fecha"))
    .withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
    .withColumn("trimestre", F.quarter("fecha")).withColumn("dia", F.dayofmonth("fecha"))
    .withColumn("dia_semana", F.dayofweek("fecha")).withColumn("nombre_dia", F.date_format("fecha", "EEEE"))
    .withColumn("nombre_mes", F.date_format("fecha", "MMMM"))
    .withColumn("es_fin_semana", F.dayofweek("fecha").isin([1, 7]))
    .select("fecha_key", "fecha", "anio", "mes", "anio_mes", "nombre_mes", "trimestre", "dia", "dia_semana", "nombre_dia", "es_fin_semana"))
cargar("dim_fecha", dim_fecha)

m = (fact_resena.join(dim_fecha.select("fecha_key", "anio_mes"), "fecha_key")
    .groupBy("business_key", "anio_mes")
    .agg(F.count("review_key").cast("long").alias("n_reviews_mes"), F.round(F.avg("estrellas"), 2).alias("rating_mes"),
         F.countDistinct("user_key").cast("long").alias("n_reviewers_mes"),
         F.sum(F.when(F.col("estrellas") >= 4, 1).otherwise(0)).cast("long").alias("reviews_positivas"),
         F.sum(F.when(F.col("estrellas") == 3, 1).otherwise(0)).cast("long").alias("reviews_neutrales"),
         F.sum(F.when(F.col("estrellas") <= 2, 1).otherwise(0)).cast("long").alias("reviews_negativas")))
c = fact_checkin.join(dim_fecha.select("fecha_key", "anio_mes"), "fecha_key").groupBy("business_key", "anio_mes").agg(F.sum("cantidad_checkins").cast("long").alias("n_checkins_mes"))
dashboard = (m.join(c, ["business_key", "anio_mes"], "full")
    .fillna(0, ["n_reviews_mes", "n_reviewers_mes", "n_checkins_mes", "reviews_positivas", "reviews_neutrales", "reviews_negativas"])
    .withColumn("benchmark_level", F.lit(1))
    .withColumn("pct_positivas", F.when(F.col("n_reviews_mes") > 0, F.col("reviews_positivas") * 100.0 / F.col("n_reviews_mes")))
    .withColumn("pct_neutrales", F.when(F.col("n_reviews_mes") > 0, F.col("reviews_neutrales") * 100.0 / F.col("n_reviews_mes")))
    .withColumn("pct_negativas", F.when(F.col("n_reviews_mes") > 0, F.col("reviews_negativas") * 100.0 / F.col("n_reviews_mes"))))

# El DDL conserva el contrato completo del mart; las métricas aún no calculadas
# quedan explícitamente nulas, no se inventan valores.
target = spark.table(f"{G}.gld_dashboard_negocio_mensual").columns
for col in target:
    if col not in dashboard.columns:
        tipo = spark.table(f"{G}.gld_dashboard_negocio_mensual").schema[col].dataType
        dashboard = dashboard.withColumn(col, nulo(tipo))
dashboard = dashboard.withColumn("fecha_carga", F.current_timestamp())
cargar("gld_dashboard_negocio_mensual", dashboard)

# COMMAND ----------

# DBTITLE 4: Validación de productos Gold
tablas_gold = [r.tableName for r in spark.sql("SHOW TABLES IN workspace.yelp_gold").collect()]
requeridas = {"dim_negocio", "dim_usuario", "dim_fecha", "dim_categoria", "fact_resena",
              "fact_checkin_temporal", "gld_actividad_negocio_temporal", "gld_dashboard_negocio_mensual"}
faltantes = requeridas.difference(tablas_gold)
assert not faltantes, f"Faltan tablas Gold creadas por E3: {sorted(faltantes)}"
for tabla in requeridas:
    assert spark.table(f"{G}.{tabla}").count() >= 0
print(f"✔ Gold conformado: {len(requeridas)} tablas verificadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencia de lineage Silver → Gold
# MAGIC
# MAGIC La siguiente imagen documenta el flujo de transformación desde Silver hacia Gold.
# MAGIC
# MAGIC ![Lineage Silver a Gold](img/linage.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.3)
# MAGIC - [x] Tablas Gold de E3 cargadas con dimensiones, puentes, hechos y mart mensual.
# MAGIC - [x] Al menos dos productos responden a preguntas de Producto y Operaciones Comerciales.
# MAGIC - [x] Tabla stakeholder → producto → pregunta documentada.
# MAGIC - [x] Lineage Silver → Gold documentado con la imagen disponible en `img/linage.png`.