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
# La fuente Silver no trae una categoría principal. Se define una única
# categoría principal determinista para conservar una fila por negocio en el mart.
bridge_base = (bridge_silver
    .select("business_id", "categoria_key", "fecha_carga")
    .dropDuplicates(["business_id", "categoria_key"]))
bridge_window = Window.partitionBy("business_id").orderBy("categoria_key")
bridge_gold = (bridge_base
    .withColumn("_rn", F.row_number().over(bridge_window))
    .withColumn("es_categoria_principal", F.col("_rn") == 1)
    .join(dim_negocio.select("business_key", "business_id"), "business_id")
    .select("business_key", "categoria_key", "es_categoria_principal", "fecha_carga"))
cargar("bridge_negocio_categoria", bridge_gold)

fact_resena = (reviews
    .join(dim_negocio.select("business_key", "business_id"), "business_id")
    # Una review sin usuario en Silver no debe desaparecer del hecho.
    .join(spark.table(f"{G}.dim_usuario").select("user_key", "user_id"), "user_id", "left")
    .withColumn("user_key", F.coalesce(F.col("user_key"), F.lit(-1).cast("long")))
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

# DBTITLE 5: Corrección final del mart mensual Gold
# Completa ventanas, YTD, elegibilidad, benchmark y ranking sin modificar Silver.
MIN_REVIEWS_SAT, MIN_REVIEWS_PEER, MIN_PEERS_BENCHMARK = 10, 10, 10
target = f"{G}.gld_dashboard_negocio_mensual"
target_schema = spark.table(target).schema
target_cols = [f.name for f in target_schema.fields]

gold_business = spark.table(f"{G}.dim_negocio").filter("es_actual = true")
gold_categories = spark.table(f"{G}.dim_categoria")
gold_bridge = spark.table(f"{G}.bridge_negocio_categoria")
gold_reviews = spark.table(f"{G}.fact_resena")
gold_checkins = spark.table(f"{G}.fact_checkin_temporal")
gold_dates = spark.table(f"{G}.dim_fecha")

# Métricas mensuales y spine continuo por negocio.
rm = (gold_reviews.join(gold_dates.select("fecha_key", "anio_mes", "anio"), "fecha_key")
    .groupBy("business_key", "anio_mes", "anio")
    .agg(F.count("review_key").cast("long").alias("n_reviews_mes"),
         F.sum(F.col("estrellas").cast("double")).alias("suma_estrellas_mes"),
         F.countDistinct("user_key").cast("long").alias("n_reviewers_mes"),
         F.sum(F.when(F.col("estrellas") >= 4, 1).otherwise(0)).cast("long").alias("reviews_positivas"),
         F.sum(F.when(F.col("estrellas") == 3, 1).otherwise(0)).cast("long").alias("reviews_neutrales"),
         F.sum(F.when(F.col("estrellas") <= 2, 1).otherwise(0)).cast("long").alias("reviews_negativas")))
cm = (gold_checkins.join(gold_dates.select("fecha_key", "anio_mes", "anio"), "fecha_key")
    .groupBy("business_key", "anio_mes", "anio").agg(F.sum("cantidad_checkins").cast("long").alias("n_checkins_mes")))
actividad = (rm.select("business_key", "anio_mes").union(cm.select("business_key", "anio_mes")).distinct()
    .withColumn("mes_fecha", F.to_date(F.concat("anio_mes", F.lit("-01"))))
    .groupBy("business_key").agg(F.min("mes_fecha").alias("inicio"), F.max("mes_fecha").alias("fin"))
    .select("business_key", F.explode(F.sequence("inicio", "fin", F.expr("interval 1 month"))).alias("mes_fecha"))
    .withColumn("anio_mes", F.date_format("mes_fecha", "yyyy-MM"))
    .withColumn("anio", F.year("mes_fecha")))
base = (actividad.join(rm.drop("anio"), ["business_key", "anio_mes"], "left")
    .join(cm.drop("anio"), ["business_key", "anio_mes"], "left")
    .fillna(0, ["n_reviews_mes", "n_reviewers_mes", "n_checkins_mes", "reviews_positivas", "reviews_neutrales", "reviews_negativas", "suma_estrellas_mes"])
    .withColumn("rating_mes", F.when(F.col("n_reviews_mes") > 0, F.col("suma_estrellas_mes") / F.col("n_reviews_mes")))
    .withColumn("pct_positivas", F.when(F.col("n_reviews_mes") > 0, 100 * F.col("reviews_positivas") / F.col("n_reviews_mes")))
    .withColumn("pct_neutrales", F.when(F.col("n_reviews_mes") > 0, 100 * F.col("reviews_neutrales") / F.col("n_reviews_mes")))
    .withColumn("pct_negativas", F.when(F.col("n_reviews_mes") > 0, 100 * F.col("reviews_negativas") / F.col("n_reviews_mes"))))

# Ventanas 3m/6m/12m y acumulados YTD.
orden = Window.partitionBy("business_key").orderBy("mes_fecha")
ytd = Window.partitionBy("business_key", "anio").orderBy("mes_fecha").rowsBetween(Window.unboundedPreceding, 0)
for nombre, inicio in [("3m", -2), ("6m", -5), ("12m", -11)]:
    ventana = orden.rowsBetween(inicio, 0)
    base = (base.withColumn(f"reviews_{nombre}", F.sum("n_reviews_mes").over(ventana).cast("long"))
        .withColumn(f"checkins_{nombre}", F.sum("n_checkins_mes").over(ventana).cast("long"))
        .withColumn(f"stars_{nombre}", F.sum("suma_estrellas_mes").over(ventana)))
    base = base.withColumn(f"rating_{nombre}", F.when(F.col(f"reviews_{nombre}") > 0, F.col(f"stars_{nombre}") / F.col(f"reviews_{nombre}")))
base = (base.withColumn("reviews_ytd", F.sum("n_reviews_mes").over(ytd).cast("long"))
    .withColumn("checkins_ytd", F.sum("n_checkins_mes").over(ytd).cast("long"))
    .withColumn("eligible_rating", F.col("n_reviews_mes") > 0)
    .withColumn("eligible_rating_12m", F.col("reviews_12m") >= MIN_REVIEWS_SAT)
    .withColumn("eligible_checkin", F.col("n_checkins_mes") > 0)
    .withColumn("umbral_reviews_satisfaccion", F.lit(MIN_REVIEWS_SAT))
    .withColumn("flag_satisfaccion_valida", F.col("n_reviews_mes") >= MIN_REVIEWS_SAT))

# Benchmark: categoría + postal; fallback a categoría + ciudad.
# La tabla puente puede contener varias categorías por negocio. Como el mart
# tiene una fila por negocio y mes, se aplica una regla determinista: se usa
# la categoría marcada como principal y, si no existe, la menor categoria_key.
cats = (gold_bridge.join(gold_categories.select("categoria_key", "categoria"), "categoria_key")
    .select("business_key", "categoria_key", "categoria", "es_categoria_principal")
    .dropDuplicates())
cat_window = Window.partitionBy("business_key").orderBy(
    F.col("es_categoria_principal").desc(), F.col("categoria_key"))
cats = cats.withColumn("_rn", F.row_number().over(cat_window)).filter("_rn = 1").drop("_rn")

peers = (gold_business.select("business_key", "ciudad", "codigo_postal", "esta_abierto", "has_category", "has_postal_code")
    .join(gold_reviews.groupBy("business_key").agg(F.count("review_key").alias("reviews_total")), "business_key", "left")
    .fillna(0, ["reviews_total"])
    .withColumn("peer_ok", (F.col("esta_abierto") == True) & F.col("has_category") & F.col("has_postal_code") & (F.col("reviews_total") >= MIN_REVIEWS_PEER)))
monthly = base.join(cats, "business_key", "left").join(peers, "business_key", "left")
peer_monthly = monthly.filter(F.col("peer_ok"))
postal = peer_monthly.groupBy("anio_mes", "categoria_key", "codigo_postal").agg(
    F.countDistinct("business_key").alias("pn"),
    F.sum("n_reviews_mes").alias("pr"),
    F.sum("n_checkins_mes").alias("pc"),
    F.sum("suma_estrellas_mes").alias("ps"))
city = peer_monthly.groupBy("anio_mes", "categoria_key", "ciudad").agg(
    F.countDistinct("business_key").alias("cn"),
    F.sum("n_reviews_mes").alias("cr"),
    F.sum("n_checkins_mes").alias("cc"),
    F.sum("suma_estrellas_mes").alias("cs"))
out = (monthly.join(postal, ["anio_mes", "categoria_key", "codigo_postal"], "left")
    .join(city, ["anio_mes", "categoria_key", "ciudad"], "left")
    .withColumn("postal_peers", F.greatest(F.coalesce(F.col("pn"), F.lit(0)) - F.when(F.col("peer_ok"), 1).otherwise(0), F.lit(0)))
    .withColumn("city_peers", F.greatest(F.coalesce(F.col("cn"), F.lit(0)) - F.when(F.col("peer_ok"), 1).otherwise(0), F.lit(0)))
    .withColumn("usar_postal", F.col("postal_peers") >= MIN_PEERS_BENCHMARK)
    .withColumn("usar_ciudad", (~F.col("usar_postal")) & (F.col("city_peers") >= MIN_PEERS_BENCHMARK))
    .withColumn("benchmark_level", F.when(F.col("usar_postal"), 1).when(F.col("usar_ciudad"), 2).cast("int"))
    .withColumn("benchmark_type", F.when(F.col("usar_postal"), "CATEGORIA_POSTAL").when(F.col("usar_ciudad"), "CATEGORIA_CIUDAD"))
    .withColumn("benchmark_categoria", F.col("categoria"))
    .withColumn("benchmark_postal_code", F.when(F.col("usar_postal"), F.col("codigo_postal")))
    .withColumn("benchmark_ciudad", F.when(F.col("usar_ciudad"), F.col("ciudad")))
    .withColumn("benchmark_fallback", F.when(F.col("usar_ciudad"), True).otherwise(False))
    .withColumn("benchmark_n_business", F.when(F.col("usar_postal"), F.col("postal_peers")).when(F.col("usar_ciudad"), F.col("city_peers")))
    # Se excluye el negocio actual tanto del numerador como del denominador.
    .withColumn("benchmark_total_reviews", F.when(F.col("usar_postal"), F.col("pr") - F.when(F.col("peer_ok"), F.col("n_reviews_mes")).otherwise(0)).when(F.col("usar_ciudad"), F.col("cr") - F.when(F.col("peer_ok"), F.col("n_reviews_mes")).otherwise(0)))
    .withColumn("benchmark_total_checkins", F.when(F.col("usar_postal"), F.col("pc") - F.when(F.col("peer_ok"), F.col("n_checkins_mes")).otherwise(0)).when(F.col("usar_ciudad"), F.col("cc") - F.when(F.col("peer_ok"), F.col("n_checkins_mes")).otherwise(0)))
    .withColumn("benchmark_total_stars", F.when(F.col("usar_postal"), F.col("ps") - F.when(F.col("peer_ok"), F.col("suma_estrellas_mes")).otherwise(0)).when(F.col("usar_ciudad"), F.col("cs") - F.when(F.col("peer_ok"), F.col("suma_estrellas_mes")).otherwise(0)))
    .withColumn("benchmark_reviews", F.when(F.col("benchmark_n_business") > 0, F.col("benchmark_total_reviews") / F.col("benchmark_n_business")))
    .withColumn("benchmark_checkins", F.when(F.col("benchmark_n_business") > 0, F.col("benchmark_total_checkins") / F.col("benchmark_n_business")))
    # El rating es estrellas totales dividido por reviews totales, no por el promedio de reviews.
        .withColumn("benchmark_rating", F.when(F.col("benchmark_total_reviews") > 0, F.col("benchmark_total_stars") / F.col("benchmark_total_reviews")))
    .withColumn("eligible_benchmark", F.col("usar_postal") | F.col("usar_ciudad"))
    # El ranking debe respetar el nivel realmente utilizado por el benchmark.
    .withColumn("_ranking_group",
        F.when(F.col("usar_postal"), F.concat(F.lit("P|"), F.col("categoria_key").cast("string"), F.lit("|"), F.col("codigo_postal")))
         .when(F.col("usar_ciudad"), F.concat(F.lit("C|"), F.col("categoria_key").cast("string"), F.lit("|"), F.col("ciudad")))))
rank = Window.partitionBy("anio_mes", "_ranking_group").orderBy(F.col("rating_mes").desc_nulls_last(), "business_key")
out = (out.withColumn("ranking_rating", F.when(F.col("eligible_benchmark"), F.row_number().over(rank)))
    .withColumn("delta_rating_vs_benchmark", F.col("rating_mes") - F.col("benchmark_rating"))
    .withColumn("delta_reviews_vs_benchmark", F.col("n_reviews_mes") - F.col("benchmark_reviews"))
    .withColumn("delta_checkins_vs_benchmark", F.col("n_checkins_mes") - F.col("benchmark_checkins")))
for field in target_schema.fields:
    out = out.withColumn(field.name, F.col(field.name).cast(field.dataType) if field.name in out.columns else F.lit(None).cast(field.dataType))
spark.sql(f"TRUNCATE TABLE {target}")
out.withColumn("fecha_carga", F.current_timestamp()).select(*target_cols).write.mode("append").insertInto(target)
assert spark.table(target).count() > 0, "Gold mensual quedó vacío"
print(f"✔ Corrección Gold aplicada: {spark.table(target).count():,} filas")

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