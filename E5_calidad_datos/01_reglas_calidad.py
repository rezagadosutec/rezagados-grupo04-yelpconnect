# Databricks notebook source
# MAGIC %md
# MAGIC # E5 · Reglas de Calidad de Datos + Motor de Evaluación
# MAGIC **Se define en:** M2/M3 (reglas en lenguaje de negocio) · **Se implementa tras:** M6 (cuando ya hay datos en silver)
# MAGIC
# MAGIC Enfoque *rules-as-data*: las reglas viven en la tabla `yelp_gov.dq_reglas` (no
# MAGIC hardcodeadas), un motor genérico las ejecuta y persiste resultados en
# MAGIC `yelp_gov.dq_resultados`. Ese historial alimenta el dashboard de E8 y el job de E7.
# MAGIC
# MAGIC **Mínimo 12 reglas** cubriendo las 6 dimensiones de calidad y los dolores del caso
# MAGIC (duplicidad de reseñas, check-ins inconsistentes, integridad referencial).
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Reglas de calidad de datos (definición en lenguaje de negocio)» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Aquí se implementan y ejecutan esas reglas: el documento define QUÉ debe cumplirse y por qué; la plataforma demuestra que se cumple.

# COMMAND ----------

# DBTITLE 1: Catálogo de reglas de calidad alineado al modelo físico E3
from pyspark.sql import Row

S = "workspace.yelp_silver"
reglas = [
    Row(regla_id="DQ-001", tabla_objetivo=f"{S}.slv_review", columna="estrellas", dimension="validez", descripcion="Toda reseña tiene entre 1 y 5 estrellas", expresion_sql="estrellas BETWEEN 1 AND 5", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-002", tabla_objetivo=f"{S}.slv_review", columna="review_id", dimension="unicidad", descripcion="No existen dos reseñas con el mismo review_id", expresion_sql="review_id IS NOT NULL", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-003", tabla_objetivo=f"{S}.slv_review", columna="business_id", dimension="integridad", descripcion="Toda reseña referencia un negocio existente", expresion_sql="business_id IN (SELECT business_id FROM workspace.yelp_silver.slv_business)", criticidad="alta", umbral_pct=99.5),
    Row(regla_id="DQ-004", tabla_objetivo=f"{S}.slv_business", columna="business_id", dimension="unicidad", descripcion="No existen dos negocios con el mismo business_id", expresion_sql="business_id IS NOT NULL", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-006", tabla_objetivo=f"{S}.slv_checkin", columna="fecha_hora_checkin", dimension="consistencia", descripcion="Cada check-in tiene timestamp válido", expresion_sql="fecha_hora_checkin IS NOT NULL AND fecha_key IS NOT NULL", criticidad="alta", umbral_pct=99.5),
    Row(regla_id="DQ-007", tabla_objetivo=f"{S}.slv_checkin", columna="fecha_hora_checkin", dimension="actualidad", descripcion="Un check-in no puede estar fechado en el futuro", expresion_sql="fecha_hora_checkin <= current_timestamp()", criticidad="media", umbral_pct=99.5),
    Row(regla_id="DQ-010", tabla_objetivo=f"{S}.slv_user", columna="user_id", dimension="unicidad", descripcion="No existen dos usuarios con el mismo user_id", expresion_sql="user_id IS NOT NULL", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-011", tabla_objetivo=f"{S}.slv_review", columna="user_id", dimension="integridad", descripcion="Toda reseña referencia un usuario existente", expresion_sql="user_id IN (SELECT user_id FROM workspace.yelp_silver.slv_user)", criticidad="alta", umbral_pct=99.5),
    Row(regla_id="DQ-012", tabla_objetivo=f"{S}.slv_checkin", columna="business_id", dimension="integridad", descripcion="Todo check-in referencia un negocio existente", expresion_sql="business_id IN (SELECT business_id FROM workspace.yelp_silver.slv_business)", criticidad="alta", umbral_pct=99.5),
    Row(regla_id="DQ-013", tabla_objetivo=f"{S}.slv_review", columna="texto", dimension="completitud", descripcion="Toda reseña habilitada tiene texto no vacío", expresion_sql="texto IS NOT NULL AND trim(texto) <> ''", criticidad="media", umbral_pct=99.5),
    Row(regla_id="DQ-014", tabla_objetivo=f"{S}.slv_business_category", columna="categoria_key", dimension="validez", descripcion="Toda categoría asignada pertenece al catálogo gobernado", expresion_sql="categoria_key IN (SELECT categoria_key FROM workspace.yelp_silver.slv_category)", criticidad="media", umbral_pct=99.5),
    Row(regla_id="DQ-015", tabla_objetivo=f"{S}.slv_business", columna="nombre", dimension="completitud", descripcion="Todo negocio tiene nombre no vacío", expresion_sql="nombre IS NOT NULL AND trim(nombre) <> ''", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-016", tabla_objetivo=f"{S}.slv_business", columna="estrellas", dimension="validez", descripcion="El rating del negocio está entre 1 y 5", expresion_sql="estrellas IS NULL OR estrellas BETWEEN 1 AND 5", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-017", tabla_objetivo=f"{S}.slv_checkin", columna="hora", dimension="validez", descripcion="La hora del check-in está entre 0 y 23", expresion_sql="hora IS NULL OR hora BETWEEN 0 AND 23", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-018", tabla_objetivo=f"{S}.slv_review", columna="fecha_resena", dimension="consistencia", descripcion="Toda reseña habilitada tiene fecha válida", expresion_sql="fecha_resena IS NOT NULL", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-019", tabla_objetivo=f"{S}.slv_business_category", columna="business_id", dimension="integridad", descripcion="Toda relación categoría-negocio referencia un negocio existente", expresion_sql="business_id IN (SELECT business_id FROM workspace.yelp_silver.slv_business)", criticidad="alta", umbral_pct=100.0),
]
spark.createDataFrame(reglas).write.mode("overwrite").saveAsTable("workspace.yelp_gov.dq_reglas")
display(spark.table("workspace.yelp_gov.dq_reglas"))

# COMMAND ----------

# DBTITLE 1: Motor genérico de evaluación (no modificar — extender si hace falta)
from pyspark.sql import functions as F

def evaluar_reglas():
    resultados = []
    for r in spark.table("workspace.yelp_gov.dq_reglas").collect():
        total = spark.table(r.tabla_objetivo).count()
        ok = spark.sql(f"SELECT count(*) c FROM {r.tabla_objetivo} WHERE {r.expresion_sql}").first().c
        # La unicidad requiere tratamiento especial: comparar filas vs distintos
        if r.dimension == "unicidad" and r.columna != "*":
            ok = spark.sql(f"SELECT count(DISTINCT {r.columna}) c FROM {r.tabla_objetivo}").first().c
        pct = round(100.0 * ok / total, 2) if total else 0.0
        resultados.append((r.regla_id, r.tabla_objetivo, total, ok, pct, pct >= r.umbral_pct))
    df = spark.createDataFrame(resultados,
        "regla_id STRING, tabla_objetivo STRING, filas_totales BIGINT, filas_ok BIGINT, pct_cumplimiento DOUBLE, paso BOOLEAN"
    ).withColumn("ejecucion_ts", F.current_timestamp())
    df.select("ejecucion_ts","regla_id","tabla_objetivo","filas_totales","filas_ok","pct_cumplimiento","paso") \
      .write.mode("append").saveAsTable("workspace.yelp_gov.dq_resultados")
    return df

display(evaluar_reglas())

# COMMAND ----------

# DBTITLE 1: Scorecard de calidad (insumo directo del dashboard E8)
# MAGIC %sql
# MAGIC SELECT r.dimension,
# MAGIC        count(*)                                    AS reglas,
# MAGIC        sum(CASE WHEN res.paso THEN 1 ELSE 0 END)   AS reglas_ok,
# MAGIC        round(avg(res.pct_cumplimiento), 2)         AS cumplimiento_promedio
# MAGIC FROM workspace.yelp_gov.dq_resultados res
# MAGIC JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
# MAGIC WHERE res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
# MAGIC GROUP BY r.dimension ORDER BY cumplimiento_promedio;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E5)
# MAGIC - [x] 16 reglas en las 6 dimensiones, alineadas con las columnas físicas de E3.
# MAGIC - [ ] DQ-005 sobre Tip queda fuera de alcance porque `slv_tip` no existe en el modelo físico aprobado.
# MAGIC - [ ] Motor ejecutado con historial en `dq_resultados` (≥2 corridas en fechas distintas).
# MAGIC - [ ] Para cada regla FALLIDA: decisión documentada (cuarentena, corrección en pipeline, o aceptación del riesgo con firma del "Data Owner").
# MAGIC - [ ] Bonus: agregar constraint `CHECK` o expectativa en el pipeline E7 para la regla más crítica (calidad *preventiva* vs *detectiva*).
