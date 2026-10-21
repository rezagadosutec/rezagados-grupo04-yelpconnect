# Databricks notebook source
# MAGIC %md
# MAGIC # E8 · Dashboard de BI (AI/BI Dashboard + Genie)
# MAGIC **Cierra con:** M9 BI y Visualización
# MAGIC
# MAGIC El entregable son DOS artefactos en el workspace:
# MAGIC 1. **AI/BI Dashboard** "Plataforma de Datos YelpConnect — Grupo 04" con 2 páginas:
# MAGIC    - *Página Negocio:* KPIs para los stakeholders del caso (rating, reseñas, check-ins).
# MAGIC    - *Página Gobierno:* el dashboard que le mostrarían a la CDO — salud de calidad (E5),
# MAGIC      completitud de metadata (E4), cobertura de ownership (E6).
# MAGIC 2. **Genie Space** sobre la capa gold, con instrucciones y sinónimos del glosario E4,
# MAGIC    para que un usuario de negocio pregunte en lenguaje natural.
# MAGIC
# MAGIC Reglas: el dashboard consume SOLO `yelp_gold` y `yelp_gov` (nunca silver/bronze —
# MAGIC eso valida la arquitectura de capas), y cada visual declara a qué stakeholder responde.
# MAGIC
# MAGIC Este notebook deja listas las consultas base; el ensamblado se hace en la UI.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Requerimientos de información y KPIs por stakeholder» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Cada visual de este dashboard debe responder a un requerimiento declarado en el documento; un KPI sin stakeholder que lo pidió no va al tablero.

# COMMAND ----------

# DBTITLE 1: Q1 — Evolución de reseñas y rating (Producto)
# MAGIC %sql
# MAGIC SELECT anio_mes AS mes,
# MAGIC        sum(n_reviews_mes) AS resenas,
# MAGIC        round(sum(rating_mes * n_reviews_mes) / nullif(sum(n_reviews_mes), 0), 2) AS rating
# MAGIC FROM workspace.yelp_gold.gld_dashboard_negocio_mensual
# MAGIC GROUP BY anio_mes ORDER BY anio_mes;

# COMMAND ----------

# DBTITLE 1: Q2 — Salud de calidad de datos (CDO)
# MAGIC %sql
# MAGIC SELECT r.dimension, round(avg(res.pct_cumplimiento),2) AS cumplimiento,
# MAGIC        sum(CASE WHEN NOT res.paso THEN 1 ELSE 0 END)  AS reglas_en_rojo
# MAGIC FROM workspace.yelp_gov.dq_resultados res
# MAGIC JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
# MAGIC WHERE res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
# MAGIC GROUP BY r.dimension;

# COMMAND ----------

# DBTITLE 1: Q3 — KPIs de gobierno (CDO)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT round(100.0 * sum(CASE WHEN descripcion_negocio IS NOT NULL THEN 1 ELSE 0 END) / count(*), 1)
# MAGIC      FROM workspace.yelp_gov.diccionario_datos) AS pct_columnas_documentadas,
# MAGIC   (SELECT count(DISTINCT responsable) FROM workspace.yelp_gov.diccionario_datos
# MAGIC      WHERE responsable IS NOT NULL) AS responsables_documentados,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.glosario_negocio) AS terminos_glosario,
# MAGIC   (SELECT count(DISTINCT regla_id) FROM workspace.yelp_gov.dq_reglas) AS reglas_dq;

# COMMAND ----------

# DBTITLE 1: Q4 — Top de negocios por rating y volumen (Producto/Marketing)
# MAGIC %sql
# MAGIC SELECT b.nombre, b.ciudad, d.anio_mes, d.rating_mes, d.n_reviews_mes,
# MAGIC        d.benchmark_rating, d.delta_rating_vs_benchmark, d.ranking_rating
# MAGIC FROM workspace.yelp_gold.gld_dashboard_negocio_mensual d
# MAGIC JOIN workspace.yelp_gold.dim_negocio b USING (business_key)
# MAGIC WHERE d.n_reviews_mes >= 10
# MAGIC ORDER BY d.rating_mes DESC, d.n_reviews_mes DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1: Q5 — Check-ins por día y franja (Operaciones Comerciales)
# MAGIC %sql
# MAGIC SELECT f.fecha, a.franja_horaria, sum(a.cantidad_checkins) AS checkins
# MAGIC FROM workspace.yelp_gold.gld_actividad_negocio_temporal a
# MAGIC JOIN workspace.yelp_gold.dim_fecha f USING (fecha_key)
# MAGIC GROUP BY f.fecha, a.franja_horaria
# MAGIC ORDER BY f.fecha, a.franja_horaria;

# COMMAND ----------

# DBTITLE 1: Q6 — Distribución de satisfacción (Marketing)
# MAGIC %sql
# MAGIC SELECT anio_mes AS mes,
# MAGIC        sum(reviews_positivas) AS positivas,
# MAGIC        sum(reviews_neutrales) AS neutrales,
# MAGIC        sum(reviews_negativas) AS negativas,
# MAGIC        round(100.0 * sum(reviews_positivas) / nullif(sum(n_reviews_mes), 0), 2) AS pct_positivas,
# MAGIC        round(100.0 * sum(reviews_negativas) / nullif(sum(n_reviews_mes), 0), 2) AS pct_negativas
# MAGIC FROM workspace.yelp_gold.gld_dashboard_negocio_mensual
# MAGIC GROUP BY anio_mes ORDER BY anio_mes;

# COMMAND ----------

# DBTITLE 1: Q7 — Reglas críticas y reseñas duplicadas (Trust & Safety)
# MAGIC %sql
# MAGIC SELECT res.ejecucion_ts, r.regla_id, r.descripcion,
# MAGIC        res.pct_cumplimiento, res.paso
# MAGIC FROM workspace.yelp_gov.dq_resultados res
# MAGIC JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
# MAGIC WHERE r.regla_id IN ('DQ-001', 'DQ-002', 'DQ-003', 'DQ-011', 'DQ-013')
# MAGIC ORDER BY res.ejecucion_ts DESC, r.regla_id;

# COMMAND ----------

# DBTITLE 1: Q8 — Tendencia de ejecuciones del pipeline (Data Governance)
# MAGIC %sql
# MAGIC SELECT date_trunc('day', ejecucion_ts) AS dia,
# MAGIC        capa, count(*) AS registros_bitacora,
# MAGIC        sum(filas) AS filas_procesadas,
# MAGIC        round(avg(duracion_seg), 2) AS duracion_promedio_seg
# MAGIC FROM workspace.yelp_gov.pipeline_bitacora
# MAGIC GROUP BY date_trunc('day', ejecucion_ts), capa
# MAGIC ORDER BY dia, capa;


# COMMAND ----------

# DBTITLE 1: Q9 — Estado actual del benchmark por negocio (Producto)
# MAGIC %sql
# MAGIC WITH rep AS (
# MAGIC   SELECT b.*, n.nombre,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY b.business_key, b.anio_mes
# MAGIC            ORDER BY CASE WHEN b.eligible_benchmark THEN 0 ELSE 1 END,
# MAGIC                     CASE WHEN b.eligible_rating THEN 0 ELSE 1 END,
# MAGIC                     CASE WHEN b.flag_satisfaccion_valida THEN 0 ELSE 1 END,
# MAGIC                     CASE WHEN b.eligible_rating_12m THEN 0 ELSE 1 END,
# MAGIC                     b.benchmark_level,
# MAGIC                     CASE WHEN b.benchmark_fallback THEN 1 ELSE 0 END,
# MAGIC                     b.benchmark_categoria) AS rn
# MAGIC   FROM workspace.yelp_gold.gld_dashboard_negocio_mensual b
# MAGIC   JOIN workspace.yelp_gold.dim_negocio n
# MAGIC     ON b.business_key = n.business_key AND n.es_actual = true
# MAGIC ),
# MAGIC ultimo AS (
# MAGIC   SELECT business_key, MAX(anio_mes) AS anio_mes
# MAGIC   FROM rep WHERE rn = 1 AND eligible_benchmark = true
# MAGIC   GROUP BY business_key
# MAGIC ),
# MAGIC fallback AS (
# MAGIC   SELECT business_key, MAX(anio_mes) AS anio_mes
# MAGIC   FROM rep WHERE rn = 1 GROUP BY business_key
# MAGIC )
# MAGIC SELECT r.business_key, r.nombre,
# MAGIC        CONCAT(r.nombre, ' (', CAST(r.business_key AS STRING), ')') AS negocio_etiqueta,
# MAGIC        r.anio_mes, r.benchmark_type, r.benchmark_categoria, r.benchmark_ciudad,
# MAGIC        r.benchmark_postal_code, r.benchmark_n_business, r.benchmark_level,
# MAGIC        CASE WHEN r.benchmark_level = 1 THEN '1 · Categoría + código postal'
# MAGIC             WHEN r.benchmark_level = 2 THEN '2 · Categoría + ciudad'
# MAGIC             ELSE CONCAT('Nivel ', CAST(r.benchmark_level AS STRING)) END AS benchmark_nivel,
# MAGIC        CASE WHEN r.benchmark_fallback THEN 'Grupo ampliado por falta de evidencia'
# MAGIC             ELSE 'Misma categoría y código postal' END AS detalle_grupo,
# MAGIC        CASE WHEN r.eligible_benchmark THEN 'Elegible (benchmark confiable)'
# MAGIC             ELSE 'No confiable por evidencia insuficiente' END AS estado_benchmark,
# MAGIC        CASE WHEN r.flag_satisfaccion_valida THEN 'Válida (mínimo cumplido)'
# MAGIC             ELSE 'No concluyente' END AS estado_satisfaccion,
# MAGIC        CASE WHEN r.eligible_rating_12m THEN 'Historial suficiente'
# MAGIC             ELSE 'Historia insuficiente para 12 meses' END AS estado_tendencia_12m
# MAGIC FROM rep r
# MAGIC JOIN (SELECT f.business_key, COALESCE(u.anio_mes, f.anio_mes) AS anio_mes
# MAGIC       FROM fallback f LEFT JOIN ultimo u USING (business_key)) p
# MAGIC   ON p.business_key = r.business_key AND p.anio_mes = r.anio_mes
# MAGIC WHERE r.rn = 1;

# COMMAND ----------

# DBTITLE 1: Q10 — KPIs negocio versus benchmark (Producto)
# MAGIC %sql
# MAGIC WITH rep AS (
# MAGIC   SELECT b.*, n.nombre,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY b.business_key, b.anio_mes
# MAGIC            ORDER BY CASE WHEN b.eligible_benchmark THEN 0 ELSE 1 END,
# MAGIC                     b.benchmark_level,
# MAGIC                     CASE WHEN b.benchmark_fallback THEN 1 ELSE 0 END) AS rn
# MAGIC   FROM workspace.yelp_gold.gld_dashboard_negocio_mensual b
# MAGIC   JOIN workspace.yelp_gold.dim_negocio n
# MAGIC     ON b.business_key = n.business_key AND n.es_actual = true
# MAGIC ), actual AS (
# MAGIC   SELECT *, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')') AS negocio_etiqueta,
# MAGIC          CONCAT(CASE WHEN eligible_benchmark THEN '1' ELSE '0' END, anio_mes) AS clave_orden
# MAGIC   FROM rep WHERE rn = 1
# MAGIC )
# MAGIC SELECT negocio_etiqueta, anio_mes, rating_mes AS rating_negocio,
# MAGIC        CASE WHEN eligible_benchmark THEN benchmark_rating END AS rating_benchmark,
# MAGIC        CASE WHEN eligible_benchmark THEN delta_rating_vs_benchmark END AS delta_rating,
# MAGIC        n_reviews_mes AS reviews_negocio,
# MAGIC        CASE WHEN eligible_benchmark THEN benchmark_reviews END AS reviews_benchmark,
# MAGIC        n_checkins_mes AS checkins_negocio,
# MAGIC        CASE WHEN eligible_benchmark THEN benchmark_checkins END AS checkins_benchmark,
# MAGIC        CASE WHEN eligible_benchmark THEN ranking_rating END AS ranking_rating,
# MAGIC        benchmark_n_business AS n_negocios_benchmark
# MAGIC FROM actual;

# COMMAND ----------

# DBTITLE 1: Q11 — Ventanas móviles 3m, 6m y 12m (Producto)
# MAGIC %sql
# MAGIC WITH rep AS (
# MAGIC   SELECT b.*, n.nombre,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY b.business_key, b.anio_mes
# MAGIC            ORDER BY CASE WHEN b.eligible_benchmark THEN 0 ELSE 1 END,
# MAGIC                     b.benchmark_level) AS rn
# MAGIC   FROM workspace.yelp_gold.gld_dashboard_negocio_mensual b
# MAGIC   JOIN workspace.yelp_gold.dim_negocio n
# MAGIC     ON b.business_key = n.business_key AND n.es_actual = true
# MAGIC ), actual AS (
# MAGIC   SELECT * FROM rep WHERE rn = 1
# MAGIC )
# MAGIC SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')') AS negocio_etiqueta,
# MAGIC        anio_mes, 'Rating' AS metrica, '3 meses' AS ventana, CAST(rating_3m AS DOUBLE) AS valor FROM actual
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, 'Rating', '6 meses', CAST(rating_6m AS DOUBLE) FROM actual
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, 'Rating', '12 meses', CAST(rating_12m AS DOUBLE) FROM actual
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, 'Reviews', '3 meses', CAST(reviews_3m AS DOUBLE) FROM actual
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, 'Reviews', '6 meses', CAST(reviews_6m AS DOUBLE) FROM actual
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, 'Reviews', '12 meses', CAST(reviews_12m AS DOUBLE) FROM actual
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, 'Check-ins', '3 meses', CAST(checkins_3m AS DOUBLE) FROM actual
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, 'Check-ins', '6 meses', CAST(checkins_6m AS DOUBLE) FROM actual
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, 'Check-ins', '12 meses', CAST(checkins_12m AS DOUBLE) FROM actual;

# COMMAND ----------

# DBTITLE 1: Q12 — Actividad temporal por negocio (Operaciones Comerciales)
# MAGIC %sql
# MAGIC SELECT a.business_key,
# MAGIC        n.nombre,
# MAGIC        CONCAT(n.nombre, ' (', CAST(a.business_key AS STRING), ')') AS negocio_etiqueta,
# MAGIC        TO_DATE(CAST(a.fecha_key AS STRING), 'yyyyMMdd') AS fecha,
# MAGIC        a.dia_semana,
# MAGIC        CASE WHEN a.es_fin_semana THEN 'Fin de semana' ELSE 'Día laborable' END AS tipo_dia,
# MAGIC        a.franja_horaria,
# MAGIC        SUM(a.cantidad_checkins) AS checkins_total
# MAGIC FROM workspace.yelp_gold.gld_actividad_negocio_temporal a
# MAGIC JOIN workspace.yelp_gold.dim_negocio n
# MAGIC   ON a.business_key = n.business_key AND n.es_actual = true
# MAGIC GROUP BY a.business_key, n.nombre, a.fecha_key, a.dia_semana, a.es_fin_semana, a.franja_horaria
# MAGIC ORDER BY fecha, a.franja_horaria;

# COMMAND ----------

# DBTITLE 1: Q13 — Series negocio versus benchmark y satisfacción (Marketing)
# MAGIC %sql
# MAGIC WITH rep AS (
# MAGIC   SELECT b.*, n.nombre,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY b.business_key, b.anio_mes
# MAGIC            ORDER BY CASE WHEN b.eligible_benchmark THEN 0 ELSE 1 END,
# MAGIC                     b.benchmark_level) AS rn
# MAGIC   FROM workspace.yelp_gold.gld_dashboard_negocio_mensual b
# MAGIC   JOIN workspace.yelp_gold.dim_negocio n
# MAGIC     ON b.business_key = n.business_key AND n.es_actual = true
# MAGIC )
# MAGIC SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')') AS negocio_etiqueta,
# MAGIC        anio_mes, TO_DATE(CONCAT(anio_mes, '-01')) AS mes,
# MAGIC        'Rating' AS metrica, 'Negocio' AS tipo, CAST(NULL AS STRING) AS categoria, CAST(rating_mes AS DOUBLE) AS valor
# MAGIC FROM rep WHERE rn = 1
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, TO_DATE(CONCAT(anio_mes, '-01')), 'Rating', 'Benchmark', CAST(NULL AS STRING), CAST(benchmark_rating AS DOUBLE) FROM rep WHERE rn = 1
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, TO_DATE(CONCAT(anio_mes, '-01')), 'Reviews', 'Negocio', CAST(NULL AS STRING), CAST(n_reviews_mes AS DOUBLE) FROM rep WHERE rn = 1
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, TO_DATE(CONCAT(anio_mes, '-01')), 'Reviews', 'Benchmark', CAST(NULL AS STRING), CAST(benchmark_reviews AS DOUBLE) FROM rep WHERE rn = 1
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, TO_DATE(CONCAT(anio_mes, '-01')), 'Check-ins', 'Negocio', CAST(NULL AS STRING), CAST(n_checkins_mes AS DOUBLE) FROM rep WHERE rn = 1
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, TO_DATE(CONCAT(anio_mes, '-01')), 'Check-ins', 'Benchmark', CAST(NULL AS STRING), CAST(benchmark_checkins AS DOUBLE) FROM rep WHERE rn = 1
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, TO_DATE(CONCAT(anio_mes, '-01')), 'Satisfacción', 'Distribución', 'Positivas (4-5 estrellas)', CAST(pct_positivas AS DOUBLE) FROM rep WHERE rn = 1 AND flag_satisfaccion_valida
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, TO_DATE(CONCAT(anio_mes, '-01')), 'Satisfacción', 'Distribución', 'Neutrales (3 estrellas)', CAST(pct_neutrales AS DOUBLE) FROM rep WHERE rn = 1 AND flag_satisfaccion_valida
# MAGIC UNION ALL SELECT business_key, nombre, CONCAT(nombre, ' (', CAST(business_key AS STRING), ')'), anio_mes, TO_DATE(CONCAT(anio_mes, '-01')), 'Satisfacción', 'Distribución', 'Negativas (1-2 estrellas)', CAST(pct_negativas AS DOUBLE) FROM rep WHERE rn = 1 AND flag_satisfaccion_valida;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist de ensamblado en la UI
# MAGIC - [ ] Dashboard con 2 páginas, ≥6 visuales, filtro por ciudad/mes y textos de contexto.
# MAGIC - [ ] Cada visual tiene subtítulo con el stakeholder al que responde.
# MAGIC - [ ] Genie Space creado sobre `yelp_gold` con ≥5 instrucciones/sinónimos del glosario;
# MAGIC       incluir captura de 3 preguntas de negocio respondidas correctamente
# MAGIC       (y 1 donde Genie falló + cómo lo corrigieron con instrucciones → esa iteración se evalúa).
# MAGIC - [ ] Publicar el dashboard y programar refresco alineado al job E7.4.
# MAGIC
# MAGIC ### Definition of Done (E8)
# MAGIC - [ ] Dashboard publicado consumiendo solo gold/gov.
# MAGIC - [ ] Genie Space con evidencia de iteración.
# MAGIC - [ ] Storytelling: 1 slide-resumen por página explicando la decisión que habilita.
