# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EX · Asistente de insights de reseñas para Producto
# MAGIC **Cierra con:** M8 IA y Datos No Estructurados · **Puntaje:** bonus
# MAGIC
# MAGIC **Caso de uso para la demo:** un copiloto ayuda a Producto a investigar un negocio
# MAGIC cuya satisfacción mensual cae: encuentra reseñas recientes de baja calificación,
# MAGIC agrupa los temas y presenta hasta 10 ejemplos para revisión humana.
# MAGIC El job prepara y prioriza la evidencia; Genie permite explorarla en lenguaje natural.
# MAGIC El resultado es una señal para investigar, no una decisión automática.
# MAGIC
# MAGIC **Límite del dato:** Yelp Open Dataset contiene reseñas sobre negocios locales, no sobre
# MAGIC la aplicación YelpConnect. Este caso ayuda a entender la experiencia en los negocios
# MAGIC listados; no mide directamente la usabilidad de la app.
# MAGIC
# MAGIC ⚠️ FinOps: las AI Functions consumen cuota. La primera demo procesa solo 10 reseñas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1: 1 — Elegir un negocio con una señal que Producto pueda investigar
# MAGIC -- Busca negocios con rating menor que en su mes previo con reseñas (mínimo 5 reseñas
# MAGIC -- en cada punto). Elige un business_id del resultado y úsalo en la celda siguiente.
# MAGIC -- La comparación es exploratoria: los meses comparados pueden no ser consecutivos.
# MAGIC WITH historial AS (
# MAGIC   SELECT d.business_key, d.anio_mes, d.rating_mes, d.n_reviews_mes,
# MAGIC          lag(d.rating_mes) OVER (PARTITION BY d.business_key ORDER BY d.anio_mes) AS rating_previo,
# MAGIC          lag(d.anio_mes) OVER (PARTITION BY d.business_key ORDER BY d.anio_mes) AS mes_previo
# MAGIC   FROM workspace.yelp_gold.gld_dashboard_negocio_mensual d
# MAGIC   WHERE d.n_reviews_mes >= 5 AND d.rating_mes IS NOT NULL
# MAGIC ),
# MAGIC ultimo AS (
# MAGIC   SELECT *, row_number() OVER (PARTITION BY business_key ORDER BY anio_mes DESC) AS rn
# MAGIC   FROM historial
# MAGIC )
# MAGIC SELECT b.business_id, b.nombre, b.ciudad, u.mes_previo, u.anio_mes AS mes_actual,
# MAGIC        u.rating_previo, u.rating_mes, round(u.rating_mes - u.rating_previo, 2) AS delta_rating,
# MAGIC        u.n_reviews_mes
# MAGIC FROM ultimo u
# MAGIC JOIN workspace.yelp_gold.dim_negocio b USING (business_key)
# MAGIC WHERE u.rn = 1 AND u.rating_previo IS NOT NULL AND u.rating_mes < u.rating_previo
# MAGIC ORDER BY delta_rating ASC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1: 2 — Analizar 10 reseñas recientes de baja calificación
# MAGIC -- Reemplazar el valor por el business_id copiado de la consulta anterior.
# MAGIC -- Tomamos hasta 10 reseñas recientes de 1-2 estrellas en los dos meses calendario
# MAGIC -- más recientes disponibles. Así probamos IA con costo acotado y un caso relevante.
# MAGIC CREATE OR REPLACE TABLE workspace.yelp_gold.gld_resenas_enriquecidas
# MAGIC COMMENT 'Hasta 10 reseñas recientes de baja calificación de un negocio, enriquecidas con IA para demo de Producto; texto con acceso restringido.'
# MAGIC AS
# MAGIC WITH parametros AS (
# MAGIC   SELECT 'j-qtdD55OLfSqfsWuQTDJg' AS business_id
# MAGIC ),
# MAGIC max_fecha AS (
# MAGIC   SELECT max(r.fecha_resena) AS fecha_max
# MAGIC   FROM workspace.yelp_silver.slv_review r
# MAGIC   CROSS JOIN parametros p
# MAGIC   WHERE r.business_id = p.business_id
# MAGIC ),
# MAGIC candidatas AS (
# MAGIC   SELECT r.review_id, r.business_id, r.estrellas, r.fecha_resena,
# MAGIC          r.texto, r.votos_utiles,
# MAGIC          row_number() OVER (ORDER BY r.fecha_resena DESC, r.review_id) AS rn
# MAGIC   FROM workspace.yelp_silver.slv_review r
# MAGIC   CROSS JOIN parametros p
# MAGIC   CROSS JOIN max_fecha f
# MAGIC   WHERE r.business_id = p.business_id
# MAGIC     AND r.fecha_resena >= add_months(date_trunc('MONTH', f.fecha_max), -1)
# MAGIC     AND r.fecha_resena <= f.fecha_max
# MAGIC     AND r.estrellas <= 3
# MAGIC     AND r.texto IS NOT NULL AND trim(r.texto) <> ''
# MAGIC )
# MAGIC SELECT review_id, business_id, estrellas, fecha_resena, texto, votos_utiles,
# MAGIC        ai_analyze_sentiment(texto) AS sentimiento,
# MAGIC        ai_classify(texto, array('comida','servicio','precio','ambiente','otro')) AS tema
# MAGIC FROM candidatas
# MAGIC WHERE rn <= 500;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1: 3 — Validar las etiquetas antes de confiar en ellas
# MAGIC -- Inspeccionar los valores reales que devolvió el modelo.
# MAGIC -- Revisar manualmente ejemplos; las etiquetas IA son señales, no verdad de negocio.
# MAGIC SELECT tema, sentimiento, COUNT(*) AS cantidad
# MAGIC FROM workspace.yelp_gold.gld_resenas_enriquecidas
# MAGIC GROUP BY tema, sentimiento
# MAGIC ORDER BY cantidad DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1: 4 — Crear una vista de consumo para Genie
# MAGIC -- La vista añade el nombre/ciudad del negocio y el mes para que Producto pueda
# MAGIC -- preguntar por tendencias y abrir reseñas de ejemplo. No exponerla a todo usuario:
# MAGIC -- el texto puede contener información personal.
# MAGIC CREATE OR REPLACE VIEW workspace.yelp_gold.vw_resenas_producto AS
# MAGIC SELECT
# MAGIC   r.review_id,
# MAGIC   r.business_id,
# MAGIC   b.nombre AS negocio,
# MAGIC   b.ciudad,
# MAGIC   date_format(r.fecha_resena, 'yyyy-MM') AS anio_mes,
# MAGIC   r.fecha_resena,
# MAGIC   r.estrellas,
# MAGIC   r.tema,
# MAGIC   r.sentimiento,
# MAGIC   r.texto,
# MAGIC   d.rating_mes,
# MAGIC   d.n_reviews_mes,
# MAGIC   d.pct_negativas AS pct_resenas_bajas_mes
# MAGIC FROM workspace.yelp_gold.gld_resenas_enriquecidas r
# MAGIC LEFT JOIN workspace.yelp_gold.dim_negocio b
# MAGIC   ON r.business_id = b.business_id AND b.es_actual = true
# MAGIC LEFT JOIN workspace.yelp_gold.gld_dashboard_negocio_mensual d
# MAGIC   ON b.business_key = d.business_key
# MAGIC  AND date_format(r.fecha_resena, 'yyyy-MM') = d.anio_mes;

# COMMAND ----------

# DBTITLE 1,5 — Obtener los 10 casos prioritarios y sus temas
# MAGIC %sql
# MAGIC -- DBTITLE 1: 5 — Obtener los 10 casos prioritarios y sus temas
# MAGIC -- Rating 1-2 define candidato de revisión. Orden: estrellas más bajas, sentimiento
# MAGIC -- negativo si el modelo lo identificó, recencia y votos útiles. No equivale a fraude
# MAGIC -- ni a severidad confirmada. Mostrar menos de 10 si no hay suficientes candidatas.
# MAGIC WITH priorizadas AS (
# MAGIC   SELECT v.negocio, v.ciudad, v.anio_mes, v.review_id, v.fecha_resena, v.estrellas, v.tema,
# MAGIC          v.sentimiento, t.votos_utiles, v.texto,
# MAGIC          row_number() OVER (
# MAGIC            ORDER BY v.estrellas ASC,
# MAGIC                     CASE WHEN lower(v.sentimiento) LIKE '%negative%'
# MAGIC                                OR lower(v.sentimiento) LIKE '%negativ%' THEN 0 ELSE 1 END,
# MAGIC                     v.fecha_resena DESC,
# MAGIC                     coalesce(t.votos_utiles, 0) DESC
# MAGIC          ) AS prioridad
# MAGIC   FROM workspace.yelp_gold.vw_resenas_producto v
# MAGIC   LEFT JOIN workspace.yelp_gold.gld_resenas_enriquecidas t
# MAGIC     ON t.review_id = v.review_id
# MAGIC   WHERE v.estrellas <= 3
# MAGIC )
# MAGIC SELECT prioridad, negocio, ciudad, anio_mes, review_id, fecha_resena,
# MAGIC        estrellas, tema, sentimiento, votos_utiles, texto
# MAGIC FROM priorizadas
# MAGIC WHERE prioridad <= 10
# MAGIC ORDER BY prioridad;
# MAGIC
# MAGIC -- Resumen de temas entre las reseñas candidatas (rating 1-2) del negocio seleccionado.
# MAGIC SELECT tema, COUNT(*) AS cantidad_resenas
# MAGIC FROM workspace.yelp_gold.vw_resenas_producto
# MAGIC WHERE estrellas <= 3
# MAGIC GROUP BY tema
# MAGIC ORDER BY cantidad_resenas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear el asistente en Databricks (Genie Space)
# MAGIC 1. Crear un Genie Space llamado **Asistente de insights de reseñas — Producto**.
# MAGIC 2. Añadir `workspace.yelp_gold.vw_resenas_producto` como fuente. La vista ya combina
# MAGIC    reseñas, negocio, mes y métricas mensuales Gold.
# MAGIC 3. Instrucciones de Genie: el conjunto contiene hasta 10 reseñas de baja calificación
# MAGIC    de un negocio y de sus dos meses calendario más recientes. Solo analizar el negocio
# MAGIC    que aparece en la vista. `tema` y `sentimiento` son predicciones IA; citar ejemplos,
# MAGIC    indicar cuántas reseñas hay y advertir que 10 reseñas no representan toda la clientela.
# MAGIC    No inferir causalidad ni fraude.
# MAGIC 4. Probar las preguntas de abajo y contrastar las respuestas con las consultas SQL.
# MAGIC    Conceder acceso al texto solo a usuarios autorizados.
# MAGIC
# MAGIC **Preguntas de prueba:**
# MAGIC - ¿Cuáles son las diez reseñas candidatas de este negocio y por qué quedaron primero?
# MAGIC - ¿Qué temas aparecen más en esas reseñas? Muéstrame ejemplos.
# MAGIC - ¿Qué rating y volumen tuvo este negocio en los meses analizados?
# MAGIC
# MAGIC Genie explica y recupera evidencia; el equipo de Producto decide si investigar. Una
# MAGIC reseña priorizada no es una denuncia confirmada ni prueba de fraude.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (EX)
# MAGIC - [ ] Seleccionado un negocio con caída observable de rating y suficiente volumen.
# MAGIC - [ ] Tabla enriquecida creada con hasta 10 reseñas de baja calificación del negocio/periodo.
# MAGIC - [ ] Etiquetas revisadas manualmente y SQL devuelve hasta 10 candidatas con evidencia.
# MAGIC - [ ] Genie responde las preguntas de prueba y sus resultados concuerdan con SQL.
# MAGIC - [ ] Limitaciones, acceso al texto y revisión humana documentados.