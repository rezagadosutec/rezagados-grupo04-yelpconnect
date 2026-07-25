# Databricks notebook source
# MAGIC %md
# MAGIC # E7.4 · Orquestación, monitoreo y FinOps
# MAGIC **Cierra con:** M10 DataOps / MLOps / FinOps
# MAGIC
# MAGIC El pipeline deja de ser "notebooks que alguien corre a mano" y se vuelve un
# MAGIC **Lakeflow Job** con dependencias, reintentos y alertas. Es el entregable de
# MAGIC operativización que convierte el capstone en una plataforma, no en un demo.
# MAGIC
# MAGIC ---
# MAGIC ## 1. Crear el Job (UI: Jobs & Pipelines → Create Job)
# MAGIC Grafo de tareas requerido:
# MAGIC ```
# MAGIC 01_bronze_ingesta ──► 02_silver_limpieza ──► 03_gold_productos_datos
# MAGIC                              │ (el gate DQ vive dentro de silver)
# MAGIC                              └─► si falla → notificación por email al grupo
# MAGIC ```
# MAGIC Configuración mínima evaluada:
# MAGIC - [ ] 3 tareas con dependencias (no un solo notebook gigante).
# MAGIC - [ ] Retries: 1 reintento con backoff en cada tarea.
# MAGIC - [ ] Timeout por tarea (protege la cuota diaria de Free Edition).
# MAGIC - [ ] Notificación on-failure al correo del grupo.
# MAGIC - [ ] Trigger: programado (ej. diario) O file-arrival sobre el Volume — justificar la elección.
# MAGIC
# MAGIC > ✏️ TODO — Insertar captura del grafo del job y de una corrida exitosa + una corrida
# MAGIC > donde el gate de calidad detuvo el pipeline (forzar una regla para provocarlo).

# COMMAND ----------

# DBTITLE 1: 2. Monitoreo — bitácora de corridas del pipeline
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gov.pipeline_bitacora (
# MAGIC   ejecucion_ts     TIMESTAMP,
# MAGIC   capa             STRING,
# MAGIC   tabla            STRING,
# MAGIC   filas            BIGINT,
# MAGIC   duracion_seg     DOUBLE
# MAGIC ) COMMENT 'Observabilidad del pipeline: volumetría y duración por corrida';
# MAGIC
# MAGIC -- ✏️ TODO: instrumentar los notebooks 01-03 para que registren aquí sus métricas
# MAGIC --          (helper sugerido: función registrar(capa, tabla, filas, t0) en cada notebook).

# COMMAND ----------

# DBTITLE 1: 3. FinOps en Free Edition
# MAGIC %md
# MAGIC No hay factura, pero SÍ hay cuota — el mejor simulador de FinOps posible.
# MAGIC > ✏️ TODO — Documentar 3 decisiones de eficiencia tomadas durante el proyecto y su efecto,
# MAGIC > por ejemplo: muestreo del dataset, `trigger(availableNow)` en vez de streaming continuo,
# MAGIC > evitar `count()`/`display()` innecesarios, particionado o `OPTIMIZE` de tablas grandes.
# MAGIC > Estimar además el costo mensual del pipeline si corriera en un workspace pagado
# MAGIC > (DBU serverless × corridas) — conectar con la propuesta enterprise de E2.
# MAGIC
# MAGIC ### Definition of Done (E7.4)
# MAGIC - [ ] Job multi-tarea operativo con retries, timeout y alertas (capturas).
# MAGIC - [ ] Evidencia del gate de calidad deteniendo una corrida.
# MAGIC - [ ] Bitácora poblada con ≥3 corridas.
# MAGIC - [ ] Análisis FinOps con estimación de costo enterprise.
