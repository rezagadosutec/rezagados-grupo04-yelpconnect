# Databricks notebook source
# MAGIC %md
# MAGIC # E6.1 · Estrategia de Gobierno de Datos
# MAGIC **Cierra con:** M2 Seguridad, Gobierno y Compliance (se refina durante todo el programa)
# MAGIC
# MAGIC El corazón del caso: **nadie es dueño de nada** en YelpConnect. Este entregable
# MAGIC define el modelo operativo de gobierno y lo aterriza en la matriz de roles.
# MAGIC
# MAGIC ---
# MAGIC ## 1. Modelo operativo de gobierno
# MAGIC > ✏️ TODO — Elegir y justificar: ¿centralizado, federado o híbrido? ¿Consejo de datos?
# MAGIC > ¿Qué encaja con la cultura de las 6 áreas descritas en el caso? (máx. 1 página)
# MAGIC
# MAGIC ## 2. Definición de roles
# MAGIC > ✏️ TODO — Data Owner / Data Steward / Data Custodian: responsabilidades concretas,
# MAGIC > NO definiciones de libro. Ej.: "El Owner de Reseñas aprueba cambios al umbral de
# MAGIC > la regla DQ-002 y firma las excepciones de calidad".
# MAGIC
# MAGIC ## 3. Políticas mínimas (3 obligatorias)
# MAGIC > ✏️ TODO — (a) Política de clasificación de datos, (b) Política de acceso por capa
# MAGIC > medallón, (c) Política de ciclo de vida/retención. Formato: propósito, alcance,
# MAGIC > enunciados normativos ("debe/no debe"), excepciones, responsable.

# COMMAND ----------

# DBTITLE 1: Matriz RACI de entidades (ejemplo resuelto — completar todas las entidades)
from pyspark.sql import Row

raci = [
    Row(entidad="review", dominio="Reseñas",
        data_owner="Trust & Safety (Rafael Nakamura)",
        data_steward="✏️ integrante que simula el rol",
        data_custodian="Data Platform (Laura Hernández)",
        consumidores="Marketing; Data Science; Producto",
        clasificacion="interna"),
    # ✏️ TODO — business, user (¡PII!), tip, checkin y las entidades gold
]
spark.createDataFrame(raci).write.mode("overwrite").saveAsTable("workspace.yelp_gov.matriz_raci")
display(spark.table("workspace.yelp_gov.matriz_raci"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. KPIs de gobierno (se exhiben en el dashboard E8)
# MAGIC > ✏️ TODO — Definir 3-5 KPIs medibles con lo construido: % columnas documentadas (E4),
# MAGIC > % entidades con owner (esta matriz), % reglas DQ en verde (E5), % columnas PII con tag.
# MAGIC
# MAGIC ### Definition of Done (E6.1)
# MAGIC - [ ] Modelo operativo elegido con justificación anclada al caso.
# MAGIC - [ ] Matriz RACI completa: TODAS las entidades silver y gold tienen owner.
# MAGIC - [ ] 3 políticas escritas en formato normativo.
# MAGIC - [ ] KPIs de gobierno definidos y calculables con las tablas de `yelp_gov`.
