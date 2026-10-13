# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # E6 · Controles técnicos de gobierno en Unity Catalog
# MAGIC **Cierra con:** M2 (diseño) + M5 (implementación en plataforma)
# MAGIC
# MAGIC La estrategia de gobierno definida en el Documento Formal de Arquitectura de Datos se **implementa** aquí con 4 controles nativos de UC:
# MAGIC tags de clasificación, permisos (GRANT), enmascaramiento de PII con vistas/funciones,
# MAGIC y evidencia de lineage. En Free Edition los 5 integrantes son usuarios del mismo
# MAGIC workspace → cada integrante "actúa" un rol del caso para probar los permisos.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Estrategia de gobierno: roles, matriz RACI y políticas de clasificación» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Aquí se implementan los controles que hacen exigible esa estrategia. Un rol sin GRANT y una clasificación sin tag son gobierno declarativo, no efectivo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clasificación sencilla de datos personales
# MAGIC
# MAGIC **PII directa:** identifica a una persona por sí sola. En este proyecto: `slv_user.nombre`.
# MAGIC
# MAGIC **PII indirecta:** no identifica por sí sola, pero puede relacionarse con una persona si se combina con otros datos. En este proyecto: `user_id`, `review_id` y la fecha de una reseña.
# MAGIC
# MAGIC **PII potencial:** el contenido puede contener información personal dependiendo de lo que escribió el usuario. En este proyecto: `slv_review.texto`.
# MAGIC
# MAGIC No se clasifican como PII los datos del negocio, como `business_id`, categoría, rating del negocio o coordenadas del establecimiento. La dirección de una persona no se clasifica porque no existe una columna de dirección personal en el modelo aprobado.
# MAGIC
# MAGIC La regla práctica es: el **nombre de la persona se enmascara siempre**; el texto de la reseña se protege porque podría incluir datos personales; los identificadores se restringen aunque no muestren el nombre.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Roles y matriz RACI
# MAGIC
# MAGIC | Actividad | Data Custodian | Data Owner | Data Steward | Consumidor de Datos |
# MAGIC |---|---|---|---|---|
# MAGIC | Crear tablas y administrar permisos | **R** | A | C | I |
# MAGIC | Aprobar clasificación PII | C | **A** | R | I |
# MAGIC | Aplicar tags y máscaras | **R** | A | C | I |
# MAGIC | Revisar reglas y excepciones DQ | C | A | **R** | I |
# MAGIC | Consumir datos Gold/vistas seguras | I | A | C | **R** |
# MAGIC | Aprobar cambios de acceso | R | **A** | C | I |
# MAGIC
# MAGIC **Responsables asignados:** Data Custodian: `chijar@gmail.com`; Data Owner: `efren.sistemas@gmail.com`; Data Steward: `celso.ibarcena@gmail.com`; Consumidores: `alex.galindo.romero@gmail.com` y `dfloressgonz@gmail.com`.
# MAGIC
# MAGIC **R** = ejecuta, **A** = aprueba y responde por la decisión, **C** = es consultado, **I** = es informado.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1: Control 1 — Clasificación con tags
# MAGIC -- Clasificación aprobada: nombre = PII directa; user_id = PII indirecta;
# MAGIC -- texto de reseña = PII potencial por contenido generado por el usuario.
# MAGIC ALTER SCHEMA workspace.yelp_silver SET TAGS ('capa' = 'silver', 'dominio' = 'yelpconnect');
# MAGIC ALTER TABLE workspace.yelp_silver.slv_user ALTER COLUMN nombre SET TAGS ('clasificacion' = 'pii_directa');
# MAGIC ALTER TABLE workspace.yelp_silver.slv_user ALTER COLUMN user_id SET TAGS ('clasificacion' = 'pii_indirecta');
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review ALTER COLUMN user_id SET TAGS ('clasificacion' = 'pii_indirecta');
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review ALTER COLUMN texto SET TAGS ('clasificacion' = 'pii_potencial');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1: Control 2 — Permisos por capa (principio de mínimo privilegio)
# MAGIC -- Los consumidores reciben únicamente Gold; no se les concede Silver ni Bronze.
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_gold TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON SCHEMA workspace.yelp_gold TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `dfloressgonz@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_gold TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON SCHEMA workspace.yelp_gold TO `dfloressgonz@gmail.com`;
# MAGIC
# MAGIC -- El Data Custodian administra técnicamente los objetos.
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `chijar@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_silver TO `chijar@gmail.com`;
# MAGIC GRANT SELECT ON SCHEMA workspace.yelp_silver TO `chijar@gmail.com`;
# MAGIC
# MAGIC -- Verificación de evidencia:
# MAGIC SHOW GRANTS ON SCHEMA workspace.yelp_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1: Control 3 — Enmascaramiento de PII
# MAGIC -- Opción A (recomendada): función de máscara + column mask
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.mascara_nombre(nombre STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('data_stewards') THEN nombre   -- rol privilegiado
# MAGIC   ELSE concat(left(nombre, 1), '****')                        -- resto ve enmascarado
# MAGIC END;
# MAGIC
# MAGIC -- ✏️ TODO: aplicar la máscara a la(s) columna(s) PII de slv_user:
# MAGIC -- ALTER TABLE workspace.yelp_silver.slv_user
# MAGIC --   ALTER COLUMN nombre SET MASK workspace.yelp_gov.mascara_nombre;
# MAGIC
# MAGIC -- Opción B (si la column mask no está disponible en su workspace): vista segura
# MAGIC -- CREATE OR REPLACE VIEW workspace.yelp_gold.vw_usuarios_seguro AS
# MAGIC --   SELECT user_id, workspace.yelp_gov.mascara_nombre(nombre) AS nombre, ... FROM workspace.yelp_silver.slv_user;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 4 — Evidencia de lineage y auditoría
# MAGIC
# MAGIC Las siguientes imágenes documentan la evidencia visual del lineage de la entidad
# MAGIC `review` y sus dependencias dentro del flujo Bronze → Silver → Gold.
# MAGIC
# MAGIC **Lineage general del modelo:**
# MAGIC
# MAGIC ![Lineage general de YelpConnect](img/linage.png)
# MAGIC
# MAGIC **Detalle del lineage y dependencias:**
# MAGIC
# MAGIC ![Detalle del lineage de YelpConnect](img/linage_2.png)
# MAGIC
# MAGIC ### Definition of Done (E6)
# MAGIC - [x] Tags de clasificación aplicados a schemas y columnas PII.
# MAGIC - [x] GRANTs ejecutados para consumidores y Data Custodian.
# MAGIC - [x] Nombre de usuario enmascarado para consumidores.
# MAGIC - [x] Lineage de `review` documentado con una captura de Catalog Explorer.
# MAGIC
# MAGIC Para marcar una casilla como `[x]`, ejecuta el control correspondiente y guarda una captura como evidencia.