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
# MAGIC
# MAGIC **Prerequisitos:** ejecutar después de E3 (las tablas Silver y Gold deben existir). Antes del control de máscaras, crear en la cuenta de Databricks el grupo `data_stewards` y agregar a sus integrantes autorizados. La función de máscara no crea grupos ni asigna usuarios.

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
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review ALTER COLUMN review_id SET TAGS ('clasificacion' = 'pii_indirecta');
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review ALTER COLUMN fecha_resena SET TAGS ('clasificacion' = 'pii_indirecta');
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review ALTER COLUMN texto SET TAGS ('clasificacion' = 'pii_potencial');
# MAGIC ALTER TABLE workspace.yelp_gold.dim_usuario ALTER COLUMN nombre SET TAGS ('clasificacion' = 'pii_directa');
# MAGIC ALTER TABLE workspace.yelp_gold.dim_usuario ALTER COLUMN user_id SET TAGS ('clasificacion' = 'pii_indirecta');
# MAGIC ALTER TABLE workspace.yelp_gold.dim_usuario ALTER COLUMN user_key SET TAGS ('clasificacion' = 'pii_indirecta');
# MAGIC ALTER TABLE workspace.yelp_gold.fact_resena ALTER COLUMN review_id SET TAGS ('clasificacion' = 'pii_indirecta');
# MAGIC ALTER TABLE workspace.yelp_gold.fact_resena ALTER COLUMN user_key SET TAGS ('clasificacion' = 'pii_indirecta');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1: Control 2 — Permisos de mínimo privilegio
# MAGIC -- Primero retirar el permiso amplio del esquema que se pudo conceder en una ejecución anterior.
# MAGIC -- No otorgar SELECT en Silver ni en las tablas Gold con identificadores de usuarios/reseñas.
# MAGIC REVOKE SELECT ON SCHEMA workspace.yelp_gold FROM `alex.galindo.romero@gmail.com`;
# MAGIC REVOKE SELECT ON SCHEMA workspace.yelp_gold FROM `dfloressgonz@gmail.com`;
# MAGIC
# MAGIC -- Consumidores: acceso solo a productos Gold necesarios para BI; sin dim_usuario,
# MAGIC -- fact_resena, texto de reseñas ni otras tablas que exponen identificadores individuales.
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_gold TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.dim_negocio TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.dim_fecha TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.dim_categoria TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.bridge_negocio_categoria TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.gld_dashboard_negocio_mensual TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.gld_actividad_negocio_temporal TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.fact_checkin_temporal TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_gov TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gov.dq_reglas TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gov.dq_resultados TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gov.diccionario_datos TO `alex.galindo.romero@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gov.glosario_negocio TO `alex.galindo.romero@gmail.com`;
# MAGIC
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `dfloressgonz@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_gold TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.dim_negocio TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.dim_fecha TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.dim_categoria TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.bridge_negocio_categoria TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.gld_dashboard_negocio_mensual TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.gld_actividad_negocio_temporal TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.fact_checkin_temporal TO `dfloressgonz@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_gov TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gov.dq_reglas TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gov.dq_resultados TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gov.diccionario_datos TO `dfloressgonz@gmail.com`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gov.glosario_negocio TO `dfloressgonz@gmail.com`;
# MAGIC
# MAGIC -- Data Steward: acceso privilegiado solo a las columnas/nombres que debe gobernar.
# MAGIC -- Requiere que el grupo exista previamente en la cuenta de Databricks.
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `data_stewards`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_silver TO `data_stewards`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_silver.slv_user TO `data_stewards`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_gold TO `data_stewards`;
# MAGIC GRANT SELECT ON TABLE workspace.yelp_gold.dim_usuario TO `data_stewards`;
# MAGIC
# MAGIC -- El Data Custodian administra/consulta Silver. El acceso a Silver no se da a consumidores.
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `chijar@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.yelp_silver TO `chijar@gmail.com`;
# MAGIC GRANT SELECT ON SCHEMA workspace.yelp_silver TO `chijar@gmail.com`;
# MAGIC
# MAGIC -- Evidencia: revisar estos resultados y SHOW GRANTS de las tablas protegidas.
# MAGIC SHOW GRANTS ON SCHEMA workspace.yelp_gold;
# MAGIC SHOW GRANTS ON TABLE workspace.yelp_gold.dim_usuario;
# MAGIC SHOW GRANTS ON TABLE workspace.yelp_gold.fact_resena;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBTITLE 1: Control 3 — Aplicar enmascaramiento de PII
# MAGIC -- El grupo data_stewards debe existir en la cuenta y tener solo personal autorizado.
# MAGIC -- Se oculta el nombre por completo a cualquier usuario que no sea miembro del grupo.
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.mascara_nombre(nombre STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN nombre IS NULL THEN CAST(NULL AS STRING)
# MAGIC   WHEN is_account_group_member('data_stewards') THEN nombre
# MAGIC   ELSE '****'
# MAGIC END;
# MAGIC
# MAGIC -- Aplicar la misma política tanto en Silver como en la dimensión Gold.
# MAGIC -- Así, el dato queda protegido incluso para roles autorizados a consultar esas tablas.
# MAGIC ALTER TABLE workspace.yelp_silver.slv_user
# MAGIC   ALTER COLUMN nombre SET MASK workspace.yelp_gov.mascara_nombre;
# MAGIC ALTER TABLE workspace.yelp_gold.dim_usuario
# MAGIC   ALTER COLUMN nombre SET MASK workspace.yelp_gov.mascara_nombre;
# MAGIC
# MAGIC -- La política se evalúa con la identidad que consulta; dar EXECUTE a roles autorizados
# MAGIC -- a leer las tablas protegidas. Consumers no tienen SELECT en esas tablas.
# MAGIC GRANT EXECUTE ON FUNCTION workspace.yelp_gov.mascara_nombre TO `data_stewards`;
# MAGIC GRANT EXECUTE ON FUNCTION workspace.yelp_gov.mascara_nombre TO `chijar@gmail.com`;
# MAGIC
# MAGIC -- El acceso de consumidores a dim_usuario, fact_resena y Silver no se concede.
# MAGIC -- El enmascaramiento es defensa adicional, no reemplaza los permisos de mínimo privilegio.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba del enmascaramiento y acceso
# MAGIC
# MAGIC Ejecutar las consultas siguientes iniciando sesión con cada identidad de prueba (no basta
# MAGIC con ejecutarlas como administrador). Unity Catalog no permite simular otro usuario con
# MAGIC `SET ROLE` en esta prueba.
# MAGIC
# MAGIC 1. Con un usuario que **no** pertenece a `data_stewards`, consultar `nombre` en Silver/Gold
# MAGIC    si se le concede acceso temporal para la prueba: debe aparecer `****` (o NULL si el dato es NULL).
# MAGIC 2. Con un miembro autorizado de `data_stewards`, la misma consulta debe mostrar el nombre original.
# MAGIC 3. Con cada consumidor, consultar `workspace.yelp_gold.dim_usuario` y `workspace.yelp_gold.fact_resena`:
# MAGIC    el acceso debe ser denegado. Verificar también que el consumidor puede consultar los productos
# MAGIC    Gold otorgados para BI.
# MAGIC 4. Si se concedió acceso temporal para probar la máscara, revocarlo al terminar.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT current_user() AS usuario,
# MAGIC        is_account_group_member('data_stewards') AS es_data_steward,
# MAGIC        nombre
# MAGIC FROM workspace.yelp_gold.dim_usuario
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC **Nota:** `slv_review.texto` es PII potencial y `user_id`, `review_id` y `fecha_resena` son
# MAGIC identificadores indirectos. Permanecen fuera del acceso de consumidores en este control; no se
# MAGIC deben publicar en vistas de consumo sin una política aprobada de ocultamiento/redacción.
# MAGIC
# MAGIC **Free Edition:** si `SET MASK` o `is_account_group_member` no están disponibles en el workspace,
# MAGIC no se debe declarar el control como aplicado. Como alternativa, crear vistas de consumo con
# MAGIC proyección explícita de columnas y la función de máscara; otorgar acceso solo a esas vistas y
# MAGIC mantener revocado el acceso a las tablas base.

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
# MAGIC - [ ] Nombre de usuario enmascarado y probado con usuarios steward/no-steward; consumidores sin acceso a tablas identificables.
# MAGIC - [x] Lineage de `review` documentado con una captura de Catalog Explorer.
# MAGIC
# MAGIC Para marcar una casilla como `[x]`, ejecuta el control correspondiente y guarda una captura como evidencia.