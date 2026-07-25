# Databricks notebook source
# MAGIC %md
# MAGIC # E3.2 · Modelo Físico — DDL sobre Delta Lake / Unity Catalog
# MAGIC **Cierra con:** M4 Modelado Avanzado · Se implementa en `yelp_silver` (conformado) y `yelp_gold` (dimensional).
# MAGIC
# MAGIC Reglas del entregable:
# MAGIC 1. **Toda tabla y toda columna llevan `COMMENT`** — ese metadata alimenta E4 (diccionario) automáticamente.
# MAGIC 2. Declarar constraints informativas de UC: `PRIMARY KEY` / `FOREIGN KEY` (no se
# MAGIC    enforcan, pero documentan el modelo y habilitan optimizaciones y lineage).
# MAGIC 3. Constraints sí enforzadas donde aplique: `NOT NULL` y `CHECK`.
# MAGIC
# MAGIC Abajo va UNA tabla completa de ejemplo; el grupo completa el resto según su modelo lógico.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ======================= EJEMPLO RESUELTO: slv_business =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business (
# MAGIC   business_id   STRING NOT NULL COMMENT 'Identificador único del negocio en YelpConnect',
# MAGIC   nombre        STRING NOT NULL COMMENT 'Razón comercial del negocio',
# MAGIC   ciudad        STRING          COMMENT 'Ciudad normalizada (title case, sin espacios extra)',
# MAGIC   estado        STRING          COMMENT 'Código de estado/provincia (2 letras)',
# MAGIC   latitud       DOUBLE          COMMENT 'Latitud geográfica',
# MAGIC   longitud      DOUBLE          COMMENT 'Longitud geográfica',
# MAGIC   estrellas     DOUBLE          COMMENT 'Rating promedio publicado (1.0 a 5.0)',
# MAGIC   n_resenas     INT             COMMENT 'Cantidad de reseñas acumuladas',
# MAGIC   esta_abierto  BOOLEAN         COMMENT 'TRUE si el negocio sigue operativo',
# MAGIC   categorias    ARRAY<STRING>   COMMENT 'Categorías del negocio ya parseadas del string original',
# MAGIC   fecha_carga   TIMESTAMP       COMMENT 'Timestamp de procesamiento del pipeline (auditoría)',
# MAGIC   CONSTRAINT pk_business PRIMARY KEY (business_id),
# MAGIC   CONSTRAINT chk_estrellas CHECK (estrellas BETWEEN 1.0 AND 5.0)
# MAGIC )
# MAGIC COMMENT 'Negocios locales registrados en YelpConnect — capa conformada. Data Owner: Operaciones Comerciales.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ✏️ TODO (grupo): slv_review — incluir FK a business y user, CHECK sobre stars,
# MAGIC -- y decidir/documentar el tratamiento del texto de la reseña (PII indirecta).
# MAGIC -- CREATE TABLE workspace.yelp_silver.slv_review ( ... );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ✏️ TODO (grupo): slv_user — marcar columnas PII con tags en E6 (nombre, etc.)
# MAGIC -- ✏️ TODO (grupo): slv_tip, slv_checkin (¡ojo!: checkin llega como string de fechas
# MAGIC --                  concatenadas → decidir si explota a grano evento o agrega a diario)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ✏️ TODO (grupo): capa GOLD dimensional según E3.1
# MAGIC -- dim_negocio, dim_usuario, dim_fecha, fact_resena, fact_checkin_diario
# MAGIC -- Recordar: PK/FK informativas entre hechos y dimensiones.

# COMMAND ----------

# DBTITLE 1: Validación automática del entregable (correr antes del PR)
tablas = spark.sql("SHOW TABLES IN workspace.yelp_silver").collect()
assert len(tablas) >= 5, "Deben existir al menos las 5 entidades en silver"

sin_comment = spark.sql("""
  SELECT table_name, column_name
  FROM workspace.information_schema.columns
  WHERE table_schema IN ('yelp_silver','yelp_gold')
    AND (comment IS NULL OR comment = '')
""").collect()
assert not sin_comment, f"❌ Columnas sin COMMENT (rompe E4): {[(r.table_name, r.column_name) for r in sin_comment]}"
print("✔ Modelo físico validado: todas las columnas documentadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E3.2)
# MAGIC - [ ] DDL completo de silver (5 entidades mínimo) y gold (esquema estrella).
# MAGIC - [ ] 100% de columnas con COMMENT (la celda de validación pasa).
# MAGIC - [ ] PK/FK declaradas y CHECK donde el negocio lo exige.
# MAGIC - [ ] Captura del diagrama de lineage/ER de Catalog Explorer en `img/`.
