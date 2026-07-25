# Databricks notebook source
# MAGIC %md
# MAGIC # E3.1 · Modelo de Datos — Conceptual y Lógico
# MAGIC **Cierra con:** M4 Modelado Avanzado de Datos
# MAGIC
# MAGIC Se modelan las entidades críticas de YelpConnect identificadas en E1
# MAGIC (`business`, `review`, `user`, `tip`, `checkin` + las que el grupo justifique).
# MAGIC
# MAGIC ---
# MAGIC ## 1. Modelo conceptual
# MAGIC Entidades de negocio y relaciones, SIN atributos técnicos. Usar notación
# MAGIC crow's foot. Puede dibujarse en draw.io y exportarse, o escribirse en Mermaid:
# MAGIC
# MAGIC ```mermaid
# MAGIC erDiagram
# MAGIC     USUARIO ||--o{ RESENA : escribe
# MAGIC     NEGOCIO ||--o{ RESENA : recibe
# MAGIC     %% ✏️ TODO — completar entidades TIP, CHECKIN, CATEGORIA, y las relaciones N:M
# MAGIC ```
# MAGIC
# MAGIC > ✏️ TODO — Justificar en 1 párrafo por qué estas son las entidades "críticas"
# MAGIC > (conectar con los procesos de negocio del caso: fraude, campañas, pricing de ads).
# MAGIC
# MAGIC ## 2. Modelo lógico
# MAGIC Atributos, llaves primarias/foráneas, cardinalidades y resolución de N:M.
# MAGIC Independiente del motor (sin tipos Delta/Spark todavía).
# MAGIC
# MAGIC > ✏️ TODO — Diagrama lógico + tabla de decisiones de normalización.
# MAGIC > Decisión clave a documentar: `categories` de business llega como string con comas →
# MAGIC > ¿entidad CATEGORIA + tabla puente, o array? Justificar.
# MAGIC
# MAGIC ## 3. Modelo dimensional (para la capa Gold)
# MAGIC El caso pide productos analíticos → diseñar el esquema estrella que soportará E8:
# MAGIC
# MAGIC | Elemento | Propuesta | Grano |
# MAGIC |---|---|---|
# MAGIC | Hecho: `fact_resena` | ✏️ TODO | 1 fila = 1 reseña |
# MAGIC | Hecho: `fact_checkin_diario` | ✏️ TODO | 1 fila = negocio × día |
# MAGIC | Dimensión: `dim_negocio` | ✏️ TODO (¿SCD tipo 2? justificar) | |
# MAGIC | Dimensión: `dim_usuario` | ✏️ TODO | |
# MAGIC | Dimensión: `dim_fecha` | ✏️ TODO | |
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E3.1)
# MAGIC - [ ] Conceptual: todas las entidades del caso + relaciones con cardinalidad.
# MAGIC - [ ] Lógico: PK/FK definidas, N:M resueltas, decisiones de normalización escritas.
# MAGIC - [ ] Dimensional: grano declarado por tabla de hechos y tipo de SCD justificado.
# MAGIC - [ ] Coherencia: el DDL de `02_modelo_fisico_ddl` implementa EXACTAMENTE este modelo.
