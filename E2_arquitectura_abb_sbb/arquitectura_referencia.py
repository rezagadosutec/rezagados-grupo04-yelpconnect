# Databricks notebook source
# MAGIC %md
# MAGIC # E2 · Arquitectura de Referencia — ABB / SBB
# MAGIC **Cierra con:** M5 Arquitectura Cloud (borrador conceptual desde M1) · **Marco:** TOGAF Fases B-D
# MAGIC
# MAGIC El entregable tiene dos niveles: los **Architecture Building Blocks (ABB)** —qué
# MAGIC capacidades necesita la plataforma, agnósticas de tecnología— y los **Solution
# MAGIC Building Blocks (SBB)** —con qué componentes concretos se implementa cada capacidad.
# MAGIC La gracia pedagógica: el SBB "real" propuesto para Yelp (cloud enterprise) y el SBB
# MAGIC "implementado" en el capstone (Databricks Free Edition) **no son iguales**, y ustedes
# MAGIC deben justificar el mapeo.
# MAGIC
# MAGIC ---
# MAGIC ## 1. Arquitectura conceptual (ABB)
# MAGIC > ✏️ TODO — Diagrama de capas: Ingesta · Almacenamiento · Procesamiento · Gobierno ·
# MAGIC > Calidad · Serving/BI · Seguridad transversal. `![abb](img/abb.png)`
# MAGIC
# MAGIC ## 2. Catálogo de ABBs
# MAGIC | ABB | Capacidad que provee | Requerimiento del caso que atiende |
# MAGIC |---|---|---|
# MAGIC | Ingesta batch/streaming | ✏️ TODO | Fuentes móvil, web, APIs de partners |
# MAGIC | Almacenamiento lakehouse | ✏️ TODO | ✏️ TODO |
# MAGIC | Catálogo y gobierno | ✏️ TODO | Falta de ownership y trazabilidad |
# MAGIC | Motor de calidad | ✏️ TODO | Duplicados y reseñas falsas |
# MAGIC | Serving BI/ML | ✏️ TODO | ✏️ TODO |
# MAGIC
# MAGIC ## 3. Arquitectura tecnológica (SBB) — propuesta enterprise
# MAGIC > ✏️ TODO — Selección justificada de componentes cloud (pueden proponer Azure/AWS/GCP).
# MAGIC > Incluir el diagrama de solución: `![sbb](img/sbb.png)`
# MAGIC
# MAGIC ## 4. Matriz de trazabilidad ABB → SBB enterprise → SBB capstone (Free Edition)
# MAGIC | ABB | SBB enterprise propuesto | SBB en el capstone | Brecha / justificación |
# MAGIC |---|---|---|---|
# MAGIC | Ingesta | ✏️ ej. Event Hubs + ADF | Auto Loader / read_files sobre Volume | ✏️ TODO |
# MAGIC | Catálogo | ✏️ ej. Unity Catalog + Purview | Unity Catalog (workspace) | ✏️ TODO |
# MAGIC | ... | | | |
# MAGIC
# MAGIC ## 5. Decisiones de arquitectura (ADRs)
# MAGIC > ✏️ TODO — Mínimo 3 ADRs en formato: *Contexto / Decisión / Alternativas descartadas / Consecuencias*.
# MAGIC > Ej.: ¿por qué medallón y no data vault en silver? ¿por qué lakehouse y no DWH clásico?
# MAGIC
# MAGIC ## 6. Atributos de calidad (escenarios)
# MAGIC > ✏️ TODO — 3 escenarios de atributos de calidad (escalabilidad, disponibilidad, costo)
# MAGIC > en formato estímulo → respuesta → medida.
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E2)
# MAGIC - [ ] Diagramas ABB y SBB exportados e insertados.
# MAGIC - [ ] Matriz de trazabilidad completa para TODAS las capacidades.
# MAGIC - [ ] ≥ 3 ADRs con alternativas descartadas (no solo la decisión ganadora).
# MAGIC - [ ] La arquitectura implementada en E7 es consistente con lo declarado aquí.
