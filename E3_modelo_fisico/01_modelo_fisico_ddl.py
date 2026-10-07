# Databricks notebook source
# MAGIC %md
# MAGIC # E3 · Modelo físico — DDL sobre Delta Lake / Unity Catalog
# MAGIC **Cierra con:** M4 Modelado Avanzado. Este notebook materializa el modelo conformado de Silver y el modelo dimensional/producto analítico de Gold.
# MAGIC
# MAGIC Principios aplicados:
# MAGIC 1. Toda tabla y columna tiene `COMMENT`; esta metadata alimenta E4.
# MAGIC 2. Las claves primarias y foráneas son constraints informativas de Unity Catalog.
# MAGIC 3. `NOT NULL` y `CHECK` se utilizan donde la regla de negocio es estructural.
# MAGIC 4. Silver conserva el grano operacional; Gold prepara dimensiones, hechos y el mart mensual que consumirá Power BI.
# MAGIC
# MAGIC El mart `gld_dashboard_negocio_mensual` contiene las ventanas, benchmarks,
# MAGIC rankings y flags de elegibilidad del prototipo HTML. Son métricas/features
# MAGIC derivadas materializadas para consumo BI; no constituyen todavía un Feature Store.
# MAGIC
# MAGIC > **Trazabilidad —** implementa la sección «Modelos conceptual y lógico» del Documento Formal de Arquitectura de Datos.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business (
# MAGIC   business_id STRING NOT NULL COMMENT 'Identificador natural único del negocio en Yelp',
# MAGIC   nombre STRING NOT NULL COMMENT 'Nombre comercial normalizado del negocio',
# MAGIC   direccion STRING COMMENT 'Dirección postal normalizada del negocio',
# MAGIC   ciudad STRING COMMENT 'Ciudad normalizada sin espacios extra',
# MAGIC   estado STRING COMMENT 'Código de estado o provincia normalizado',
# MAGIC   codigo_postal STRING COMMENT 'Código postal del negocio',
# MAGIC   latitud DOUBLE COMMENT 'Latitud geográfica en grados decimales',
# MAGIC   longitud DOUBLE COMMENT 'Longitud geográfica en grados decimales',
# MAGIC   estrellas DOUBLE COMMENT 'Rating promedio publicado por el negocio, entre 1 y 5',
# MAGIC   n_resenas INT COMMENT 'Cantidad acumulada de reseñas publicada por el negocio',
# MAGIC   esta_abierto BOOLEAN COMMENT 'Indica si el negocio está operativo según el origen',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de procesamiento de la fila',
# MAGIC   CONSTRAINT pk_slv_business PRIMARY KEY (business_id),
# MAGIC   CONSTRAINT chk_slv_business_stars CHECK (estrellas BETWEEN 1.0 AND 5.0),
# MAGIC   CONSTRAINT chk_slv_business_lat CHECK (latitud IS NULL OR latitud BETWEEN -90 AND 90),
# MAGIC   CONSTRAINT chk_slv_business_lon CHECK (longitud IS NULL OR longitud BETWEEN -180 AND 180)
# MAGIC ) COMMENT 'Entidad conformada de negocios locales. Grano: un registro por negocio.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_category (
# MAGIC   categoria_key BIGINT NOT NULL COMMENT 'Clave sustituta de la categoría normalizada',
# MAGIC   categoria STRING NOT NULL COMMENT 'Etiqueta de categoría proveniente de Yelp, sin jerarquía inventada',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de procesamiento de la fila',
# MAGIC   CONSTRAINT pk_slv_category PRIMARY KEY (categoria_key),
# MAGIC   CONSTRAINT uq_slv_category UNIQUE (categoria)
# MAGIC ) COMMENT 'Categorías planas normalizadas desde business.categories. Grano: una fila por categoría.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business_category (
# MAGIC   business_id STRING NOT NULL COMMENT 'Identificador del negocio',
# MAGIC   categoria_key BIGINT NOT NULL COMMENT 'Identificador de la categoría normalizada',
# MAGIC   categoria STRING NOT NULL COMMENT 'Etiqueta de categoría asociada al negocio',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de procesamiento de la relación',
# MAGIC   CONSTRAINT pk_slv_business_category PRIMARY KEY (business_id, categoria_key),
# MAGIC   CONSTRAINT fk_slv_business_category_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id),
# MAGIC   CONSTRAINT fk_slv_business_category_category FOREIGN KEY (categoria_key) REFERENCES workspace.yelp_silver.slv_category (categoria_key)
# MAGIC ) COMMENT 'Relación N:M entre negocios y categorías. Grano: una fila por negocio y categoría.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business_hours (
# MAGIC   business_id STRING NOT NULL COMMENT 'Identificador del negocio',
# MAGIC   dia_semana INT NOT NULL COMMENT 'Día de la semana según el origen',
# MAGIC   hora_apertura STRING COMMENT 'Hora de apertura publicada',
# MAGIC   hora_cierre STRING COMMENT 'Hora de cierre publicada',
# MAGIC   es_24_horas BOOLEAN COMMENT 'Indica horario 00:00 a 00:00 interpretado como 24 horas',
# MAGIC   cruza_medianoche BOOLEAN COMMENT 'Indica si la hora de cierre es menor que la apertura',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de procesamiento de la fila',
# MAGIC   CONSTRAINT pk_slv_business_hours PRIMARY KEY (business_id, dia_semana),
# MAGIC   CONSTRAINT fk_slv_business_hours_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id)
# MAGIC ) COMMENT 'Horarios actuales del negocio. Se conservan como contexto y no como verdad histórica de los check-ins.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review_quarantine (
# MAGIC   review_id STRING COMMENT 'Identificador de la reseña rechazada o duplicada',
# MAGIC   user_id STRING COMMENT 'Identificador del usuario de la reseña',
# MAGIC   business_id STRING COMMENT 'Identificador del negocio de la reseña',
# MAGIC   estrellas INT COMMENT 'Calificación recibida antes de validar',
# MAGIC   fecha_resena TIMESTAMP COMMENT 'Fecha de la reseña antes de validar',
# MAGIC   motivo_cuarentena STRING NOT NULL COMMENT 'Motivo por el cual la fila no ingresa a slv_review',
# MAGIC   fecha_cuarentena TIMESTAMP COMMENT 'Timestamp en que la fila fue enviada a cuarentena',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de ingesta original'
# MAGIC ) COMMENT 'Cuarentena de reseñas inválidas o excedentes de duplicidad; no se eliminan silenciosamente.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_user (
# MAGIC   user_id STRING NOT NULL COMMENT 'Identificador natural único del usuario',
# MAGIC   nombre STRING COMMENT 'Nombre público del usuario; dato personal indirecto',
# MAGIC   fecha_alta DATE COMMENT 'Fecha desde la que el usuario pertenece a Yelp',
# MAGIC   n_resenas INT COMMENT 'Cantidad acumulada de reseñas del usuario',
# MAGIC   votos_utiles INT COMMENT 'Cantidad acumulada de votos útiles recibidos',
# MAGIC   votos_divertidos INT COMMENT 'Cantidad acumulada de votos divertidos recibidos',
# MAGIC   votos_geniales INT COMMENT 'Cantidad acumulada de votos geniales recibidos',
# MAGIC   n_fans INT COMMENT 'Cantidad de seguidores del usuario',
# MAGIC   estrellas_promedio DOUBLE COMMENT 'Rating promedio otorgado por el usuario',
# MAGIC   elite ARRAY<STRING> COMMENT 'Años de reconocimiento Elite del usuario',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de procesamiento de la fila',
# MAGIC   CONSTRAINT pk_slv_user PRIMARY KEY (user_id),
# MAGIC   CONSTRAINT chk_slv_user_stars CHECK (estrellas_promedio IS NULL OR estrellas_promedio BETWEEN 1.0 AND 5.0)
# MAGIC ) COMMENT 'Usuarios conformados para análisis de reseñas. Grano: un registro por usuario.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review (
# MAGIC   review_id STRING NOT NULL COMMENT 'Identificador único de la reseña',
# MAGIC   user_id STRING NOT NULL COMMENT 'Identificador del usuario autor de la reseña',
# MAGIC   business_id STRING NOT NULL COMMENT 'Identificador del negocio reseñado',
# MAGIC   estrellas INT NOT NULL COMMENT 'Calificación otorgada en escala de 1 a 5',
# MAGIC   fecha_resena TIMESTAMP NOT NULL COMMENT 'Fecha y hora de publicación de la reseña',
# MAGIC   texto STRING COMMENT 'Texto libre de la reseña; contenido potencialmente sensible',
# MAGIC   votos_utiles INT COMMENT 'Votos útiles recibidos por la reseña',
# MAGIC   votos_divertidos INT COMMENT 'Votos divertidos recibidos por la reseña',
# MAGIC   votos_geniales INT COMMENT 'Votos geniales recibidos por la reseña',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de procesamiento de la fila',
# MAGIC   CONSTRAINT pk_slv_review PRIMARY KEY (review_id),
# MAGIC   CONSTRAINT fk_slv_review_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id),
# MAGIC   CONSTRAINT fk_slv_review_user FOREIGN KEY (user_id) REFERENCES workspace.yelp_silver.slv_user (user_id),
# MAGIC   CONSTRAINT chk_slv_review_stars CHECK (estrellas BETWEEN 1 AND 5)
# MAGIC ) COMMENT 'Reseñas conformadas. Grano: una fila por reseña válida o enviada a cuarentena.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_checkin (
# MAGIC   checkin_event_id STRING NOT NULL COMMENT 'Identificador técnico del evento de check-in',
# MAGIC   business_id STRING NOT NULL COMMENT 'Identificador del negocio donde ocurre el check-in',
# MAGIC   fecha_hora_checkin TIMESTAMP NOT NULL COMMENT 'Fecha y hora del check-in explotado desde la cadena original',
# MAGIC   fecha_key INT NOT NULL COMMENT 'Clave de fecha en formato YYYYMMDD',
# MAGIC   hora INT COMMENT 'Hora del evento entre 0 y 23',
# MAGIC   franja_horaria STRING COMMENT 'Franja normalizada: MADRUGADA, MANANA, TARDE o NOCHE',
# MAGIC   dia_semana STRING COMMENT 'Nombre normalizado del día de la semana',
# MAGIC   es_fin_semana BOOLEAN COMMENT 'Indica si el evento ocurrió sábado o domingo',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de procesamiento de la fila',
# MAGIC   CONSTRAINT pk_slv_checkin PRIMARY KEY (checkin_event_id),
# MAGIC   CONSTRAINT fk_slv_checkin_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id),
# MAGIC   CONSTRAINT chk_slv_checkin_hour CHECK (hora IS NULL OR hora BETWEEN 0 AND 23)
# MAGIC ) COMMENT 'Check-ins explotados desde la cadena original. Grano: una fila por evento individual.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.dim_negocio (
# MAGIC   business_key BIGINT NOT NULL COMMENT 'Clave sustituta del negocio para el modelo dimensional',
# MAGIC   business_id STRING NOT NULL COMMENT 'Identificador natural del negocio en Silver',
# MAGIC   nombre STRING NOT NULL COMMENT 'Nombre comercial del negocio',
# MAGIC   direccion STRING COMMENT 'Dirección postal del negocio',
# MAGIC   ciudad STRING COMMENT 'Ciudad del negocio',
# MAGIC   estado STRING COMMENT 'Estado o provincia del negocio',
# MAGIC   codigo_postal STRING COMMENT 'Código postal del negocio',
# MAGIC   latitud DOUBLE COMMENT 'Latitud geográfica',
# MAGIC   longitud DOUBLE COMMENT 'Longitud geográfica',
# MAGIC   esta_abierto BOOLEAN COMMENT 'Estado operativo del negocio',
# MAGIC   has_category BOOLEAN COMMENT 'Indica si el negocio tiene al menos una categoría válida',
# MAGIC   has_postal_code BOOLEAN COMMENT 'Indica si el negocio tiene código postal válido',
# MAGIC   eligible_geo BOOLEAN COMMENT 'Indica si el negocio puede participar en benchmark geográfico',
# MAGIC   estrellas_snapshot DOUBLE COMMENT 'Rating del negocio al momento de la carga',
# MAGIC   n_resenas_snapshot INT COMMENT 'Cantidad acumulada de reseñas al momento de la carga',
# MAGIC   fecha_inicio DATE COMMENT 'Inicio de vigencia del registro dimensional',
# MAGIC   fecha_fin DATE COMMENT 'Fin de vigencia del registro dimensional',
# MAGIC   es_actual BOOLEAN COMMENT 'Indica si es la versión vigente del negocio',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de carga de la dimensión',
# MAGIC   CONSTRAINT pk_dim_negocio PRIMARY KEY (business_key),
# MAGIC   CONSTRAINT uq_dim_negocio_business_id UNIQUE (business_id)
# MAGIC ) COMMENT 'Dimensión de negocios para relacionar los hechos y los productos de BI.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.dim_usuario (
# MAGIC   user_key BIGINT NOT NULL COMMENT 'Clave sustituta del usuario para el modelo dimensional; -1 representa usuario desconocido',
# MAGIC   user_id STRING NOT NULL COMMENT 'Identificador natural del usuario en Silver; UNKNOWN para la fila -1',
# MAGIC   nombre STRING COMMENT 'Nombre público del usuario, sujeto a clasificación PII',
# MAGIC   es_usuario_desconocido BOOLEAN COMMENT 'Indica si la fila representa al usuario desconocido -1',
# MAGIC   fecha_alta DATE COMMENT 'Fecha de alta del usuario',
# MAGIC   es_actual BOOLEAN COMMENT 'Indica si el registro es la versión vigente',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de carga de la dimensión',
# MAGIC   CONSTRAINT pk_dim_usuario PRIMARY KEY (user_key),
# MAGIC   CONSTRAINT uq_dim_usuario_user_id UNIQUE (user_id)
# MAGIC ) COMMENT 'Dimensión de usuarios para análisis de autores de reseñas y propinas.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.dim_fecha (
# MAGIC   fecha_key INT NOT NULL COMMENT 'Clave de fecha en formato YYYYMMDD',
# MAGIC   fecha DATE NOT NULL COMMENT 'Fecha calendario',
# MAGIC   anio INT COMMENT 'Año calendario',
# MAGIC   mes INT COMMENT 'Número de mes del año',
# MAGIC   anio_mes STRING COMMENT 'Periodo calendario en formato YYYY-MM',
# MAGIC   nombre_mes STRING COMMENT 'Nombre del mes en español',
# MAGIC   trimestre INT COMMENT 'Trimestre calendario',
# MAGIC   dia INT COMMENT 'Día del mes',
# MAGIC   dia_semana INT COMMENT 'Número del día de la semana',
# MAGIC   nombre_dia STRING COMMENT 'Nombre del día en español',
# MAGIC   es_fin_semana BOOLEAN COMMENT 'Indica si la fecha corresponde a sábado o domingo',
# MAGIC   CONSTRAINT pk_dim_fecha PRIMARY KEY (fecha_key)
# MAGIC ) COMMENT 'Dimensión calendario común para reseñas, check-ins y métricas mensuales.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.dim_categoria (
# MAGIC   categoria_key BIGINT NOT NULL COMMENT 'Clave sustituta de la categoría',
# MAGIC   categoria STRING NOT NULL COMMENT 'Nombre normalizado de la categoría del negocio',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de carga de la dimensión',
# MAGIC   CONSTRAINT pk_dim_categoria PRIMARY KEY (categoria_key),
# MAGIC   CONSTRAINT uq_dim_categoria UNIQUE (categoria)
# MAGIC ) COMMENT 'Dimensión de categorías para segmentar benchmarks y métricas de negocio.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.bridge_negocio_categoria (
# MAGIC   business_key BIGINT NOT NULL COMMENT 'Clave del negocio en dim_negocio',
# MAGIC   categoria_key BIGINT NOT NULL COMMENT 'Clave de categoría en dim_categoria',
# MAGIC   es_categoria_principal BOOLEAN COMMENT 'Indica si es la categoría principal seleccionada para el negocio',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de carga de la relación',
# MAGIC   CONSTRAINT pk_bridge_negocio_categoria PRIMARY KEY (business_key, categoria_key),
# MAGIC   CONSTRAINT fk_bridge_business FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.dim_negocio (business_key),
# MAGIC   CONSTRAINT fk_bridge_categoria FOREIGN KEY (categoria_key) REFERENCES workspace.yelp_gold.dim_categoria (categoria_key)
# MAGIC ) COMMENT 'Tabla puente para la relación muchos a muchos entre negocios y categorías.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.fact_resena (
# MAGIC   review_key BIGINT NOT NULL COMMENT 'Clave sustituta de la reseña',
# MAGIC   review_id STRING NOT NULL COMMENT 'Identificador de la reseña en Silver',
# MAGIC   business_key BIGINT NOT NULL COMMENT 'Clave del negocio en dim_negocio',
# MAGIC   user_key BIGINT NOT NULL COMMENT 'Clave del usuario en dim_usuario',
# MAGIC   fecha_key INT NOT NULL COMMENT 'Clave de la fecha de publicación en dim_fecha',
# MAGIC   estrellas INT NOT NULL COMMENT 'Calificación de la reseña entre 1 y 5',
# MAGIC   votos_utiles INT COMMENT 'Votos útiles de la reseña',
# MAGIC   votos_divertidos INT COMMENT 'Votos divertidos de la reseña',
# MAGIC   votos_geniales INT COMMENT 'Votos geniales de la reseña',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de carga del hecho',
# MAGIC   CONSTRAINT pk_fact_resena PRIMARY KEY (review_key),
# MAGIC   CONSTRAINT uq_fact_resena_review_id UNIQUE (review_id),
# MAGIC   CONSTRAINT fk_fact_resena_business FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.dim_negocio (business_key),
# MAGIC   CONSTRAINT fk_fact_resena_user FOREIGN KEY (user_key) REFERENCES workspace.yelp_gold.dim_usuario (user_key),
# MAGIC   CONSTRAINT fk_fact_resena_fecha FOREIGN KEY (fecha_key) REFERENCES workspace.yelp_gold.dim_fecha (fecha_key),
# MAGIC   CONSTRAINT chk_fact_resena_stars CHECK (estrellas BETWEEN 1 AND 5)
# MAGIC ) COMMENT 'Hecho de reseñas. Grano: una fila por reseña publicada.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.fact_checkin_temporal (
# MAGIC   business_key BIGINT NOT NULL COMMENT 'Clave del negocio en dim_negocio',
# MAGIC   fecha_key INT NOT NULL COMMENT 'Clave de fecha en dim_fecha',
# MAGIC   franja_horaria STRING NOT NULL COMMENT 'Franja: madrugada, mañana, tarde o noche',
# MAGIC   cantidad_checkins BIGINT NOT NULL COMMENT 'Cantidad de eventos de check-in consolidados',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de carga del hecho',
# MAGIC   CONSTRAINT pk_fact_checkin_temporal PRIMARY KEY (business_key, fecha_key, franja_horaria),
# MAGIC   CONSTRAINT fk_fact_checkin_temporal_business FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.dim_negocio (business_key),
# MAGIC   CONSTRAINT fk_fact_checkin_temporal_fecha FOREIGN KEY (fecha_key) REFERENCES workspace.yelp_gold.dim_fecha (fecha_key),
# MAGIC   CONSTRAINT chk_fact_checkin_temporal_nonnegative CHECK (cantidad_checkins >= 0)
# MAGIC ) COMMENT 'Hecho temporal agregado. Grano: negocio + fecha + franja. Conserva la cantidad de check-ins.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_actividad_negocio_temporal (
# MAGIC   business_key BIGINT NOT NULL COMMENT 'Clave del negocio en dim_negocio',
# MAGIC   fecha_key INT NOT NULL COMMENT 'Clave de fecha en dim_fecha',
# MAGIC   franja_horaria STRING NOT NULL COMMENT 'Franja temporal del check-in',
# MAGIC   cantidad_checkins BIGINT NOT NULL COMMENT 'Cantidad de check-ins agregados para negocio, fecha y franja',
# MAGIC   dia_semana STRING COMMENT 'Nombre del día de la actividad',
# MAGIC   es_fin_semana BOOLEAN COMMENT 'Indica si la fecha corresponde al fin de semana',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de carga del producto',
# MAGIC   CONSTRAINT pk_gld_actividad_temporal PRIMARY KEY (business_key, fecha_key, franja_horaria),
# MAGIC   CONSTRAINT fk_gld_actividad_business FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.dim_negocio (business_key),
# MAGIC   CONSTRAINT fk_gld_actividad_fecha FOREIGN KEY (fecha_key) REFERENCES workspace.yelp_gold.dim_fecha (fecha_key),
# MAGIC   CONSTRAINT chk_gld_actividad_nonnegative CHECK (cantidad_checkins >= 0)
# MAGIC ) COMMENT 'Producto Gold temporal para Power BI. Grano: negocio + fecha + franja horaria.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dashboard_negocio_mensual (
# MAGIC   business_key BIGINT NOT NULL COMMENT 'Clave del negocio seleccionado en el dashboard',
# MAGIC   anio_mes STRING NOT NULL COMMENT 'Periodo de análisis en formato YYYY-MM',
# MAGIC   categoria_key BIGINT COMMENT 'Categoría usada para formar el grupo comparable',
# MAGIC   benchmark_level INT NOT NULL COMMENT 'Nivel del benchmark: 1 categoría+postal; 2 categoría+ciudad',
# MAGIC   benchmark_type STRING COMMENT 'Tipo de benchmark: CATEGORY_POSTAL o CATEGORY_CITY',
# MAGIC   benchmark_categoria STRING COMMENT 'Categoría del grupo comparable',
# MAGIC   benchmark_postal_code STRING COMMENT 'Código postal del grupo comparable cuando aplica',
# MAGIC   benchmark_ciudad STRING COMMENT 'Ciudad del grupo comparable utilizada por el fallback',
# MAGIC   benchmark_n_business BIGINT COMMENT 'Cantidad de negocios elegibles del grupo comparable; mínimo recomendado 10',
# MAGIC   benchmark_fallback BOOLEAN COMMENT 'Indica si se utilizó fallback de categoría+postal a categoría+ciudad',
# MAGIC   n_reviews_mes BIGINT COMMENT 'Cantidad de reseñas del negocio en el mes',
# MAGIC   rating_mes DOUBLE COMMENT 'Rating promedio del negocio en el mes',
# MAGIC   n_reviewers_mes BIGINT COMMENT 'Cantidad de usuarios únicos que reseñaron en el mes',
# MAGIC   n_checkins_mes BIGINT COMMENT 'Cantidad de check-ins del negocio en el mes',
# MAGIC   reviews_positivas BIGINT COMMENT 'Cantidad de reseñas con estrellas 4 o 5',
# MAGIC   reviews_neutrales BIGINT COMMENT 'Cantidad de reseñas con 3 estrellas',
# MAGIC   reviews_negativas BIGINT COMMENT 'Cantidad de reseñas con 1 o 2 estrellas',
# MAGIC   pct_positivas DOUBLE COMMENT 'Porcentaje de reseñas positivas del mes',
# MAGIC   pct_neutrales DOUBLE COMMENT 'Porcentaje de reseñas neutrales del mes',
# MAGIC   pct_negativas DOUBLE COMMENT 'Porcentaje de reseñas negativas del mes',
# MAGIC   reviews_3m BIGINT COMMENT 'Reseñas acumuladas en la ventana móvil de 3 meses',
# MAGIC   reviews_6m BIGINT COMMENT 'Reseñas acumuladas en la ventana móvil de 6 meses',
# MAGIC   reviews_12m BIGINT COMMENT 'Reseñas acumuladas en la ventana móvil de 12 meses',
# MAGIC   rating_3m DOUBLE COMMENT 'Rating promedio móvil de 3 meses',
# MAGIC   rating_6m DOUBLE COMMENT 'Rating promedio móvil de 6 meses',
# MAGIC   rating_12m DOUBLE COMMENT 'Rating promedio móvil de 12 meses',
# MAGIC   checkins_3m BIGINT COMMENT 'Check-ins acumulados en la ventana móvil de 3 meses',
# MAGIC   checkins_6m BIGINT COMMENT 'Check-ins acumulados en la ventana móvil de 6 meses',
# MAGIC   checkins_12m BIGINT COMMENT 'Check-ins acumulados en la ventana móvil de 12 meses',
# MAGIC   reviews_ytd BIGINT COMMENT 'Reseñas acumuladas desde el inicio del año',
# MAGIC   checkins_ytd BIGINT COMMENT 'Check-ins acumulados desde el inicio del año',
# MAGIC   flag_anio_completo BOOLEAN COMMENT 'Indica si el año del periodo tiene cobertura completa',
# MAGIC   benchmark_rating DOUBLE COMMENT 'Rating promedio del grupo comparable',
# MAGIC   benchmark_reviews DOUBLE COMMENT 'Promedio mensual de reseñas del grupo comparable',
# MAGIC   benchmark_checkins DOUBLE COMMENT 'Promedio mensual de check-ins del grupo comparable',
# MAGIC   delta_rating_vs_benchmark DOUBLE COMMENT 'Diferencia del rating del negocio frente al benchmark',
# MAGIC   delta_reviews_vs_benchmark DOUBLE COMMENT 'Diferencia de reseñas frente al benchmark',
# MAGIC   delta_checkins_vs_benchmark DOUBLE COMMENT 'Diferencia de check-ins frente al benchmark',
# MAGIC   ranking_rating INT COMMENT 'Posición del negocio por rating dentro del grupo comparable',
# MAGIC   meses_historia_reviews INT COMMENT 'Meses históricos disponibles de reseñas',
# MAGIC   meses_historia_checkins INT COMMENT 'Meses históricos disponibles de check-ins',
# MAGIC   eligible_rating BOOLEAN COMMENT 'Indica si el rating mensual supera el umbral de elegibilidad',
# MAGIC   eligible_rating_12m BOOLEAN COMMENT 'Indica si existe cobertura suficiente de rating a 12 meses',
# MAGIC   eligible_checkin BOOLEAN COMMENT 'Indica si existe cobertura suficiente de check-ins',
# MAGIC   eligible_benchmark BOOLEAN COMMENT 'Indica si el benchmark tiene cobertura suficiente',
# MAGIC   umbral_reviews_satisfaccion INT COMMENT 'Mínimo de 10 reseñas requerido para mostrar satisfacción como representativa',
# MAGIC   flag_satisfaccion_valida BOOLEAN COMMENT 'Indica si existen al menos 10 reseñas y la distribución de satisfacción es válida',
# MAGIC   fecha_carga TIMESTAMP COMMENT 'Timestamp de carga del producto Gold',
# MAGIC   CONSTRAINT pk_gld_dashboard_negocio_mensual PRIMARY KEY (business_key, anio_mes, benchmark_level),
# MAGIC   CONSTRAINT fk_gld_dashboard_business FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.dim_negocio (business_key),
# MAGIC   CONSTRAINT fk_gld_dashboard_categoria FOREIGN KEY (categoria_key) REFERENCES workspace.yelp_gold.dim_categoria (categoria_key),
# MAGIC   CONSTRAINT chk_gld_dashboard_rating CHECK (rating_mes IS NULL OR rating_mes BETWEEN 1.0 AND 5.0),
# MAGIC   CONSTRAINT chk_gld_dashboard_benchmark CHECK (benchmark_n_business IS NULL OR benchmark_n_business >= 10),
# MAGIC   CONSTRAINT chk_gld_dashboard_pct CHECK ((pct_positivas IS NULL OR pct_positivas BETWEEN 0 AND 100) AND (pct_neutrales IS NULL OR pct_neutrales BETWEEN 0 AND 100) AND (pct_negativas IS NULL OR pct_negativas BETWEEN 0 AND 100))
# MAGIC ) COMMENT 'Mart Gold mensual que concentra KPIs, benchmarks, ventanas y elegibilidad para Power BI.';

# COMMAND ----------

# DBTITLE 1: Validación automática del modelo físico
tablas_silver = {r.tableName for r in spark.sql("SHOW TABLES IN workspace.yelp_silver").collect()}
tablas_gold = {r.tableName for r in spark.sql("SHOW TABLES IN workspace.yelp_gold").collect()}

silver_requeridas = {"slv_business", "slv_business_hours", "slv_category", "slv_business_category", "slv_review", "slv_review_quarantine", "slv_user", "slv_checkin"}
gold_requeridas = {"dim_negocio", "dim_usuario", "dim_fecha", "dim_categoria", "bridge_negocio_categoria", "fact_resena", "fact_checkin_temporal", "gld_actividad_negocio_temporal", "gld_dashboard_negocio_mensual"}
assert silver_requeridas.issubset(tablas_silver), f"Faltan tablas Silver: {sorted(silver_requeridas - tablas_silver)}"
assert gold_requeridas.issubset(tablas_gold), f"Faltan tablas Gold: {sorted(gold_requeridas - tablas_gold)}"

sin_comment = spark.sql("""
  SELECT table_schema, table_name, column_name
  FROM workspace.information_schema.columns
  WHERE table_schema IN ('yelp_silver','yelp_gold')
    AND (comment IS NULL OR trim(comment) = '')
""").collect()
assert not sin_comment, f"❌ Columnas sin COMMENT: {[(r.table_schema, r.table_name, r.column_name) for r in sin_comment]}"
print("✔ Modelo físico validado: Silver y Gold creados; todas las columnas documentadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E3)
# MAGIC - [x] Silver conformado con business, hours, category, bridge, review, quarantine, user y check-in.
# MAGIC - [x] Gold dimensional con dimensiones, bridge, hechos y actividad temporal agregada.
# MAGIC - [x] Mart mensual Gold alineado con el sustento EDA, el prototipo HTML y Power BI.
# MAGIC - [x] 100 % de columnas con `COMMENT`.
# MAGIC - [x] PK/FK informativas y `CHECK` donde aplica.
# MAGIC - [ ] Captura del diagrama ER y lineage en Catalog Explorer.
