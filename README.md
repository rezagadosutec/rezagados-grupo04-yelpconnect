# Capstone Project — Plataforma de Datos YelpConnect
### Programa de Arquitectura de Datos · UTEC

Este repositorio es el **esqueleto base** que cada grupo usará para desplegar sus
entregables del Capstone en **Databricks Free Edition**. La estructura sigue las
8 líneas de trabajo del caso YelpConnect y se va completando a medida que cierra
cada módulo del programa.

---

## 1. Setup inicial del grupo (Semana 0 / Onboarding)

1. **Un integrante crea la cuenta del grupo** en [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
   (correo del grupo, no personal). Free Edition permite **un solo workspace y un
   metastore por cuenta**, así que la cuenta es del equipo, no de una persona.
2. Invitar a los 5 integrantes como usuarios del workspace
   (`Settings → Identity and access → Users`).
3. Crear un repositorio **fork/clon de este esqueleto** en GitHub
   (nombre sugerido: `capstone-grupoNN-yelpconnect`).
4. En Databricks: `Workspace → Create → Git folder` y conectar el repo del grupo.
5. Ejecutar `00_setup/00_configurar_lakehouse` — crea schemas, volume y tablas de control.
6. Preparar la muestra de datos con `datos/preparar_muestra_yelp.py` (se corre
   **en tu laptop**, no en Databricks) y subir los archivos resultantes al Volume.
7. Ejecutar `00_setup/01_verificar_datos_raw` para validar que todo está en su lugar.

## 2. Convenciones del Lakehouse (obligatorias — se evalúan)

| Elemento | Convención |
|---|---|
| Catálogo | `workspace` (el default de Free Edition) |
| Schemas | `yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov` |
| Volume de datos crudos | `/Volumes/workspace/yelp_bronze/raw/` |
| Tablas bronze | `brz_<entidad>` (ej. `brz_business`) |
| Tablas silver | `slv_<entidad>` |
| Tablas gold | `gld_<producto_de_datos>` (ej. `gld_kpi_resenas_negocio`) |
| Vistas | `vw_<nombre>` |
| Idioma de comentarios/metadata | Español (o inglés, pero consistente) |
| Ramas Git | `main` protegida; trabajar en `feature/E<k>-<tema>` y merge por PR |

## 3. Mapa entregable ↔ carpeta ↔ módulo

| Entregable Capstone | Carpeta | Módulo que lo cierra |
|---|---|---|
| E1. Visión de Arquitectura (SoAW) | `E1_vision_arquitectura` | M1 Fundamentos y Estrategia |
| E2. Arquitectura ABB/SBB (Arq. de Referencia) | `E2_arquitectura_abb_sbb` | M5 Arquitectura Cloud (base en M1) |
| E3. Modelo de datos (conceptual/lógico/físico) | `E3_modelo_datos` | M4 Modelado Avanzado |
| E4. Diccionario y Glosario de Negocio | `E4_diccionario_glosario` | M3 MDM y Metadata |
| E5. Reglas de Calidad de Datos | `E5_calidad_datos` | M2/M3 (implementación tras M6) |
| E6. Estrategia y controles de Gobierno | `E6_gobierno_datos` | M2 Seguridad y Gobierno |
| E7. Pipeline de ingesta y habilitación | `E7_pipeline_lakehouse` | M6 Big Data + M7 Integración |
| E8. Dashboard BI + Genie | `E8_dashboard_bi` | M9 BI y Visualización |
| EX. Bonus: IA sobre reseñas | `EX_bonus_ia` | M8 IA y No Estructurados |
| Operación (job, monitoreo, FinOps) | `E7.../04_job_orquestacion` | M10 DataOps/MLOps/FinOps |

## 3.1 Artefactos institucionales de gobierno (E4)

El glosario y el diccionario **no se inventan en formato libre**: se completan en las
plantillas oficiales del bloque de Seguridad, Gobernanza y Compliance, que están en
`E4_diccionario_glosario/plantillas/`:

| Plantilla | Hojas | Se carga en |
|---|---|---|
| `Glosario_de_terminos_de_negocio_YelpConnect_PLANTILLA.xlsx` | Definiciones · Glosario · Guía Capstone | `workspace.yelp_gov.glosario_negocio` |
| `Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx` | Definición · Diccionario · Diccionario resumido · Guía Capstone | `workspace.yelp_gov.diccionario_datos` |

Las tablas Delta tienen **exactamente las mismas columnas que las plantillas** (12 y 25
respectivamente), así que el Excel no se traduce: se carga. Flujo:

1. El grupo completa el Excel (interfaz de negocio, se discute con las "áreas" del caso).
2. Sube el archivo a `/Volumes/workspace/yelp_gov/artefactos/` sin cambiarle el nombre.
3. Ejecuta los notebooks `E4.../01_glosario_negocio` y `E4.../02_diccionario_datos`.
4. Los validadores comprueban cobertura, ownership, clasificación y que toda regla citada
   como `DQ-nnn` exista realmente en `yelp_gov.dq_reglas`.

El diccionario cierra el círculo: su columna de clasificación se aplica como **tag real**
en Unity Catalog y alimenta los permisos de E6.

## 4. Reglas de juego en Free Edition

- Solo hay **cómputo serverless** con **cuota diaria**. Si la agotan, el cómputo se
  apaga hasta el día siguiente (los datos NO se pierden). Moraleja FinOps: trabajen
  con la **muestra**, no con el dataset completo, y eviten `display()` de millones de filas.
- El dataset oficial es el **Yelp Open Dataset** muestreado a UNA ciudad
  (el script de muestreo lo hace por ustedes). Objetivo: < 300 MB en crudo.
- Todo entregable debe ser **reproducible**: si el profesor clona el repo y ejecuta
  los notebooks en orden, obtiene el mismo resultado.

## 5. Definition of Done global

Un entregable está terminado cuando: (a) el notebook corre de inicio a fin sin errores,
(b) las tablas/objetos existen en el schema correcto con comentarios y tags,
(c) el markdown del notebook documenta decisiones y supuestos, y
(d) está en `main` vía Pull Request revisado por al menos otro integrante.
