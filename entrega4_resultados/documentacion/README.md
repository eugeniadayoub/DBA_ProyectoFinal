# Proyecto: Municipios PDET - MongoDB

Este proyecto carga municipios PDET y datos de building footprints (Google y Microsoft) desde archivos GeoJSON a una base de datos MongoDB usando Python y Docker.

## Estructura de carpetas

```
DBA_ProyectoFinal/
├── setup.sh                    <-- Script inicial: carga municipios PDET
├── setup_google.sh             <-- Script: carga Google footprints
├── setup_microsoft.sh          <-- Script: carga Microsoft footprints
├── setup_eda.sh                <-- Script: ejecuta análisis exploratorio (EDA)
├── docker-compose.yml          <-- Define los servicios de DB y Python
└── data/
    ├── Dockerfile              <-- Define el entorno de Python
    ├── cargar_municipios.py    <-- Carga municipios PDET
    ├── cargar_google_footprints.py      <-- Carga Google Open Buildings
    ├── cargar_microsoft_footprints.py   <-- Carga Microsoft Building Footprints
    ├── eda_footprints.py       <-- Análisis exploratorio de datos
    ├── municipios_pdet_filtrados.geojson
    └── samples/
        ├── sample_google1.geojson
        └── sample_microsoft.geojson
```

## Requisitos

- Docker (para MongoDB)
- Un entorno de terminal bash (incluido en macOS, Linux, y en Windows a través de Git Bash o WSL).

## Pasos para ejecutar

### 1. Dar permisos de ejecución a los scripts

En la raíz del proyecto (`DBA_ProyectoFinal/`):

```bash
chmod +x setup.sh
chmod +x setup_google.sh
chmod +x setup_microsoft.sh
chmod +x setup_eda.sh
```

### 2. Ejecutar los scripts en orden

#### a) Cargar Municipios PDET (Paso 1)

```bash
./setup.sh
```

#### b) Cargar Google Open Buildings Footprints (Paso 2)

```bash
./setup_google.sh
```

#### c) Cargar Microsoft Building Footprints (Paso 3)

```bash
./setup_microsoft.sh
```

#### d) Ejecutar Análisis Exploratorio de Datos - EDA (Paso 4)

```bash
./setup_eda.sh
```

Este último comando generará un archivo `EDA_Footprints_YYYYMMDD_HHMMSS.txt` con todos los resultados del análisis.

## ¿Qué hace cada script?

### setup.sh
El script `setup.sh` se encarga de todo el proceso inicial:
- Verifica que Docker y Docker Compose estén instalados.
- Construye la imagen de Python definida en `data/Dockerfile` (instalando pymongo dentro de ella).
- Levanta el contenedor de la base de datos `mongo-upme`.
- Una vez la base de datos está lista, levanta el contenedor `etl-loader`, que ejecuta el script `cargar_municipios.py` para poblar la base de datos.
- Muestra los logs del script de carga en tu terminal para que puedas ver el progreso.

Al finalizar, el script `etl-loader` se detendrá, pero la base de datos `mongo-upme` quedará corriendo con todos los datos cargados y lista para usarse.

### setup_google.sh
El script `setup_google.sh`:
- Asegura que MongoDB esté corriendo.
- Ejecuta `cargar_google_footprints.py` que:
  - Lee el archivo `samples/sample_google1.geojson` (200 edificaciones)
  - Carga los datos a la colección `google_footprints`
  - Crea índice espacial 2dsphere en el campo `geometry`
  - Muestra estadísticas de área, confianza y coordenadas

### setup_microsoft.sh
El script `setup_microsoft.sh`:
- Asegura que MongoDB esté corriendo.
- Ejecuta `cargar_microsoft_footprints.py` que:
  - Lee el archivo `samples/sample_microsoft.geojson` (200 edificaciones)
  - Carga los datos a la colección `microsoft_footprints`
  - Crea índice espacial 2dsphere en el campo `geometry`
  - Calcula centroides y muestra estadísticas de complejidad de polígonos

### setup_eda.sh
El script `setup_eda.sh`:
- Asegura que MongoDB esté corriendo.
- Ejecuta `eda_footprints.py` que realiza un análisis exploratorio completo:
  - Resumen general de todas las colecciones
  - Estructura y campos de cada dataset
  - Análisis detallado de Google Open Buildings (área, confianza, distribuciones)
  - Análisis detallado de Microsoft Building Footprints (geometrías, complejidad)
  - Comparación entre ambos datasets
  - Evaluación de calidad de datos
- **Guarda automáticamente** los resultados en un archivo `.txt` con timestamp
- Muestra los resultados en pantalla y los guarda simultáneamente

## Visualizar los datos en MongoDB Compass

Puedes usar [MongoDB Compass](https://www.mongodb.com/products/compass) para explorar los datos cargados:

1. Abre MongoDB Compass.
2. Conéctate usando la URI:
   ```
   mongodb://localhost:27017/
   ```
3. Selecciona la base de datos `proyecto_upme`.
4. Explora las colecciones disponibles:
   - `mgn_municipios_pdet` - 101 municipios PDET
   - `google_footprints` - 200 edificaciones de Google Open Buildings
   - `microsoft_footprints` - 200 edificaciones de Microsoft Building Footprints

Si tienes Docker corriendo y los scripts se ejecutaron correctamente, deberías ver:

**Municipios PDET:**
- `codigo_municipio`, `nombre_municipio`, `departamento`, `geometry`

**Google Footprints:**
- `latitude`, `longitude`, `area_in_meters`, `confidence`, `full_plus_code`, `geometry`, `geometry_wkt`

**Microsoft Footprints:**
- `centroid_latitude`, `centroid_longitude`, `geometry`

## Resultados del EDA

Después de ejecutar `./setup_eda.sh`, encontrarás un archivo con formato:
```
EDA_Footprints.txt
```

Este archivo contiene:
- Estadísticas completas de ambos datasets
- Distribuciones de área y confianza
- Rangos geográficos
- Calidad de datos
- Comparación Google vs Microsoft

Puedes incluir este archivo directamente en tu entrega del proyecto.

## Comandos útiles

```bash
# Ver logs de MongoDB
docker-compose logs mongo-upme

# Detener todos los contenedores
docker-compose down

# Reconstruir imágenes si modificas el Dockerfile
docker-compose build --no-cache

# Ver los archivos EDA generados
ls -lt EDA_Footprints_*.txt | head -5
```

## Estructura de la Base de Datos

**Base de datos:** `proyecto_upme`

**Colecciones:**
1. `mgn_municipios_pdet` - Municipios PDET con geometrías
2. `google_footprints` - Building footprints de Google con metadatos
3. `microsoft_footprints` - Building footprints de Microsoft con polígonos detallados

**Índices espaciales:**
- Todas las colecciones tienen índice 2dsphere en el campo `geometry`
- Índices adicionales en campos relevantes (confidence, area_in_meters, etc.)
