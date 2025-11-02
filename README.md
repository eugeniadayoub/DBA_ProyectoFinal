# Proyecto: Municipios PDET - MongoDB

Este proyecto carga municipios PDET desde un archivo GeoJSON a una base de datos MongoDB usando Python.

## Estructura de carpetas

```
DBA_ProyectoFinal/
├── setup.sh             <-- Script de automatización
├── docker-compose.yml   <-- Define los servicios de DB y Python
└── data/
    ├── Dockerfile         <-- Define el entorno de Python
    ├── cargar_municipios.py
    └── municipios_pdet_filtrados.geojson
```

## Requisitos

- Docker (para MongoDB)
- Un entorno de terminal bash (incluido en macOS, Linux, y en Windows a través de Git Bash o WSL).

## Pasos para ejecutar

1. **Dar permisos de ejecución al script**

   En la raíz del proyecto (`DBA_ProyectoFinal/`):
   
   ```powershell
   chmod +x setup.sh
   ```

2. **Ejecutar el script de configuración**

En la raíz del proyecto (`DBA_ProyectoFinal/`):
   
   ```powershell
   ./setup.sh
   ```

## ¿Qué hace el script setup.sh?

El script setup.sh se encarga de todo el proceso:
- Verifica que Docker y Docker Compose estén instalados.
- Construye la imagen de Python definida en data/Dockerfile (instalando pymongo dentro de ella).
- Levanta el contenedor de la base de datos mongo-upme.
- Una vez la base de datos está lista, levanta el contenedor etl-loader, que ejecuta el script cargar_municipios.py para poblar la base de datos.
- Muestra los logs del script de carga en tu terminal para que puedas ver el progreso.
Al finalizar, el script etl-loader se detendrá, pero la base de datos mongo-upme quedará corriendo con todos los datos cargados y lista para usarse.


## Visualizar los datos en MongoDB Compass

Puedes usar [MongoDB Compass](https://www.mongodb.com/products/compass) para explorar los datos cargados:

1. Abre MongoDB Compass.
2. Conéctate usando la URI:
   ```
   mongodb://localhost:27017/
   ```
3. Selecciona la base de datos `proyecto_upme`.
4. Selecciona la colección `mgn_municipios_pdet`.
5. Visualiza, filtra y explora los documentos insertados.

Si tienes Docker corriendo y el script se ejecutó correctamente, deberías ver los municipios con los campos:
`codigo_municipio`, `nombre_municipio`, `departamento`, `geometry`.

