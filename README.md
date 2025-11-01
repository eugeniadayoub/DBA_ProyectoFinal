# Proyecto: Municipios PDET - MongoDB

Este proyecto carga municipios PDET desde un archivo GeoJSON a una base de datos MongoDB usando Python.

## Estructura de carpetas

```
DBA_ProyectoFinal/
├── docker-compose.yml
├── data/
│   ├── cargar_municipios.py
│   ├── municipios_pdet_filtrados.geojson
│   └── municipios_pdet_filtrados.qmd
```

## Requisitos

- Docker (para MongoDB)
- Python 3.x
- Paquete pymongo (`pip install pymongo`) o (`py -m pip install pymongo`)

## Pasos para ejecutar

1. **Levantar MongoDB con Docker**

   En la raíz del proyecto (`DBA_ProyectoFinal/`):
   
   ```powershell
   docker-compose up -d
   ```
   Esto inicia MongoDB en el puerto 27017.

2. **Instalar dependencias Python**

   En la carpeta `data/`:
   
   ```powershell
   py -m pip install pymongo
   ```

3. **Ejecutar el script de carga**

   En la carpeta `data/`:
   
   ```powershell
   py cargar_municipios.py
   ```
   El script:
   - Limpia la colección `mgn_municipios_pdet` en la base `proyecto_upme`.
   - Lee el archivo `municipios_pdet_filtrados.geojson`.
   - Inserta los municipios con los campos: `codigo_municipio`, `nombre_municipio`, `departamento`, `geometry`.
   - Crea el índice geoespacial.
   - Muestra un ejemplo de documento insertado.


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

