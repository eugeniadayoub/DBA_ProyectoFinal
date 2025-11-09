# 1. Usar una imagen oficial de Python
FROM python:3.10-slim

# 2. Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Instalar las dependencias de Python
# Agregamos Shapely para manipulación de geometrías (WKT, centroides, áreas)
RUN pip install pymongo shapely

# 4. Copiar TODOS los archivos de la carpeta data al contenedor
# Esto incluye: scripts .py, archivos .geojson, y la carpeta samples/
COPY . .

# 5. El comando que se ejecutará cuando el contenedor inicie
# Este es el comando por defecto, pero puede ser sobrescrito
CMD ["python3", "cargar_municipios.py"]
