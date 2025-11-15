FROM python:3.10-slim

WORKDIR /app

# Instalar GDAL y dependencias geoespaciales
RUN apt-get update && apt-get install -y \
    binutils \
    libproj-dev \
    gdal-bin \
    curl \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
RUN pip install --no-cache-dir \
    pymongo \
    shapely \
    fiona \
    pyproj \
    tqdm \
    pandas \
    openpyxl \
    gdown

# Copiar TODO el contenido de data/ al contenedor
COPY . .

# Hacer ejecutable el script setup
RUN chmod +x setup.sh
RUN chmod +x run_etl.sh

# Comando por defecto
CMD ["python3", "cargar_municipios.py"]
