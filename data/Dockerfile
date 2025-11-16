FROM --platform=linux/amd64 python:3.10-slim

WORKDIR /app

# Instalar GDAL y dependencias geoespaciales
RUN apt-get update && apt-get install -y \
    binutils \
    libproj-dev \
    gdal-bin \
    curl \
    wget \
    ca-certificates \
    dos2unix \
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

# Copiar todo
COPY . .

# Dar permisos a scripts dentro del contenedor
RUN chmod +x /app/*.sh && chmod +x /app/scripts/*.sh

CMD ["bash"]
