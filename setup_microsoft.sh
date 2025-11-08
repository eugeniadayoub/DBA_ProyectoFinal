#!/bin/bash
set -e

echo "--- Cargando Microsoft Building Footprints ---"

# Asegurarse que MongoDB esté corriendo
docker-compose up -d mongo-upme

# Esperar a que MongoDB esté listo
echo "Esperando a que MongoDB esté listo..."
sleep 3

# Ejecutar el script de carga de Microsoft
docker-compose run --rm etl-loader python3 cargar_microsoft_footprints.py

echo "--- ¡Carga de Microsoft completada! ---"
