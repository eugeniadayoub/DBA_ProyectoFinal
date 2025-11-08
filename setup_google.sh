#!/bin/bash
set -e

echo "--- Cargando Google Open Buildings Footprints ---"

# Asegurarse que MongoDB esté corriendo
docker-compose up -d mongo-upme

# Esperar a que MongoDB esté listo
echo "Esperando a que MongoDB esté listo..."
sleep 3

# Ejecutar el script de carga de Google
docker-compose run --rm etl-loader python3 cargar_google_footprints.py

echo "--- ¡Carga de Google completada! ---"
