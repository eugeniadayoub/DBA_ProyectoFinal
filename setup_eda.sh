#!/bin/bash
set -e

# Definir nombre del archivo de salida con timestamp
OUTPUT_FILE="EDA_Footprints.txt"

echo "--- Ejecutando Análisis Exploratorio de Datos (EDA) ---"

# Asegurarse que MongoDB esté corriendo
docker-compose up -d mongo-upme

# Esperar a que MongoDB esté listo
echo "Esperando a que MongoDB esté listo..."
sleep 2

# Ejecutar el script de EDA y guardar en archivo
echo "Iniciando análisis..."
echo "Guardando resultados en: $OUTPUT_FILE"
echo ""

# Ejecutar y mostrar en pantalla Y guardar en archivo simultáneamente
docker-compose run --rm etl-loader python3 eda_footprints.py | tee "$OUTPUT_FILE"

echo ""
echo "--- ¡Análisis completado! ---"
echo "✓ Resultados guardados en: $OUTPUT_FILE"
echo "  Puedes revisarlo con: cat $OUTPUT_FILE"
