#!/bin/bash

# Descarga el archivo de footprints de Microsoft desde Google Drive

DRIVE_URL="${MICROSOFT_DRIVE_URL}"
OUTPUT_FILE="${MICROSOFT_INPUT_FILE:-samples/sample_microsoft.geojsonl}"

if [ -z "$DRIVE_URL" ]; then
    echo "⚠ MICROSOFT_DRIVE_URL no definida. Omitiendo descarga de footprints Microsoft."
    exit 0
fi

echo "▶ Descargando Microsoft footprints desde Google Drive..."
echo "  URL: $DRIVE_URL"
echo "  Destino: $OUTPUT_FILE"

# Detectar si es Google Drive y extraer file ID
if [[ "$DRIVE_URL" =~ drive\.google\.com ]]; then
    # Extraer ID de diferentes formatos de URL de Google Drive
    if [[ "$DRIVE_URL" =~ /d/([a-zA-Z0-9_-]+) ]]; then
        FILE_ID="${BASH_REMATCH[1]}"
    elif [[ "$DRIVE_URL" =~ id=([a-zA-Z0-9_-]+) ]]; then
        FILE_ID="${BASH_REMATCH[1]}"
    else
        echo "✗ ERROR: No se pudo extraer el file ID de la URL de Google Drive"
        exit 1
    fi
    
    echo "  File ID: $FILE_ID"
    
    # Usar gdown (instalado en el Dockerfile)
    python3 -m gdown "https://drive.google.com/uc?id=${FILE_ID}" -O "$OUTPUT_FILE" --fuzzy
    
    if [ $? -eq 0 ]; then
        echo "✓ Microsoft footprints descargados exitosamente"
        ls -lh "$OUTPUT_FILE"
        # Convertir a GeoJSON (FeatureCollection) para compatibilidad con loaders que esperan un único objeto
        CONVERTED="$(dirname "$OUTPUT_FILE")/sample_microsoft.geojson"
        echo "▶ Convirtiendo $OUTPUT_FILE -> $CONVERTED"
        python3 /app/scripts/convert_geojsonl_to_geojson.py "/app/$OUTPUT_FILE" "/app/${CONVERTED}"
        if [ $? -eq 0 ]; then
            echo "✓ Conversión completada: $CONVERTED"
        else
            echo "⚠ ERROR: Falló la conversión a GeoJSON" >&2
            exit 1
        fi
    else
        echo "✗ ERROR: Fallo la descarga con gdown"
        exit 1
    fi
else
    # Para otros hosts, intentar curl o wget
    echo "  Intentando con curl..."
    curl -L -o "$OUTPUT_FILE" "$DRIVE_URL"
    
    if [ $? -ne 0 ]; then
        echo "  curl falló. Intentando con wget..."
        wget -O "$OUTPUT_FILE" "$DRIVE_URL"
        
        if [ $? -ne 0 ]; then
            echo "✗ ERROR: No se pudo descargar el archivo"
            exit 1
        fi
    fi
    
    echo "✓ Archivo descargado exitosamente"
    ls -lh "$OUTPUT_FILE"
fi
