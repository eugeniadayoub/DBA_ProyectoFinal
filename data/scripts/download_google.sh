#!/bin/bash
set -e

echo "▶ Descargando Google Building Footprints (4 archivos CSV.GZ)..."

# URLs de Google Drive (convertir view links a download links)
URLS=(
    "https://drive.google.com/uc?export=download&id=1YfbnVlwE855q8sFe4Ttih7akMA7bJreZ"
    "https://drive.google.com/uc?export=download&id=13TOK9-kXNmUVlDLwemBT28J3TAnbimbM"
    "https://drive.google.com/uc?export=download&id=1I_brih7Ov32O5Jj5Cqjl8TPkJy5ikBcl"
    "https://drive.google.com/uc?export=download&id=1BXT4VLF2j-7GEdUnNMR7xmIUCpMlWJb_"
)

# Nombres de destino
DESTINOS=(
    "samples/google_part1.csv.gz"
    "samples/google_part2.csv.gz"
    "samples/google_part3.csv.gz"
    "samples/google_part4.csv.gz"
)

# Descargar cada archivo
for i in "${!URLS[@]}"; do
    URL="${URLS[$i]}"
    DEST="${DESTINOS[$i]}"
    
    if [ -f "$DEST" ]; then
        echo "  ✓ Ya existe: $DEST (omitiendo descarga)"
        continue
    fi
    
    echo "  Descargando archivo $((i+1))/4..."
    echo "    URL: $URL"
    echo "    Destino: $DEST"
    
    # Extraer file ID de la URL
    FILE_ID=$(echo "$URL" | grep -oP 'id=\K[^&]+' || echo "")
    
    if [ -n "$FILE_ID" ]; then
        echo "    File ID: $FILE_ID"
        gdown "https://drive.google.com/uc?id=$FILE_ID" -O "$DEST"
    else
        echo "  ✗ ERROR: No se pudo extraer file ID de $URL"
        exit 1
    fi
    
    if [ -f "$DEST" ]; then
        SIZE=$(stat -f%z "$DEST" 2>/dev/null || stat -c%s "$DEST" 2>/dev/null || echo "?")
        echo "  ✓ Descargado: $DEST ($SIZE bytes)"
    else
        echo "  ✗ ERROR: Falló la descarga de $DEST"
        exit 1
    fi
done

echo "✓ Todos los archivos Google descargados exitosamente"
ls -lh samples/google_part*.csv.gz 2>/dev/null || true
