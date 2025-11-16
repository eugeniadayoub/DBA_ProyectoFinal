#!/usr/bin/env bash
set -euo pipefail

# Descarga el ZIP MGN desde OneDrive u otra URL especificada en la variable de entorno ONEDRIVE_URL
# Uso: export ONEDRIVE_URL="<link compartido>"; ./download_mgn.sh

OUT="/app/MGN2024_00_COLOMBIA.zip"

if [ -f "$OUT" ]; then
  echo "Download skipped: $OUT ya existe."
  exit 0
fi

if [ -n "${ONEDRIVE_URL:-}" ]; then
  URL="$ONEDRIVE_URL"
elif [ -f "/app/onedrive_url.txt" ]; then
  URL=$(cat /app/onedrive_url.txt)
else
  echo "ERROR: no se encontró URL de descarga. Define la variable de entorno ONEDRIVE_URL o crea /app/onedrive_url.txt con la URL compartida de OneDrive."
  exit 2
fi

echo "Descargando MGN ZIP desde: $URL"
echo "-> destino: $OUT"

# Detectar si es Google Drive y usar gdown
if [[ "$URL" == *"drive.google.com"* ]]; then
  echo "Detectado enlace de Google Drive; usando gdown..."
  if command -v python3 >/dev/null 2>&1; then
    # Extraer file ID de la URL si es necesario
    if [[ "$URL" =~ id=([^&]+) ]]; then
      FILE_ID="${BASH_REMATCH[1]}"
    elif [[ "$URL" =~ /d/([^/]+) ]]; then
      FILE_ID="${BASH_REMATCH[1]}"
    else
      echo "ERROR: no se pudo extraer el file ID de la URL de Google Drive."
      exit 4
    fi
    python3 -m gdown "https://drive.google.com/uc?id=${FILE_ID}" -O "$OUT" --fuzzy
  else
    echo "ERROR: Python3 no disponible para ejecutar gdown."
    exit 3
  fi
else
  # Descarga normal para otros hosts
  if command -v curl >/dev/null 2>&1; then
    curl -L --fail -o "$OUT" "$URL"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$OUT" "$URL"
  else
    echo "ERROR: ni curl ni wget están disponibles en la imagen. Instala curl o wget en el Dockerfile."
    exit 3
  fi
fi

if [ -f "$OUT" ]; then
  echo "Descarga finalizada: $(stat -c%s "$OUT") bytes descargados."
else
  echo "ERROR: la descarga falló."
  exit 4
fi

exit 0
