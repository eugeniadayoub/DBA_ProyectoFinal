#!/usr/bin/env bash
set -euo pipefail

echo "[ETL] Iniciando pipeline ETL corregido para Entrega 4"

# Espera para MongoDB
sleep 10

# ================================================
# PASO 1: Descargar y cargar municipios MGN
# ================================================
echo "[ETL] PASO 1: Cargando municipios MGN (DANE)..."
if [ ! -f /app/MGN2024_00_COLOMBIA.zip ]; then
  echo "[ETL] ZIP MGN no encontrado. Descargando..."
  if [ -x /app/scripts/download_mgn.sh ]; then
    /app/scripts/download_mgn.sh
  else
    sh /app/scripts/download_mgn.sh || true
  fi
fi

python3 /app/cargar_municipios.py

# ================================================
# PASO 2: Crear colección PDET desde Excel
# ================================================
echo "[ETL] PASO 2: Creando colección mgn_municipios_pdet..."
python3 /app/scripts/create_mgn_municipios_pdet.py

# ================================================
# PASO 3: Descargar footprints (sin cargar aún)
# ================================================
echo "[ETL] PASO 3: Descargando datasets de footprints..."

# Microsoft
MICROSOFT_FILE="${MICROSOFT_INPUT_FILE:-samples/sample_microsoft.geojsonl}"
if [ ! -f "/app/$MICROSOFT_FILE" ]; then
  echo "[ETL] Descargando Microsoft footprints..."
  if [ -x /app/scripts/download_microsoft.sh ]; then
    /app/scripts/download_microsoft.sh
  else
    sh /app/scripts/download_microsoft.sh || true
  fi
fi

# Google
GOOGLE_FILE="${GOOGLE_INPUT_FILE:-samples/google_buildings.geojson}"
if [ ! -f "/app/$GOOGLE_FILE" ]; then
  echo "[ETL] Descargando Google footprints..."
  if [ -x /app/scripts/download_google.sh ]; then
    /app/scripts/download_google.sh
  else
    sh /app/scripts/download_google.sh || true
  fi

  # Convertir CSV.GZ a GeoJSON si es necesario
  if [ ! -f "/app/$GOOGLE_FILE" ] && [ -f "/app/samples/google_part1.csv.gz" ]; then
    echo "[ETL] Convirtiendo Google CSV.GZ a GeoJSON..."
    python3 /app/scripts/convert_csv_to_geojson.py \
      /app/samples/google_part1.csv.gz \
      /app/samples/google_part2.csv.gz \
      /app/samples/google_part3.csv.gz \
      /app/samples/google_part4.csv.gz \
      /app/samples/google_buildings.geojson
  fi
fi

# ================================================
# PASO 4: Cargar footprints CON FILTRO PDET
# ================================================
echo "[ETL] PASO 4: Cargando Google footprints (solo PDET)..."
python3 /app/cargar_google_footprints.py

echo "[ETL] PASO 5: Cargando Microsoft footprints (solo PDET)..."
python3 /app/cargar_microsoft_footprints.py

# ================================================
# PASO 6: Análisis EDA
# ================================================
echo "[ETL] PASO 6: Ejecutando análisis exploratorio..."
python3 /app/eda_footprints.py

echo "[ETL] Pipeline finalizado exitosamente."
exit 0
