#!/usr/bin/env bash
set -euo pipefail

echo "[ETL] Iniciando pipeline ETL: municipios -> PDET -> footprints"

# Espera corta para asegurar que mongo esté listo
sleep 10

# ================================================
# Descargar y cargar municipios MGN
# ================================================
echo "[ETL] Ejecutando: cargar_municipios.py"
if [ ! -f /app/MGN2024_00_COLOMBIA.zip ]; then
	echo "[ETL] ZIP MGN no encontrado en /app. Intentando descargar..."
	if [ -x /app/scripts/download_mgn.sh ]; then
		/app/scripts/download_mgn.sh
	else
		# si no es ejecutable, intentar con sh
		sh /app/scripts/download_mgn.sh || true
	fi
fi

python3 /app/cargar_municipios.py

# ================================================
# Crear colección PDET
# ================================================
echo "[ETL] Ejecutando: create_mgn_municipios_pdet.py"
python3 /app/scripts/create_mgn_municipios_pdet.py

# ================================================
# Descargar y cargar footprints Microsoft
# ================================================
echo "[ETL] Verificando Microsoft footprints..."
MICROSOFT_FILE="${MICROSOFT_INPUT_FILE:-samples/sample_microsoft.geojsonl}"
if [ ! -f "/app/$MICROSOFT_FILE" ]; then
	echo "[ETL] Microsoft footprints no encontrados. Intentando descargar..."
	if [ -x /app/scripts/download_microsoft.sh ]; then
		/app/scripts/download_microsoft.sh
	else
		sh /app/scripts/download_microsoft.sh || true
	fi
fi

echo "[ETL] Ejecutando: cargar_microsoft_footprints.py"
python3 /app/cargar_microsoft_footprints.py

# ================================================
# Descargar y cargar footprints Google
# ================================================
echo "[ETL] Verificando Google footprints..."
GOOGLE_FILE="${GOOGLE_INPUT_FILE:-samples/google_buildings.geojson}"
if [ ! -f "/app/$GOOGLE_FILE" ]; then
	echo "[ETL] Google footprints no encontrados. Intentando descargar y convertir..."
	if [ -x /app/scripts/download_google.sh ]; then
		/app/scripts/download_google.sh
	else
		sh /app/scripts/download_google.sh || true
	fi
	
	# Convertir CSV.GZ a GeoJSON
	echo "[ETL] Convirtiendo Google CSV.GZ a GeoJSON..."
	python3 /app/scripts/convert_csv_to_geojson.py \
		/app/samples/google_part1.csv.gz \
		/app/samples/google_part2.csv.gz \
		/app/samples/google_part3.csv.gz \
		/app/samples/google_part4.csv.gz \
		/app/samples/google_buildings.geojson
fi

echo "[ETL] Ejecutando: cargar_google_footprints.py"
python3 /app/cargar_google_footprints.py

echo "[ETL] Pipeline finalizado."

# Mantener salida de contenedor limpia
exit 0
