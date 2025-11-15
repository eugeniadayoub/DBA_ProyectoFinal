#!/usr/bin/env bash
set -euo pipefail

echo "[ETL] Iniciando pipeline ETL: cargar municipios -> crear colección PDET"

# Espera corta para asegurar que mongo esté listo
sleep 10

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

echo "[ETL] Ejecutando: create_mgn_municipios_pdet.py"
python3 /app/scripts/create_mgn_municipios_pdet.py

echo "[ETL] Pipeline finalizado."

# Mantener salida de contenedor limpia
exit 0
