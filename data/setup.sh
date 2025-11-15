#!/bin/bash#!/bin/sh

set -eset -e # <-- ¡Importante! El script fallará si un paso falla.



echo "=============================================# setup.sh

INICIANDO PIPELINE DE DATOS - PROYECTO UPME# Orquesta todo el pipeline de ELT en el orden correcto.

============================================="

echo "============================================="

# Esperar a que MongoDB esté listoecho "INICIANDO PIPELINE DE DATOS - PROYECTO UPME"

echo "Esperando a que MongoDB inicie..."echo "============================================="

sleep 10

# Esperar 10s para asegurar que MongoDB esté 100% listo

# Ejecutar script de carga de municipiosecho "Esperando a que MongoDB inicie..."

echo "Ejecutando carga de municipios MGN (DANE)..."sleep 10

python3 /app/cargar_municipios.py

# --- PASO 1: ENTREGA 2 (Municipios) ---

echo "=============================================echo "Ejecutando script de carga de municipios (ELT)..."

PIPELINE COMPLETADO EXITOSAMENTEpython3 /app/cargar_municipios.py

============================================="

# --- PASO 2: ENTREGA 3 (Edificios Google) ---
echo "Ejecutando script de carga de Google Footprints..."
python3 /app/cargar_google_footprints.py

# --- PASO 3: ENTREGA 3 (Edificios Microsoft) ---
echo "Ejecutando script de carga de Microsoft Footprints..."
python3 /app/cargar_microsoft_footprints.py

# --- PASO 4: ANÁLISIS (EDA) ---
echo "Ejecutando script de análisis (EDA)..."
python3 /app/eda_footprints.py

echo "============================================="
echo "PIPELINE FINALIZADO."
echo "============================================="