#!/bin/bash

# Termina el script inmediatamente si un comando falla
set -e

echo "--- Iniciando configuración del proyecto (100% Dockerizado) ---"

# --- 1. Verificación de Dependencias (Ahora solo Docker) ---
command -v docker >/dev/null 2>&1 || { 
  echo >&2 "Error: 'docker' no está instalado. Por favor, instala Docker Desktop."; 
  exit 1; 
}
command -v docker-compose >/dev/null 2>&1 || { 
  echo >&2 "Error: 'docker-compose' no se encuentra. Asegúrate de que Docker Desktop esté instalado y corriendo."; 
  exit 1; 
}

echo "[PASO 1/2] Construyendo y levantando contenedores..."
echo "Esto puede tardar un momento la primera vez (construyendo imagen de Python)..."

# --build: Fuerza la construcción del Dockerfile de Python
# -d: Corre en segundo plano (detached)
docker-compose up --build -d

echo "[PASO 2/2] Esperando y mostrando logs del script de carga de datos..."
echo "El script está cargando los datos en la BD. Esto puede tomar unos segundos."

# Muestra los logs del script 'etl-loader' en tiempo real.
# Cuando el script termine de ejecutarse, este comando también lo hará.
docker-compose logs -f etl-loader

echo "--- ¡Configuración completada! ---"
echo "El script de carga finalizó."
echo "La base de datos 'mongo-proyecto-upme' está corriendo con los datos cargados."
echo "Puedes verificar en MongoDB Compass: mongodb://localhost:27017"
