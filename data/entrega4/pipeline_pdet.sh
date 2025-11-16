#!/bin/bash

echo "=== Asegurando índices ==="
mongosh local entrega4/ensure_indexes.js

echo "=== Filtrando por municipios PDET ==="
mongosh local entrega4/run_pdet_filter.js

echo "=== Exportando Google ==="
python3 entrega4/export_pdet_google.py

echo "=== Exportando Microsoft ==="
python3 entrega4/export_pdet_microsoft.py

echo "=== Pipeline PDET COMPLETADO ==="
