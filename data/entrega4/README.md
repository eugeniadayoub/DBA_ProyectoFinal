## Comandos de ejecución

# Correr el pipeline

chmod +x entrega4/run_pdet_pipeline.sh
./entrega4/run_pdet_pipeline.sh

Se ejecutan automáticamente

# Crear índices espaciales
mongosh local entrega4/ensure_indexes.js

# Filtrar Google y Microsoft por municipios PDET
mongosh local entrega4/run_pdet_filter.js

# Exportar Google
python3 entrega4/export_pdet_google.py

# Exportar Microsoft
python3 entrega4/export_pdet_microsoft.py