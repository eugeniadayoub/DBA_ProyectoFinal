# 1. Usar una imagen oficial de Python
FROM python:3.10-slim

# 2. Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Instalar las dependencias de Python (el 'pip install' que fallaba)
# Esto se ejecuta DENTRO del contenedor, de forma aislada y segura.
RUN pip install pymongo

# 4. Copiar los archivos necesarios desde tu PC al contenedor
COPY . .
# 5. El comando que se ejecutará cuando el contenedor inicie
CMD ["python3", "cargar_municipios.py"]
