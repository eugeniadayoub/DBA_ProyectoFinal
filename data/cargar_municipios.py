from pymongo import MongoClient, GEOSPHERE
import json
import os

GEOJSON_FILE = 'municipios_pdet_filtrados.geojson'
MONGO_URI = 'mongodb://mongo-upme:27017/'
DB_NAME = 'proyecto_upme'
COLLECTION_NAME = 'mgn_municipios_pdet'

try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print(f"Conectado a MongoDB (Base de datos: {DB_NAME}, Colección: {COLLECTION_NAME})")
except Exception as e:
    print(f"ERROR: No se pudo conectar a MongoDB.")
    print(f"Detalle: {e}")
    exit()

collection.delete_many({})
print("Colección limpiada (delete_many).")

if not os.path.exists(GEOJSON_FILE):
    print(f"ERROR: No se encontró el archivo '{GEOJSON_FILE}'")
    client.close()
    exit()

with open(GEOJSON_FILE, 'r', encoding='utf-8') as f:
    geojson_data = json.load(f)
features = geojson_data.get('features', [])
if not features:
    print("ERROR: El archivo GeoJSON no tiene 'features' o está vacío.")
    client.close()
    exit()

print(f"Leyendo {len(features)} municipios del archivo GeoJSON...")

documentos_para_insertar = []

for feature in features:
    if feature.get('geometry') and feature.get('properties'):
        props = feature['properties']
        documento = {
            'codigo_municipio': props.get('MunicipiosPDET(MunicipiosPDET)_C�digo DANE Municipio'),
            'nombre_municipio': props.get('MunicipiosPDET(MunicipiosPDET)_Municipio'),
            'departamento': props.get('MunicipiosPDET(MunicipiosPDET)_Departamento'),
            'geometry': feature['geometry']
        }
        documentos_para_insertar.append(documento)

if not documentos_para_insertar:
    print("No se prepararon documentos. Revisa el GeoJSON.")
    client.close()
    exit()

try:
    collection.insert_many(documentos_para_insertar)
    print(f"¡ÉXITO! Se insertaron {len(documentos_para_insertar)} municipios en la colección.")
except Exception as e:
    print(f"ERROR: Falló la inserción de datos. Detalle: {e}")
    client.close()
    exit()

try:
    print("Creando índice 2dsphere en el campo 'geometry'...")
    collection.create_index([("geometry", GEOSPHERE)])
    print("¡Índice '2dsphere' creado exitosamente!")
except Exception as e:
    print(f"ERROR: Falló la creación del índice. Detalle: {e}")
    client.close()
    exit()

count = collection.count_documents({})
print(f"Verificación: La colección '{COLLECTION_NAME}' ahora tiene {count} documentos.")
print("Un documento de ejemplo de la BD:")
print(collection.find_one())

client.close()
