from pymongo import MongoClient, GEOSPHERE
import json
import os
from datetime import datetime

# Configuración
GEOJSON_FILE = 'samples/sample_google1.geojson'
MONGO_URI = 'mongodb://mongo-upme:27017/'
DB_NAME = 'proyecto_upme'
COLLECTION_NAME = 'google_footprints'

print("="*60)
print("CARGA DE GOOGLE OPEN BUILDINGS FOOTPRINTS")
print("="*60)

# 1. Conectar a MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print(f"✓ Conectado a MongoDB")
    print(f"  Base de datos: {DB_NAME}")
    print(f"  Colección: {COLLECTION_NAME}")
except Exception as e:
    print(f"✗ ERROR: No se pudo conectar a MongoDB.")
    print(f"  Detalle: {e}")
    exit(1)

# 2. Limpiar colección (si existe)
collection.delete_many({})
print(f"✓ Colección limpiada")

# 3. Verificar que existe el archivo
if not os.path.exists(GEOJSON_FILE):
    print(f"✗ ERROR: No se encontró el archivo '{GEOJSON_FILE}'")
    client.close()
    exit(1)

# 4. Leer el GeoJSON
try:
    with open(GEOJSON_FILE, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    features = geojson_data.get('features', [])
    
    if not features:
        print("✗ ERROR: El archivo GeoJSON no tiene 'features' o está vacío.")
        client.close()
        exit(1)
    
    print(f"✓ Archivo leído correctamente")
    print(f"  Total de features: {len(features)}")
except Exception as e:
    print(f"✗ ERROR: No se pudo leer el archivo GeoJSON.")
    print(f"  Detalle: {e}")
    client.close()
    exit(1)

# 5. Preparar documentos para insertar
print("\n" + "="*60)
print("TRANSFORMANDO DATOS...")
print("="*60)

documentos_para_insertar = []
errores = 0

for idx, feature in enumerate(features, 1):
    try:
        if feature.get('geometry') and feature.get('properties'):
            props = feature['properties']
            
            # Google usa Point geometry en el GeoJSON
            # Pero también tiene un campo 'geometry' en properties con el polígono WKT
            documento = {
                'source': 'Google Open Buildings',
                'latitude': props.get('latitude'),
                'longitude': props.get('longitude'),
                'area_in_meters': props.get('area_in_meters'),
                'confidence': props.get('confidence'),
                'full_plus_code': props.get('full_plus_code'),
                'geometry_wkt': props.get('geometry'),  # Polígono en formato WKT
                'geometry': feature['geometry'],  # Point en formato GeoJSON
                'loaded_at': datetime.utcnow()
            }
            documentos_para_insertar.append(documento)
            
            if idx % 50 == 0:
                print(f"  Procesados: {idx}/{len(features)}")
    except Exception as e:
        errores += 1
        print(f"  ⚠ Error en feature {idx}: {e}")

print(f"\n✓ Transformación completa")
print(f"  Documentos válidos: {len(documentos_para_insertar)}")
print(f"  Errores encontrados: {errores}")

if not documentos_para_insertar:
    print("✗ No se prepararon documentos. Abortando.")
    client.close()
    exit(1)

# 6. Insertar en MongoDB
print("\n" + "="*60)
print("INSERTANDO EN MONGODB...")
print("="*60)

try:
    inicio = datetime.now()
    result = collection.insert_many(documentos_para_insertar)
    fin = datetime.now()
    tiempo_carga = (fin - inicio).total_seconds()
    
    print(f"✓ INSERCIÓN EXITOSA")
    print(f"  Documentos insertados: {len(result.inserted_ids)}")
    print(f"  Tiempo de carga: {tiempo_carga:.2f} segundos")
    print(f"  Velocidad: {len(result.inserted_ids)/tiempo_carga:.2f} docs/segundo")
except Exception as e:
    print(f"✗ ERROR: Falló la inserción de datos.")
    print(f"  Detalle: {e}")
    client.close()
    exit(1)

# 7. Crear índice 2dsphere
print("\n" + "="*60)
print("CREANDO ÍNDICE ESPACIAL...")
print("="*60)

try:
    # Primero verificar si el índice ya existe
    indices_existentes = list(collection.list_indexes())
    indice_existe = any(
        'geometry' in idx.get('key', {}) and idx['key']['geometry'] == '2dsphere'
        for idx in indices_existentes
    )
    
    if not indice_existe:
        collection.create_index([("geometry", GEOSPHERE)])
        print("✓ Índice '2dsphere' creado en campo 'geometry'")
    else:
        print("✓ Índice '2dsphere' ya existía")
    
    # También crear índices adicionales útiles
    collection.create_index([("confidence", 1)])
    collection.create_index([("area_in_meters", 1)])
    print("✓ Índices adicionales creados (confidence, area_in_meters)")
    
except Exception as e:
    print(f"✗ ERROR: Falló la creación de índices.")
    print(f"  Detalle: {e}")
    client.close()
    exit(1)

# 8. Verificación final
print("\n" + "="*60)
print("VERIFICACIÓN FINAL")
print("="*60)

count = collection.count_documents({})
print(f"✓ Documentos en colección: {count}")

# Mostrar un documento de ejemplo
print("\n📄 Ejemplo de documento:")
ejemplo = collection.find_one()
if ejemplo:
    print(f"  - ID: {ejemplo['_id']}")
    print(f"  - Source: {ejemplo['source']}")
    print(f"  - Latitude: {ejemplo['latitude']}")
    print(f"  - Longitude: {ejemplo['longitude']}")
    print(f"  - Area (m²): {ejemplo['area_in_meters']}")
    print(f"  - Confidence: {ejemplo['confidence']}")
    print(f"  - Plus Code: {ejemplo['full_plus_code']}")
    print(f"  - Geometry type: {ejemplo['geometry']['type']}")

# Estadísticas básicas
print("\n📊 Estadísticas básicas:")
pipeline_stats = [
    {
        '$group': {
            '_id': None,
            'avg_area': {'$avg': '$area_in_meters'},
            'min_area': {'$min': '$area_in_meters'},
            'max_area': {'$max': '$area_in_meters'},
            'avg_confidence': {'$avg': '$confidence'},
            'min_confidence': {'$min': '$confidence'},
            'max_confidence': {'$max': '$confidence'}
        }
    }
]

stats = list(collection.aggregate(pipeline_stats))
if stats:
    s = stats[0]
    print(f"  Área promedio: {s['avg_area']:.2f} m²")
    print(f"  Área mínima: {s['min_area']:.2f} m²")
    print(f"  Área máxima: {s['max_area']:.2f} m²")
    print(f"  Confianza promedio: {s['avg_confidence']:.4f}")
    print(f"  Confianza mínima: {s['min_confidence']:.4f}")
    print(f"  Confianza máxima: {s['max_confidence']:.4f}")

print("\n" + "="*60)
print("✓ CARGA COMPLETADA EXITOSAMENTE")
print("="*60)

client.close()
