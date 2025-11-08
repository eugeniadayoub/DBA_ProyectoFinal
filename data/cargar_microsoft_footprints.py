from pymongo import MongoClient, GEOSPHERE
import json
import os
from datetime import datetime

# Configuración
GEOJSON_FILE = 'samples/sample_microsoft.geojson'
MONGO_URI = 'mongodb://mongo-upme:27017/'
DB_NAME = 'proyecto_upme'
COLLECTION_NAME = 'microsoft_footprints'

print("="*60)
print("CARGA DE MICROSOFT BUILDING FOOTPRINTS")
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
    print(f"  Directorio actual: {os.getcwd()}")
    print(f"  Archivos disponibles:")
    for root, dirs, files in os.walk('.'):
        for file in files:
            if file.endswith('.geojson'):
                print(f"    - {os.path.join(root, file)}")
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
        if feature.get('geometry'):
            # Microsoft solo tiene geometry (Polygon), no tiene properties adicionales
            # Vamos a calcular el centroide y área desde la geometría
            geometry = feature['geometry']
            
            # Calcular centroide aproximado (promedio de coordenadas)
            if geometry['type'] == 'Polygon':
                coords = geometry['coordinates'][0]  # Primer anillo del polígono
                lons = [c[0] for c in coords]
                lats = [c[1] for c in coords]
                centroid_lon = sum(lons) / len(lons)
                centroid_lat = sum(lats) / len(lats)
            else:
                centroid_lon = None
                centroid_lat = None
            
            # Microsoft no tiene properties en este dataset, 
            # solo la geometría del polígono
            documento = {
                'source': 'Microsoft Building Footprints',
                'centroid_latitude': centroid_lat,
                'centroid_longitude': centroid_lon,
                'geometry': geometry,  # Polygon en formato GeoJSON
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
    
    # Índices adicionales
    collection.create_index([("centroid_latitude", 1)])
    collection.create_index([("centroid_longitude", 1)])
    print("✓ Índices adicionales creados (centroid_latitude, centroid_longitude)")
    
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
    print(f"  - Centroid Lat: {ejemplo.get('centroid_latitude', 'N/A')}")
    print(f"  - Centroid Lon: {ejemplo.get('centroid_longitude', 'N/A')}")
    print(f"  - Geometry type: {ejemplo['geometry']['type']}")
    print(f"  - Coordinates: {len(ejemplo['geometry']['coordinates'][0])} puntos en el polígono")

# Estadísticas básicas
print("\n📊 Estadísticas básicas:")

# Calcular áreas usando agregación con $geoNear o simplemente contar
print(f"  Total de edificaciones: {count}")

# Rangos de coordenadas de centroides
pipeline_coords = [
    {
        '$match': {
            'centroid_latitude': {'$ne': None},
            'centroid_longitude': {'$ne': None}
        }
    },
    {
        '$group': {
            '_id': None,
            'lat_min': {'$min': '$centroid_latitude'},
            'lat_max': {'$max': '$centroid_latitude'},
            'lon_min': {'$min': '$centroid_longitude'},
            'lon_max': {'$max': '$centroid_longitude'}
        }
    }
]

coords_stats = list(collection.aggregate(pipeline_coords))
if coords_stats:
    print(f"  Rango de Latitudes: {coords_stats[0]['lat_min']:.6f} a {coords_stats[0]['lat_max']:.6f}")
    print(f"  Rango de Longitudes: {coords_stats[0]['lon_min']:.6f} a {coords_stats[0]['lon_max']:.6f}")

# Distribución por tipo de geometría
pipeline_geom = [
    {
        '$group': {
            '_id': '$geometry.type',
            'count': {'$sum': 1}
        }
    }
]

geom_stats = list(collection.aggregate(pipeline_geom))
if geom_stats:
    print(f"\n  Tipos de geometría:")
    for g in geom_stats:
        print(f"    - {g['_id']}: {g['count']} edificaciones")

print("\n" + "="*60)
print("✓ CARGA COMPLETADA EXITOSAMENTE")
print("="*60)

client.close()
