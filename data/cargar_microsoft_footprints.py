from pymongo import MongoClient, GEOSPHERE
import json
import os
from datetime import datetime
from shapely.geometry import shape

# Configuración
GEOJSON_FILE = 'samples/sample_microsoft.geojson'
MONGO_URI = 'mongodb://mongo-upme:27017/'
DB_NAME = 'proyecto_upme'
COLLECTION_NAME = 'buildings_microsoft'  # ← Nombre correcto según Primera Entrega

print("="*60)
print("CARGA DE MICROSOFT BUILDING FOOTPRINTS")
print("Siguiendo modelo de Primera Entrega")
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
print("TRANSFORMANDO DATOS AL MODELO DE PRIMERA ENTREGA...")
print("="*60)

documentos_para_insertar = []
errores = 0
contador_id = 1

for idx, feature in enumerate(features, 1):
    try:
        if feature.get('geometry'):
            geometry = feature['geometry']
            
            # Calcular centroide usando Shapely
            try:
                polygon_shapely = shape(geometry)
                centroid = polygon_shapely.centroid
                centroid_geojson = {
                    'type': 'Point',
                    'coordinates': [centroid.x, centroid.y]
                }
                
                # Calcular área en m² (aproximado, asumiendo coordenadas geográficas)
                # Área en grados cuadrados convertida a m²
                # Factor aproximado: 1 grado² ≈ 12321 km² a latitud ecuatorial
                # Usamos cálculo más preciso con Shapely en proyección UTM simplificada
                area_grados = polygon_shapely.area
                # Aproximación: área en m² usando factor de conversión
                # 1 grado lat ≈ 111km, 1 grado lon ≈ 111km * cos(lat)
                lat_promedio = centroid.y
                factor_conversion = (111000 ** 2) * abs(0.9) # Factor conservador
                area_m2 = area_grados * factor_conversion
                
            except Exception as e:
                print(f"  ⚠ Error calculando centroide/área en feature {idx}: {e}")
                errores += 1
                continue
            
            # Crear documento según modelo de Primera Entrega
            documento = {
                'building_id': f"MS-Bldg-{contador_id:08d}",  # ← Campo requerido
                'fuente': 'Microsoft',  # ← Campo requerido (no "source")
                'codigo_municipio': None,  # ← Se llenará en fase de integración
                'geometry': geometry,  # ← Polygon en GeoJSON (ya lo tenemos correcto)
                'centroid': centroid_geojson,  # ← Centroid como GeoJSON Point
                'area_m2': area_m2,  # ← Área calculada
                'loaded_at': datetime.utcnow()
            }
            
            documentos_para_insertar.append(documento)
            contador_id += 1
            
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

# 7. Crear índices espaciales
print("\n" + "="*60)
print("CREANDO ÍNDICES ESPACIALES...")
print("="*60)

try:
    # Índice 2dsphere en geometry (Polygon)
    collection.create_index([("geometry", GEOSPHERE)])
    print("✓ Índice 2dsphere creado en 'geometry'")
    
    # Índice 2dsphere en centroid (Point)
    collection.create_index([("centroid", GEOSPHERE)])
    print("✓ Índice 2dsphere creado en 'centroid'")
    
    # Índices adicionales útiles
    collection.create_index([("building_id", 1)], unique=True)
    print("✓ Índice único creado en 'building_id'")
    
    collection.create_index([("codigo_municipio", 1)])
    print("✓ Índice creado en 'codigo_municipio'")
    
    collection.create_index([("area_m2", 1)])
    print("✓ Índice creado en 'area_m2'")
    
except Exception as e:
    print(f"⚠ ERROR al crear índices: {e}")

# 8. Verificación final
print("\n" + "="*60)
print("VERIFICACIÓN FINAL")
print("="*60)

count = collection.count_documents({})
print(f"✓ Documentos en colección: {count}")

# Mostrar un documento de ejemplo
print("\n📄 Ejemplo de documento (modelo Primera Entrega):")
ejemplo = collection.find_one()
if ejemplo:
    print(f"  - ID MongoDB: {ejemplo['_id']}")
    print(f"  - building_id: {ejemplo['building_id']}")
    print(f"  - fuente: {ejemplo['fuente']}")
    print(f"  - codigo_municipio: {ejemplo['codigo_municipio']}")
    print(f"  - geometry type: {ejemplo['geometry']['type']}")
    print(f"  - centroid: {ejemplo['centroid']}")
    print(f"  - area_m2: {ejemplo['area_m2']:.2f}")
    print(f"  - loaded_at: {ejemplo['loaded_at']}")

# Estadísticas básicas
print("\n📊 Estadísticas de área:")
pipeline_stats = [
    {
        '$group': {
            '_id': None,
            'area_min': {'$min': '$area_m2'},
            'area_max': {'$max': '$area_m2'},
            'area_avg': {'$avg': '$area_m2'},
            'area_total': {'$sum': '$area_m2'}
        }
    }
]

stats = list(collection.aggregate(pipeline_stats))
if stats:
    s = stats[0]
    print(f"  Área mínima:   {s['area_min']:.2f} m²")
    print(f"  Área máxima:   {s['area_max']:.2f} m²")
    print(f"  Área promedio: {s['area_avg']:.2f} m²")
    print(f"  Área total:    {s['area_total']:.2f} m² ({s['area_total']/10000:.2f} hectáreas)")

print("\n" + "="*60)
print("✓ CARGA COMPLETADA - MODELO PRIMERA ENTREGA")
print("="*60)
print()

client.close()
