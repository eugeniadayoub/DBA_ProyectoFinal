from pymongo import MongoClient, GEOSPHERE
import json
import os
from datetime import datetime
from shapely.geometry import shape, mapping, Point
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient

# Configuración
GEOJSON_FILE = os.getenv('GOOGLE_INPUT_FILE', 'samples/google_buildings.geojson')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo-upme:27017/')
DB_NAME = os.getenv('DB_NAME', 'dba_proyectofinal')
COLLECTION_NAME = 'buildings_google'
PDET_COLLECTION = 'mgn_municipios_pdet'

print("="*60)
print("CARGA DE GOOGLE BUILDING FOOTPRINTS - SOLO PDET")
print("Con filtrado espacial y asignación de código de municipio")
print("="*60)

# 1. Conectar a MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    pdet_collection = db[PDET_COLLECTION]
    print(f"✓ Conectado a MongoDB")
    print(f"  Base de datos: {DB_NAME}")
    print(f"  Colección destino: {COLLECTION_NAME}")
    print(f"  Colección PDET: {PDET_COLLECTION}")
except Exception as e:
    print(f"✗ ERROR: No se pudo conectar a MongoDB.")
    print(f"  Detalle: {e}")
    exit(1)

# 2. Cargar municipios PDET en memoria
print("\n" + "="*60)
print("CARGANDO MUNICIPIOS PDET EN MEMORIA...")
print("="*60)

try:
    municipios_pdet = list(pdet_collection.find({}, {
        'codigo_municipio': 1,
        'nombre_municipio': 1,
        'departamento': 1,
        'geometry': 1
    }))
    
    if not municipios_pdet:
        print("✗ ERROR: No hay municipios PDET en la base de datos.")
        print("  Ejecuta primero: python3 /app/scripts/create_mgn_municipios_pdet.py")
        client.close()
        exit(1)
    
    print(f"✓ Cargados {len(municipios_pdet)} municipios PDET")
    
    # Convertir geometrías a Shapely para búsqueda rápida
    municipios_shapes = []
    for mpio in municipios_pdet:
        try:
            geom = shape(mpio['geometry'])
            municipios_shapes.append({
                'codigo': mpio['codigo_municipio'],
                'nombre': mpio.get('nombre_municipio', ''),
                'shape': geom
            })
        except Exception as e:
            print(f"  ⚠ Error procesando municipio {mpio.get('codigo_municipio')}: {e}")
    
    print(f"✓ {len(municipios_shapes)} geometrías preparadas para búsqueda espacial")
    
except Exception as e:
    print(f"✗ ERROR al cargar municipios PDET: {e}")
    client.close()
    exit(1)

# 3. Limpiar colección destino
collection.delete_many({})
print(f"✓ Colección limpiada")

# 4. Verificar archivo
if not os.path.exists(GEOJSON_FILE):
    print(f"✗ ERROR: No se encontró el archivo '{GEOJSON_FILE}'")
    client.close()
    exit(1)

# 5. Función para encontrar municipio que contiene un punto
def find_municipio_for_point(lat, lon, municipios_list):
    """Encuentra el municipio PDET que contiene este punto"""
    point = Point(lon, lat)
    
    for mpio in municipios_list:
        try:
            if mpio['shape'].contains(point):
                return mpio['codigo']
        except Exception:
            continue
    
    return None

# 6. Leer y procesar GeoJSON con filtrado
print("\n" + "="*60)
print("PROCESANDO FOOTPRINTS CON FILTRO PDET...")
print("="*60)

def iter_features_from_featurecollection(path):
    """Generador que itera Features desde un GeoJSON FeatureCollection"""
    with open(path, 'r', encoding='utf-8') as f:
        buf = ''
        while True:
            chunk = f.read(8192)
            if not chunk:
                return
            buf += chunk
            idx = buf.find('"features"')
            if idx != -1:
                arr_idx = buf.find('[', idx)
                if arr_idx != -1:
                    consumed = len(buf[:arr_idx+1])
                    f.seek(f.tell() - len(buf) + consumed)
                    break
        
        depth = 0
        in_str = False
        escape = False
        obj_buf = ''
        while True:
            ch = f.read(1)
            if not ch:
                break
            if in_str:
                obj_buf += ch
                if escape:
                    escape = False
                elif ch == '\\':
                    escape = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '{':
                depth += 1
                obj_buf += ch
            elif ch == '}':
                depth -= 1
                obj_buf += ch
                if depth == 0:
                    try:
                        yield json.loads(obj_buf)
                    except Exception:
                        pass
                    obj_buf = ''
            elif ch == '"':
                in_str = True
                obj_buf += ch
            else:
                if depth > 0:
                    obj_buf += ch

try:
    print("Leyendo GeoJSON en streaming...")
    try:
        size = os.path.getsize(GEOJSON_FILE)
    except Exception:
        size = None
    
    if size and size < (50 * 1024 * 1024):
        with open(GEOJSON_FILE, 'r', encoding='utf-8') as _f:
            js = json.load(_f)
            features_iter = iter(js.get('features', []))
    else:
        features_iter = iter_features_from_featurecollection(GEOJSON_FILE)
    
    print("✓ Inicio de lectura listo")
except Exception as e:
    print(f"✗ ERROR: No se pudo leer el GeoJSON: {e}")
    client.close()
    exit(1)

# 7. Procesar features con filtro PDET
errores = 0
contador_id = 1
procesados = 0
filtrados_pdet = 0
fuera_pdet = 0

BATCH_SIZE = int(os.getenv('GOOGLE_BATCH_SIZE', '5000'))
batch = []
inserted_count = 0

print(f"✓ BATCH_SIZE = {BATCH_SIZE}")
print("\nProcesando edificios...")

for feature in features_iter:
    procesados += 1
    
    try:
        # Extraer geometría y propiedades
        geometry = None
        properties = {}
        
        if isinstance(feature, dict) and feature.get('geometry'):
            geometry = feature['geometry']
            properties = feature.get('properties', {}) or {}
        elif isinstance(feature, dict) and feature.get('type') and feature.get('coordinates'):
            geometry = {'type': feature.get('type'), 'coordinates': feature.get('coordinates')}
            properties = {}
        
        if geometry is None:
            errores += 1
            continue
        
        # Obtener coordenadas del centroide/punto
        if geometry['type'] == 'Point':
            coords = geometry['coordinates']
            lon, lat = coords[0], coords[1]
        else:
            # Si es polígono, calcular centroide
            try:
                poly = shape(geometry)
                centroid = poly.centroid
                lon, lat = centroid.x, centroid.y
            except Exception:
                errores += 1
                continue
        
        # FILTRO CRÍTICO: Buscar si está en un municipio PDET
        codigo_mpio = find_municipio_for_point(lat, lon, municipios_shapes)
        
        if codigo_mpio is None:
            # NO está en ningún municipio PDET, omitir
            fuera_pdet += 1
            continue
        
        # SÍ está en PDET, procesar y guardar
        filtrados_pdet += 1
        
        # Normalizar geometría
        def normalize_geometry_geojson(geom_json):
            try:
                g = shape(geom_json)
            except Exception:
                return None
            if not g.is_valid:
                try:
                    from shapely.ops import make_valid
                    g = make_valid(g)
                except Exception:
                    try:
                        g = g.buffer(0)
                    except Exception:
                        return None
            try:
                if isinstance(g, Polygon):
                    g = orient(g, sign=1.0)
                elif isinstance(g, MultiPolygon):
                    g = MultiPolygon([orient(p, sign=1.0) for p in g.geoms])
            except Exception:
                pass
            return g
        
        try:
            polygon_shapely = normalize_geometry_geojson(geometry)
            if polygon_shapely is None:
                raise ValueError('geometría inválida')
            
            centroid = polygon_shapely.centroid
            centroid_geojson = {
                'type': 'Point',
                'coordinates': [centroid.x, centroid.y]
            }
            
            # Calcular área
            area_grados = polygon_shapely.area
            lat_promedio = centroid.y
            factor_conversion = (111000 ** 2) * abs(0.9)
            area_m2 = area_grados * factor_conversion
            
        except Exception as e:
            errores += 1
            continue
        
        # Crear documento CON codigo_municipio asignado
        documento = {
            'building_id': f"G-Bldg-{contador_id:08d}",
            'fuente': 'Google',
            'codigo_municipio': codigo_mpio,  # ← ASIGNADO!
            'geometry': mapping(polygon_shapely),
            'centroid': centroid_geojson,
            'area_m2': area_m2,
            'loaded_at': datetime.utcnow()
        }
        
        if properties:
            documento['properties'] = properties
        
        batch.append(documento)
        contador_id += 1
        
        if procesados % 10000 == 0:
            print(f"  Procesados: {procesados:,} | En PDET: {filtrados_pdet:,} | Fuera: {fuera_pdet:,}")
        
        if len(batch) >= BATCH_SIZE:
            try:
                collection.insert_many(batch)
                inserted_count += len(batch)
                print(f"  ✓ Insertados: {inserted_count:,}")
            except Exception as e:
                print(f"✗ ERROR al insertar batch: {e}")
            batch = []
    
    except Exception as e:
        errores += 1

print(f"\n✓ Procesamiento completo")
print(f"  Total procesados: {procesados:,}")
print(f"  En municipios PDET: {filtrados_pdet:,}")
print(f"  Fuera de PDET: {fuera_pdet:,}")
print(f"  Errores: {errores:,}")

# Insertar batch restante
if batch:
    try:
        collection.insert_many(batch)
        inserted_count += len(batch)
        print(f"  ✓ Insertados (final): {inserted_count:,}")
    except Exception as e:
        print(f"✗ ERROR al insertar batch final: {e}")

if inserted_count == 0:
    print("✗ No se insertó ningún documento.")
    client.close()
    exit(1)

# 8. Crear índices
print("\n" + "="*60)
print("CREANDO ÍNDICES...")
print("="*60)

try:
    collection.create_index([("geometry", GEOSPHERE)])
    print("✓ Índice 2dsphere en 'geometry'")
    
    collection.create_index([("centroid", GEOSPHERE)])
    print("✓ Índice 2dsphere en 'centroid'")
    
    collection.create_index([("building_id", 1)], unique=True)
    print("✓ Índice único en 'building_id'")
    
    collection.create_index([("codigo_municipio", 1)])
    print("✓ Índice en 'codigo_municipio'")
    
    collection.create_index([("area_m2", 1)])
    print("✓ Índice en 'area_m2'")
    
except Exception as e:
    print(f"⚠ ERROR al crear índices: {e}")

# 9. Verificación final
print("\n" + "="*60)
print("VERIFICACIÓN FINAL")
print("="*60)

count = collection.count_documents({})
print(f"✓ Documentos en colección: {count:,}")

# Verificar que TODOS tienen codigo_municipio
sin_codigo = collection.count_documents({'codigo_municipio': None})
print(f"✓ Documentos sin codigo_municipio: {sin_codigo}")

if sin_codigo > 0:
    print("⚠ ADVERTENCIA: Hay documentos sin código de municipio")

# Mostrar ejemplo
ejemplo = collection.find_one()
if ejemplo:
    print(f"\n📄 Ejemplo de documento:")
    print(f"  - building_id: {ejemplo['building_id']}")
    print(f"  - fuente: {ejemplo['fuente']}")
    print(f"  - codigo_municipio: {ejemplo['codigo_municipio']}")
    print(f"  - area_m2: {ejemplo['area_m2']:.2f}")

# Estadísticas por municipio
print("\n📊 Top 5 municipios con más edificios:")
pipeline = [
    {'$group': {
        '_id': '$codigo_municipio',
        'count': {'$sum': 1},
        'area_total': {'$sum': '$area_m2'}
    }},
    {'$sort': {'count': -1}},
    {'$limit': 5}
]

for doc in collection.aggregate(pipeline):
    print(f"  Municipio {doc['_id']}: {doc['count']:,} edificios, {doc['area_total']/10000:.2f} ha")

print("\n" + "="*60)
print("✓ CARGA COMPLETADA - SOLO EDIFICIOS EN PDET")
print("="*60)

client.close()

# Eliminar archivo para liberar espacio
if os.path.exists(GEOJSON_FILE):
    try:
        os.remove(GEOJSON_FILE)
        print(f"🗑️  Archivo eliminado: {GEOJSON_FILE}")
    except Exception as e:
        print(f"⚠️  No se pudo eliminar {GEOJSON_FILE}: {e}")
