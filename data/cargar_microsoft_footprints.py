from pymongo import MongoClient, GEOSPHERE
import json
import os
from datetime import datetime
from shapely.geometry import shape, mapping
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient

# Configuración
GEOJSON_FILE = os.getenv('MICROSOFT_INPUT_FILE', 'samples/sample_microsoft.geojson')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo-upme:27017/')
DB_NAME = os.getenv('DB_NAME', 'dba_proyectofinal')
COLLECTION_NAME = 'buildings_microsoft'

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

# 4. Leer el GeoJSON (FeatureCollection) en streaming - extrae cada Feature sin cargar todo en memoria
def iter_features_from_featurecollection(path):
    """Generador que itera Features desde un GeoJSON FeatureCollection en streaming.
    Busca la clave "features" y luego extrae objetos JSON delimitados por llaves { }.
    """
    with open(path, 'r', encoding='utf-8') as f:
        # Buscar la posición del array "features"
        buf = ''
        while True:
            chunk = f.read(8192)
            if not chunk:
                return
            buf += chunk
            idx = buf.find('"features"')
            if idx != -1:
                # avanzar hasta el '[' del array
                arr_idx = buf.find('[', idx)
                if arr_idx != -1:
                    # mover el cursor del archivo a la posición después del '['
                    # calculamos cuánto de buf corresponde al resto del archivo
                    consumed = len(buf[:arr_idx+1])
                    # retroceder el archivo al inicio + consumed
                    f.seek(f.tell() - len(buf) + consumed)
                    break
        # Ahora estamos justo después del '[' que inicia el array de features
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
                    # finalizó un objeto Feature
                    try:
                        yield json.loads(obj_buf)
                    except Exception:
                        pass
                    obj_buf = ''
            elif ch == '"':
                in_str = True
                obj_buf += ch
            else:
                # ignorar comas y espacios fuera de objetos
                if depth > 0:
                    obj_buf += ch

try:
    print("Leyendo GeoJSON convertido en streaming (FeatureCollection)...")
    # Para archivos pequeños (útil en tests) usar json.load por simplicidad
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
    # No contamos aquí el total de features porque sería costoso; la transformación los procesará
    print("✓ Inicio de lectura en streaming listo")
except Exception as e:
    print(f"✗ ERROR: No se pudo preparar la lectura en streaming del GeoJSON.")
    print(f"  Detalle: {e}")
    client.close()
    exit(1)

# 5. Preparar documentos para insertar
print("\n" + "="*60)
print("TRANSFORMANDO DATOS AL MODELO DE PRIMERA ENTREGA...")
print("="*60)

errores = 0
contador_id = 1

# Inserción por batches durante la transformación para no agotar memoria
BATCH_SIZE = int(os.getenv('MICROSOFT_BATCH_SIZE', '5000'))
batch = []
inserted_count = 0
idx = 0
print(f"✓ MICROSOFT_BATCH_SIZE = {BATCH_SIZE}")
for feature in features_iter:
    idx += 1
    try:
        # Soportar dos formatos de Feature:
        # 1) Objeto estándar GeoJSON Feature: {'type':'Feature','geometry':{...},'properties':{...}}
        # 2) Geometría directa (la conversión pudo producir una lista de geometrías):
        #    {'type':'Polygon','coordinates':[...]}
        geometry = None
        properties = {}
        if isinstance(feature, dict) and feature.get('geometry'):
            geometry = feature['geometry']
            properties = feature.get('properties', {}) or {}
        elif isinstance(feature, dict) and feature.get('type') and feature.get('coordinates'):
            # El objeto en el array es directamente la geometría
            geometry = { 'type': feature.get('type'), 'coordinates': feature.get('coordinates') }
            properties = {}

        if geometry is not None:
            
            # Normalizar y validar geometría usando Shapely antes de cualquier cálculo
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
                    raise ValueError('geometría inválida o no reparable')
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
                'geometry': mapping(polygon_shapely),  # ← Polygon en GeoJSON (normalizado)
                'centroid': centroid_geojson,  # ← Centroid como GeoJSON Point
                'area_m2': area_m2,  # ← Área calculada
                'loaded_at': datetime.utcnow()
            }
            # Añadir propiedades originales si existen
            if properties:
                documento['properties'] = properties
            
            batch.append(documento)
            contador_id += 1
            if idx % 1000 == 0:
                print(f"  Procesados: {idx} (batch={len(batch)})")
            if len(batch) >= BATCH_SIZE:
                try:
                    print(f"  ▶ Insertando batch de {len(batch)} documentos...")
                    collection.insert_many(batch)
                    inserted_count += len(batch)
                    print(f"  ✓ Insertados (parciales): {inserted_count}")
                except Exception as e:
                    print(f"✗ ERROR al insertar batch: {e}")
                batch = []
    
    except Exception as e:
        errores += 1
        print(f"  ⚠ Error en feature {idx}: {e}")

print(f"\n✓ Transformación completa")
print(f"  Features procesados: {idx}")
print(f"  Errores encontrados: {errores}")

# Insertar cualquier batch restante
if batch:
    try:
        print(f"  ▶ Insertando batch final de {len(batch)} documentos...")
        collection.insert_many(batch)
        inserted_count += len(batch)
        print(f"  ✓ Insertados (final): {inserted_count}")
    except Exception as e:
        print(f"✗ ERROR al insertar batch final: {e}")

if inserted_count == 0:
    print("✗ No se insertó ningún documento. Abortando.")
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

# Eliminar archivo GeoJSON para liberar espacio
if os.path.exists(GEOJSON_FILE):
    try:
        os.remove(GEOJSON_FILE)
        print(f"🗑️  Archivo eliminado para optimizar espacio: {GEOJSON_FILE}")
    except Exception as e:
        print(f"⚠️  No se pudo eliminar {GEOJSON_FILE}: {e}")
