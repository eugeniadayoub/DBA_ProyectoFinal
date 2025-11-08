from pymongo import MongoClient
import json
from datetime import datetime
from collections import Counter

# Configuración
MONGO_URI = 'mongodb://mongo-upme:27017/'
DB_NAME = 'proyecto_upme'

print("="*70)
print("ANÁLISIS EXPLORATORIO DE DATOS (EDA)")
print("Building Footprints: Google vs Microsoft")
print("="*70)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. Conectar a MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    google_col = db['google_footprints']
    microsoft_col = db['microsoft_footprints']
    municipios_col = db['mgn_municipios_pdet']
    print(f"✓ Conectado a MongoDB")
    print(f"  Base de datos: {DB_NAME}")
    print()
except Exception as e:
    print(f"✗ ERROR: No se pudo conectar a MongoDB.")
    print(f"  Detalle: {e}")
    exit(1)

# ============================================================================
# SECCIÓN 1: RESUMEN GENERAL
# ============================================================================
print("="*70)
print("1. RESUMEN GENERAL DE LAS COLECCIONES")
print("="*70)

# Contar documentos
google_count = google_col.count_documents({})
microsoft_count = microsoft_col.count_documents({})
municipios_count = municipios_col.count_documents({})

print(f"\n📊 Número de documentos por colección:")
print(f"  - Google Open Buildings:      {google_count:,} edificaciones")
print(f"  - Microsoft Building Footprints: {microsoft_count:,} edificaciones")
print(f"  - Municipios PDET:            {municipios_count:,} municipios")
print(f"  - Total footprints:           {google_count + microsoft_count:,} edificaciones")

# ============================================================================
# SECCIÓN 2: ESTRUCTURA DE DATOS
# ============================================================================
print("\n" + "="*70)
print("2. ESTRUCTURA Y CAMPOS DE LOS DATASETS")
print("="*70)

print("\n📋 Google Open Buildings - Campos disponibles:")
google_sample = google_col.find_one()
if google_sample:
    campos_google = list(google_sample.keys())
    for campo in campos_google:
        tipo = type(google_sample[campo]).__name__
        print(f"  - {campo}: {tipo}")
else:
    print("  ⚠ No hay datos en Google")

print("\n📋 Microsoft Building Footprints - Campos disponibles:")
microsoft_sample = microsoft_col.find_one()
if microsoft_sample:
    campos_microsoft = list(microsoft_sample.keys())
    for campo in campos_microsoft:
        tipo = type(microsoft_sample[campo]).__name__
        print(f"  - {campo}: {tipo}")
else:
    print("  ⚠ No hay datos en Microsoft")

# ============================================================================
# SECCIÓN 3: ANÁLISIS DE GOOGLE OPEN BUILDINGS
# ============================================================================
print("\n" + "="*70)
print("3. ANÁLISIS DETALLADO: GOOGLE OPEN BUILDINGS")
print("="*70)

# 3.1 Estadísticas de Área
print("\n📐 Estadísticas de Área (m²):")
pipeline_google_area = [
    {
        '$group': {
            '_id': None,
            'area_min': {'$min': '$area_in_meters'},
            'area_max': {'$max': '$area_in_meters'},
            'area_avg': {'$avg': '$area_in_meters'},
            'area_sum': {'$sum': '$area_in_meters'}
        }
    }
]

google_area_stats = list(google_col.aggregate(pipeline_google_area))
if google_area_stats:
    stats = google_area_stats[0]
    print(f"  Área mínima:      {stats['area_min']:.2f} m²")
    print(f"  Área máxima:      {stats['area_max']:.2f} m²")
    print(f"  Área promedio:    {stats['area_avg']:.2f} m²")
    print(f"  Área total:       {stats['area_sum']:.2f} m² ({stats['area_sum']/10000:.2f} hectáreas)")

# 3.2 Distribución de áreas por rangos
print("\n📊 Distribución de edificaciones por tamaño:")
pipeline_rangos = [
    {
        '$bucket': {
            'groupBy': '$area_in_meters',
            'boundaries': [0, 50, 100, 200, 500, 1000, 10000],
            'default': 'Muy grande',
            'output': {
                'count': {'$sum': 1},
                'avg_area': {'$avg': '$area_in_meters'}
            }
        }
    }
]

google_rangos = list(google_col.aggregate(pipeline_rangos))
for rango in google_rangos:
    rango_str = f"{rango['_id']}" if isinstance(rango['_id'], str) else f"{rango['_id']}+ m²"
    print(f"  {rango_str:15s}: {rango['count']:3d} edificaciones (avg: {rango['avg_area']:.2f} m²)")

# 3.3 Estadísticas de Confianza
print("\n🎯 Estadísticas de Confianza (Confidence):")
pipeline_google_conf = [
    {
        '$group': {
            '_id': None,
            'conf_min': {'$min': '$confidence'},
            'conf_max': {'$max': '$confidence'},
            'conf_avg': {'$avg': '$confidence'}
        }
    }
]

google_conf_stats = list(google_col.aggregate(pipeline_google_conf))
if google_conf_stats:
    stats = google_conf_stats[0]
    print(f"  Confianza mínima:   {stats['conf_min']:.4f}")
    print(f"  Confianza máxima:   {stats['conf_max']:.4f}")
    print(f"  Confianza promedio: {stats['conf_avg']:.4f}")

# 3.4 Distribución de confianza
print("\n📊 Distribución por nivel de confianza:")
pipeline_conf_dist = [
    {
        '$bucket': {
            'groupBy': '$confidence',
            'boundaries': [0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
            'default': 'Fuera de rango',
            'output': {
                'count': {'$sum': 1}
            }
        }
    }
]

conf_dist = list(google_col.aggregate(pipeline_conf_dist))
for rango in conf_dist:
    limite = rango['_id']
    print(f"  Confianza {limite:.1f}+: {rango['count']:3d} edificaciones ({rango['count']/google_count*100:.1f}%)")

# 3.5 Rangos geográficos
print("\n🌍 Rangos Geográficos (Coordenadas):")
pipeline_coords = [
    {
        '$group': {
            '_id': None,
            'lat_min': {'$min': '$latitude'},
            'lat_max': {'$max': '$latitude'},
            'lon_min': {'$min': '$longitude'},
            'lon_max': {'$max': '$longitude'}
        }
    }
]

coords_stats = list(google_col.aggregate(pipeline_coords))
if coords_stats:
    c = coords_stats[0]
    print(f"  Latitud:  {c['lat_min']:.6f}° a {c['lat_max']:.6f}°")
    print(f"  Longitud: {c['lon_min']:.6f}° a {c['lon_max']:.6f}°")
    print(f"  Extensión lat: {c['lat_max'] - c['lat_min']:.6f}° (~{(c['lat_max'] - c['lat_min']) * 111:.2f} km)")
    print(f"  Extensión lon: {c['lon_max'] - c['lon_min']:.6f}° (~{(c['lon_max'] - c['lon_min']) * 111:.2f} km)")

# ============================================================================
# SECCIÓN 4: ANÁLISIS DE MICROSOFT BUILDING FOOTPRINTS
# ============================================================================
print("\n" + "="*70)
print("4. ANÁLISIS DETALLADO: MICROSOFT BUILDING FOOTPRINTS")
print("="*70)

# 4.1 Tipo de geometrías
print("\n📐 Tipos de geometría:")
pipeline_geom_types = [
    {
        '$group': {
            '_id': '$geometry.type',
            'count': {'$sum': 1}
        }
    }
]

geom_types = list(microsoft_col.aggregate(pipeline_geom_types))
for geom in geom_types:
    print(f"  {geom['_id']:15s}: {geom['count']:3d} edificaciones")

# 4.2 Rangos geográficos (centroides)
print("\n🌍 Rangos Geográficos (Centroides calculados):")
pipeline_coords_ms = [
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

coords_stats_ms = list(microsoft_col.aggregate(pipeline_coords_ms))
if coords_stats_ms:
    c = coords_stats_ms[0]
    print(f"  Latitud:  {c['lat_min']:.6f}° a {c['lat_max']:.6f}°")
    print(f"  Longitud: {c['lon_min']:.6f}° a {c['lon_max']:.6f}°")
    print(f"  Extensión lat: {c['lat_max'] - c['lat_min']:.6f}° (~{(c['lat_max'] - c['lat_min']) * 111:.2f} km)")
    print(f"  Extensión lon: {c['lon_max'] - c['lon_min']:.6f}° (~{(c['lon_max'] - c['lon_min']) * 111:.2f} km)")

# 4.3 Complejidad de polígonos (número de vértices)
print("\n📊 Complejidad de los polígonos:")
pipeline_vertices = [
    {
        '$project': {
            'num_vertices': {
                '$size': {'$arrayElemAt': ['$geometry.coordinates', 0]}
            }
        }
    },
    {
        '$group': {
            '_id': None,
            'min_vertices': {'$min': '$num_vertices'},
            'max_vertices': {'$max': '$num_vertices'},
            'avg_vertices': {'$avg': '$num_vertices'}
        }
    }
]

vertices_stats = list(microsoft_col.aggregate(pipeline_vertices))
if vertices_stats:
    v = vertices_stats[0]
    print(f"  Vértices mínimos:  {v['min_vertices']}")
    print(f"  Vértices máximos:  {v['max_vertices']}")
    print(f"  Vértices promedio: {v['avg_vertices']:.2f}")

# ============================================================================
# SECCIÓN 5: COMPARACIÓN ENTRE DATASETS
# ============================================================================
print("\n" + "="*70)
print("5. COMPARACIÓN: GOOGLE vs MICROSOFT")
print("="*70)

print("\n📊 Diferencias principales:")
print(f"\n  Cantidad:")
print(f"    Google:     {google_count:3d} edificaciones")
print(f"    Microsoft:  {microsoft_count:3d} edificaciones")
print(f"    Diferencia: {abs(google_count - microsoft_count):3d} edificaciones")

print(f"\n  Tipo de geometría:")
print(f"    Google:     Point (centroide) + WKT Polygon")
print(f"    Microsoft:  Polygon (GeoJSON)")

print(f"\n  Metadatos:")
print(f"    Google:     Área, Confianza, Plus Code")
print(f"    Microsoft:  Solo geometría (sin metadatos)")

print(f"\n  Índices espaciales:")
google_indexes = list(google_col.list_indexes())
microsoft_indexes = list(microsoft_col.list_indexes())
print(f"    Google:     {len(google_indexes)} índices")
for idx in google_indexes:
    print(f"      - {idx['name']}: {idx.get('key', {})}")
print(f"    Microsoft:  {len(microsoft_indexes)} índices")
for idx in microsoft_indexes:
    print(f"      - {idx['name']}: {idx.get('key', {})}")

# ============================================================================
# SECCIÓN 6: CALIDAD DE DATOS
# ============================================================================
print("\n" + "="*70)
print("6. CALIDAD DE DATOS")
print("="*70)

print("\n🔍 Google Open Buildings:")
# Verificar nulos
google_nulls = {
    'latitude': google_col.count_documents({'latitude': None}),
    'longitude': google_col.count_documents({'longitude': None}),
    'area_in_meters': google_col.count_documents({'area_in_meters': None}),
    'confidence': google_col.count_documents({'confidence': None}),
    'geometry': google_col.count_documents({'geometry': None})
}

print(f"  Valores nulos/faltantes:")
for campo, count in google_nulls.items():
    porcentaje = (count / google_count * 100) if google_count > 0 else 0
    print(f"    {campo:20s}: {count:3d} ({porcentaje:.1f}%)")

# Verificar outliers en área
google_outliers = google_col.count_documents({'area_in_meters': {'$gt': 1000}})
print(f"\n  Outliers (área > 1000 m²): {google_outliers} ({google_outliers/google_count*100:.1f}%)")

print("\n🔍 Microsoft Building Footprints:")
# Verificar nulos
microsoft_nulls = {
    'centroid_latitude': microsoft_col.count_documents({'centroid_latitude': None}),
    'centroid_longitude': microsoft_col.count_documents({'centroid_longitude': None}),
    'geometry': microsoft_col.count_documents({'geometry': None})
}

print(f"  Valores nulos/faltantes:")
for campo, count in microsoft_nulls.items():
    porcentaje = (count / microsoft_count * 100) if microsoft_count > 0 else 0
    print(f"    {campo:20s}: {count:3d} ({porcentaje:.1f}%)")

# ============================================================================
# FINALIZACIÓN
# ============================================================================
print("\n" + "="*70)
print("✓ ANÁLISIS EXPLORATORIO COMPLETADO")
print("="*70)
print()

client.close()
