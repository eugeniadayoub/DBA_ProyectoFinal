#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carga municipios desde el shapefile oficial MGN de DANE (dentro del ZIP)
a MongoDB con índice 2dsphere.
"""
import os
import sys
import zipfile
import tempfile
from datetime import datetime
from pymongo import MongoClient, InsertOne
import fiona
from shapely.geometry import shape, mapping
from shapely.ops import transform
from pyproj import Transformer
from tqdm import tqdm

# Config
ZIP_PATH = os.getenv("MGN_ZIP_PATH", "/app/MGN2024_00_COLOMBIA.zip")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo-upme:27017")
DB_NAME = "dba_proyectofinal"
COL_NAME = "municipalities"
BATCH = 500
TARGET_SHP = "ADMINISTRATIVO/MGN_ADM_MPIO_GRAFICO.shp"

def to_wgs84(geom, src_crs):
    """Transforma geometría a WGS84 (EPSG:4326)"""
    if not src_crs:
        return geom
    try:
        transformer = Transformer.from_crs(src_crs, "EPSG:4326", always_xy=True)
        return transform(transformer.transform, geom)
    except Exception as e:
        print(f"Warning: no se pudo transformar CRS: {e}")
        return geom


def normalize_shapely_geom(geom):
    """Recibe una geometría Shapely y trata de garantizar que sea válida.
    Retorna la geometría corregida o None si no se puede arreglar.
    """
    try:
        if geom.is_valid:
            return geom
    except Exception:
        pass
    try:
        from shapely.ops import make_valid
        g = make_valid(geom)
        if g.is_valid:
            return g
    except Exception:
        try:
            g = geom.buffer(0)
            if g.is_valid:
                return g
        except Exception:
            return None
    try:
        # como último recurso intentar buffer(0)
        g = geom.buffer(0)
        if g.is_valid:
            return g
    except Exception:
        return None
    return None

def find_shapefile(tmpdir, target):
    """Busca el shapefile específico en el directorio extraído"""
    for root, _, files in os.walk(tmpdir):
        for f in files:
            full_path = os.path.join(root, f)
            if target in full_path.replace("\\", "/"):
                if f.lower().endswith(".shp"):
                    return full_path
    return None

def validate_mgn_fields(properties):
    """Intenta mapear/validar campos MGN DANE de forma tolerante a mayúsculas/minúsculas.

    Devuelve un diccionario con las claves reales en el shapefile para los campos
    esperados: cod_dpto_key, cod_mpio_key, nombre_key.
    Si no logra mapear alguno, termina con un error legible.
    """
    import re

    keys = list(properties.keys())
    keys_lower = [k.lower() for k in keys]

    def find_key(tokens):
        # busca una key cuyo nombre contenga todos los tokens (en minúsculas)
        for orig, low in zip(keys, keys_lower):
            if all(tok in low for tok in tokens):
                return orig
        return None

    mapping = {}
    # dpto code: suele contener 'dpto' y 'ccd' o 'ccdgo'
    mapping['cod_dpto_key'] = find_key(['dpto', 'ccd']) or find_key(['dpto', 'ccdgo']) or find_key(['dpto'])
    # mpio code: suele contener 'mpio' y 'ccd' o 'cdpmp'
    mapping['cod_mpio_key'] = find_key(['mpio', 'ccd']) or find_key(['mpio', 'cdp']) or find_key(['mpio'])
    # nombre municipio: suele contener 'mpio' y 'nmbr' o 'nombre' o 'cnmbr'
    mapping['nombre_key'] = find_key(['mpio', 'nmbr']) or find_key(['mpio', 'nombre']) or find_key(['mpio', 'cnmbr']) or find_key(['mpio'])

    missing = [k for k, v in mapping.items() if v is None]
    if missing:
        print(f"ERROR: No se pudieron identificar los campos MGN necesarios: {missing}")
        print("Keys encontradas (normalizadas):", keys_lower)
        print("Keys originales:", keys)
        sys.exit(1)

    print("Mapeo de campos MGN detectado:")
    for k, v in mapping.items():
        print(f"  {k} -> {v}")

    return mapping

def main():
    print("=" * 60)
    print("CARGA DE MUNICIPIOS MGN (DANE) A MONGODB")
    print("=" * 60)
    
    if not os.path.exists(ZIP_PATH):
        print(f"ERROR: No se encontró el ZIP: {ZIP_PATH}")
        sys.exit(1)
    
    print(f"Conectando a MongoDB: {MONGO_URI}")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[COL_NAME]
    
    # Drop colección anterior (opcional - comenta si quieres preservar)
    print(f"Limpiando colección anterior: {COL_NAME}")
    coll.drop()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Extrayendo ZIP: {ZIP_PATH}")
        with zipfile.ZipFile(ZIP_PATH, 'r') as z:
            z.extractall(tmpdir)
        
        shp = find_shapefile(tmpdir, TARGET_SHP)
        if not shp:
            print(f"ERROR: No se encontró {TARGET_SHP} en el ZIP")
            sys.exit(1)
        
        print(f"Shapefile encontrado: {shp}")
        
        ops = []
        total = 0
        
        with fiona.open(shp, 'r') as src:
            src_crs = src.crs_wkt or src.crs
            print(f"CRS original: {src_crs}")
            print(f"Total features en shapefile: {len(src)}")
            
            # Validar primer feature y obtener mapeo de campos
            first = next(iter(src), None)
            if first:
                field_map = validate_mgn_fields(first.get("properties", {}))
            else:
                print("ERROR: shapefile vacío")
                sys.exit(1)
            
            # Volver al inicio
            src.close()
            src = fiona.open(shp, 'r')
            
            for feat in tqdm(src, desc="Procesando municipios", total=len(src)):
                props = feat.get("properties", {})
                try:
                    geom = shape(feat["geometry"])
                except Exception as e:
                    print(f"Warning: geometría inválida, saltando: {e}")
                    continue
                
                # Transformar a WGS84
                geom = to_wgs84(geom, src_crs)
                
                # Crear código único concatenando dpto+municipio usando el mapeo detectado
                cod_dpto = str(props.get(field_map['cod_dpto_key'], "")).zfill(2)
                cod_mpio = str(props.get(field_map['cod_mpio_key'], "")).zfill(3)
                cod_completo = f"{cod_dpto}{cod_mpio}"
                
                doc = {
                    "cod_dpto": cod_dpto,
                    "cod_mpio": cod_mpio,
                    "cod_completo": cod_completo,
                    "nombre": props.get(field_map['nombre_key'], ""),
                    "properties": props,
                    # Normalizar geometría antes de insertar
                    # Si no se puede normalizar, se omite el registro
                    "geometry": None,
                    "ingest_date": datetime.utcnow(),
                    "source": "MGN_DANE_2024"
                }

                # intentar normalizar la geometría
                normalized_geom = normalize_shapely_geom(geom)
                if normalized_geom is None:
                    print(f"  ⚠ Geometría inválida no reparable para municipio {cod_completo}; se omite.")
                    continue
                doc['geometry'] = mapping(normalized_geom)

                ops.append(InsertOne(doc))
                
                if len(ops) >= BATCH:
                    coll.bulk_write(ops, ordered=False)
                    total += len(ops)
                    ops = []
            
            if ops:
                coll.bulk_write(ops, ordered=False)
                total += len(ops)
    
    # Crear índices
    print("Creando índices...")
    coll.create_index([("geometry", "2dsphere")])
    coll.create_index([("cod_completo", 1)])
    coll.create_index([("cod_dpto", 1)])
    coll.create_index([("nombre", 1)])
    
    print("="*60)
    print(f"✓ COMPLETADO: {total} municipios cargados en {DB_NAME}.{COL_NAME}")
    print("="*60)
    
    # Mostrar ejemplo
    ejemplo = coll.find_one({}, {"properties": 1, "cod_completo": 1, "nombre": 1})
    if ejemplo:
        print("\nEjemplo de documento insertado:")
        print(f"  Código: {ejemplo.get('cod_completo')}")
        print(f"  Nombre: {ejemplo.get('nombre')}")
        print(f"  Keys properties: {list(ejemplo.get('properties', {}).keys())[:10]}")
    
    client.close()
    
    # Eliminar archivo ZIP para liberar espacio
    if os.path.exists(ZIP_PATH):
        try:
            os.remove(ZIP_PATH)
            print(f"\n🗑️  Archivo eliminado para optimizar espacio: {ZIP_PATH}")
        except Exception as e:
            print(f"⚠️  No se pudo eliminar {ZIP_PATH}: {e}")

if __name__ == "__main__":
    main()
