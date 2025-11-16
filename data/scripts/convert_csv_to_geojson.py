#!/usr/bin/env python3
"""
Convert Google Building Footprints CSV.GZ files to a single merged GeoJSON FeatureCollection.
Reads CSV.GZ files, extracts geometry (WKT or lat/lon), converts to GeoJSON Features,
and writes a single FeatureCollection. Deletes CSV.GZ files after successful conversion.

Usage:
  python3 convert_csv_to_geojson.py input1.csv.gz input2.csv.gz ... output.geojson
"""
import sys
import gzip
import csv
import json
from pathlib import Path


def wkt_to_geojson_coords(wkt_str):
    """Convert WKT POLYGON string to GeoJSON coordinates array.
    Example WKT: POLYGON((-68.123 4.567, -68.124 4.568, ...))
    or: POLYGON ((-68.123 4.567, -68.124 4.568, ...))
    """
    try:
        # Remove 'POLYGON' prefix and clean up
        wkt_clean = wkt_str.strip()
        if wkt_clean.upper().startswith('POLYGON'):
            wkt_clean = wkt_clean[7:].strip()  # Remove 'POLYGON'
        
        # Remove outer parentheses - handle both "(" and "(("
        wkt_clean = wkt_clean.strip()
        if wkt_clean.startswith('(('):
            wkt_clean = wkt_clean[2:-2]  # Remove "((" and "))"
        elif wkt_clean.startswith('('):
            wkt_clean = wkt_clean[1:-1]  # Remove "(" and ")"
        
        # Split by comma to get coordinate pairs
        pairs = wkt_clean.split(',')
        coords = []
        for pair in pairs:
            parts = pair.strip().split()
            if len(parts) >= 2:
                try:
                    lon = float(parts[0])
                    lat = float(parts[1])
                    coords.append([lon, lat])
                except ValueError:
                    continue
        
        if len(coords) < 3:  # A polygon needs at least 3 points
            return None
        
        return [coords]  # Polygon exterior ring
    except Exception as e:
        print(f"  ⚠ Error parsing WKT: {wkt_str[:50]}... - {e}")
        return None


def csv_row_to_feature(row, row_num):
    """Convert CSV row to GeoJSON Feature.
    Expected columns: geometry (WKT) or latitude/longitude, plus other properties.
    """
    try:
        # Try to find geometry column (WKT format)
        geom = None
        properties = {}
        
        for key, val in row.items():
            key_lower = key.lower()
            if key_lower in ('geometry', 'geom', 'wkt'):
                # Parse WKT
                coords = wkt_to_geojson_coords(val)
                if coords:
                    geom = {'type': 'Polygon', 'coordinates': coords}
            elif key_lower == 'latitude' and 'longitude' in [k.lower() for k in row.keys()]:
                # Point geometry from lat/lon
                lat = float(val)
                lon = float(row.get('longitude') or row.get('Longitude') or row.get('LONGITUDE'))
                # For buildings we expect polygons, but if only point available, skip or use point
                # For now, skip rows with only lat/lon (buildings should have polygons)
                continue
            else:
                properties[key] = val
        
        if geom is None:
            return None
        
        return {
            'type': 'Feature',
            'geometry': geom,
            'properties': properties
        }
    except Exception as e:
        print(f"  ⚠ Error en fila {row_num}: {e}")
        return None


def convert_csv_gz_to_geojson(input_paths, output_path):
    """Read multiple CSV.GZ files and write a single GeoJSON FeatureCollection."""
    output = Path(output_path)
    total_features = 0
    total_errors = 0
    
    print(f"▶ Convirtiendo {len(input_paths)} archivos CSV.GZ -> {output}")
    
    with output.open('w', encoding='utf-8') as fout:
        fout.write('{"type":"FeatureCollection","features":[\n')
        first = True
        
        for input_path in input_paths:
            inp = Path(input_path)
            if not inp.exists():
                print(f"  ⚠ Archivo no encontrado: {inp}")
                continue
            
            print(f"  Procesando: {inp.name}...")
            row_count = 0
            
            with gzip.open(inp, 'rt', encoding='utf-8') as fin:
                reader = csv.DictReader(fin)
                for row_num, row in enumerate(reader, start=1):
                    row_count = row_num
                    feature = csv_row_to_feature(row, row_num)
                    if feature:
                        if not first:
                            fout.write(',\n')
                        fout.write(json.dumps(feature, ensure_ascii=False))
                        first = False
                        total_features += 1
                    else:
                        total_errors += 1
                    
                    if row_num % 10000 == 0:
                        print(f"    Procesadas {row_num} filas...")
            
            print(f"    ✓ {inp.name}: {row_count} filas procesadas")
        
        fout.write('\n]}\n')
    
    print(f"✓ Conversión completada: {output}")
    print(f"  Features totales: {total_features}")
    print(f"  Errores/omitidos: {total_errors}")
    
    # Eliminar archivos CSV.GZ originales
    for input_path in input_paths:
        inp = Path(input_path)
        if inp.exists():
            try:
                inp.unlink()
                print(f"🗑️  Archivo original eliminado: {inp}")
            except Exception as e:
                print(f"⚠️  No se pudo eliminar {inp}: {e}")


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Uso: convert_csv_to_geojson.py input1.csv.gz [input2.csv.gz ...] output.geojson')
        sys.exit(2)
    
    inputs = sys.argv[1:-1]
    output = sys.argv[-1]
    convert_csv_gz_to_geojson(inputs, output)
