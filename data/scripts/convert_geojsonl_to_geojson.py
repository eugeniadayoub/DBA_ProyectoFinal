#!/usr/bin/env python3
import sys
import json
from pathlib import Path


def convert(input_path, output_path):
    inp = Path(input_path)
    out = Path(output_path)

    if not inp.exists():
        print(f"✗ ERROR: archivo de entrada no encontrado: {inp}")
        sys.exit(2)

    print(f"▶ Convirtiendo {inp} -> {out} (streaming)")

    with inp.open('r', encoding='utf-8') as fin, out.open('w', encoding='utf-8') as fout:
        fout.write('{"type":"FeatureCollection","features":[\n')
        first = True
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                # ignorar líneas no JSON
                continue

            # si la línea es un FeatureCollection, iterar sus features
            if isinstance(obj, dict) and 'features' in obj and isinstance(obj['features'], list):
                for feat in obj['features']:
                    if not first:
                        fout.write(',\n')
                    fout.write(json.dumps(feat, ensure_ascii=False))
                    first = False
            else:
                if not first:
                    fout.write(',\n')
                fout.write(json.dumps(obj, ensure_ascii=False))
                first = False

        fout.write('\n]}\n')

    print(f"✓ Conversión completada: {out}")
    
    # Eliminar archivo .geojsonl original para liberar espacio
    try:
        inp.unlink()
        print(f"🗑️  Archivo original eliminado: {inp}")
    except Exception as e:
        print(f"⚠️  No se pudo eliminar {inp}: {e}")


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Uso: convert_geojsonl_to_geojson.py input.geojsonl output.geojson')
        sys.exit(2)
    convert(sys.argv[1], sys.argv[2])
