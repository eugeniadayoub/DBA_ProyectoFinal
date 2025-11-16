#!/usr/bin/env python3
import sys
import json

def iter_features_from_featurecollection(path):
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

def extract(input_path, output_path, n):
    n = int(n)
    out = {'type': 'FeatureCollection', 'features': []}
    count = 0
    for feat in iter_features_from_featurecollection(input_path):
        out['features'].append(feat)
        count += 1
        if count >= n:
            break
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)
    print(f'Extracted {count} features to {output_path}')

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Usage: extract_features.py input.geojson output.geojson N')
        sys.exit(2)
    extract(sys.argv[1], sys.argv[2], sys.argv[3])
