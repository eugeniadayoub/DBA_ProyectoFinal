#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crear colección `mgn_municipios_pdet` en MongoDB a partir de
`MunicipiosPDET.xlsx` (dentro de /app) y la colección ya cargada `municipalities`.

Estructura de salida (por documento):
  _id ObjectId
  codigo_municipio: String (código DANE, 5 dígitos, ej. '05001')
  nombre_municipio: String
  departamento: String
  pdet: Boolean (True)
  geometry: GeoJSON (Polygon/MultiPolygon)

Uso (variables de entorno):
  MONGO_URI (default mongodb://mongo-upme:27017)
  DB_NAME (default dba_proyectofinal)
  INPUT_XLSX (default /app/MunicipiosPDET.xlsx)
  MUNI_COLL (colección de municipios; default municipalities)
  OUT_COLL (colección de salida; default mgn_municipios_pdet)
  NO_DROP (si '1', no borra la colección destino antes)

El script busca en el Excel una columna con el código DANE (int o string).
Si falta una librería (pandas/openpyxl) da instrucciones de instalación.
"""
import os
import sys
from pymongo import MongoClient


MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo-upme:27017")
DB_NAME = os.getenv("DB_NAME", "dba_proyectofinal")
INPUT_XLSX = os.getenv("INPUT_XLSX", "/app/MunicipiosPDET.xlsx")
MUNI_COLL = os.getenv("MUNI_COLL", "municipalities")
OUT_COLL = os.getenv("OUT_COLL", "mgn_municipios_pdet")
NO_DROP = os.getenv("NO_DROP", "0")


def read_excel_codes(path):
    try:
        import pandas as pd
    except Exception:
        print("ERROR: este script necesita 'pandas' y 'openpyxl'. Instala con: pip install pandas openpyxl")
        sys.exit(1)

    if not os.path.exists(path):
        print(f"ERROR: no se encontró el archivo Excel: {path}")
        sys.exit(1)

    df = pd.read_excel(path)
    cols = [c.lower() for c in df.columns]

    # buscar columna de código
    code_col = None
    for candidate in ['codigo', 'codigo_municipio', 'cod_dane', 'cod', 'codigo_dane', 'codigo_mpio', 'cod_mpio', 'codigo_municipio_dane']:
        if candidate in cols:
            code_col = df.columns[cols.index(candidate)]
            break

    # si no encuentra, intentar detectar columna numérica con valores 4-6 dígitos
    if code_col is None:
        for c, low in zip(df.columns, cols):
            ser = df[c].dropna()
            if ser.empty:
                continue
            sample = ser.astype(str).str.replace(r"\D", "", regex=True)
            lens = sample.str.len()
            if (lens.between(4, 6).mean() > 0.6):
                code_col = c
                break

    if code_col is None:
        print("ERROR: no se pudo identificar la columna de códigos en el Excel. Columnas encontradas:", list(df.columns))
        sys.exit(1)

    codes = []
    import re
    for v in df[code_col].dropna():
        s = str(v).strip()
        digits = re.sub(r"\D", "", s)
        if not digits:
            continue
        digits = digits.zfill(5)
        codes.append(digits)

    return set(codes)


def main():
    print("Creando colección mgn_municipios_pdet desde Excel de PDET (server-side)")
    print(f"Leyendo Excel: {INPUT_XLSX}")

    codes = read_excel_codes(INPUT_XLSX)
    codes_list = list(codes)
    print(f"Códigos PDET detectados en Excel: {len(codes_list)}")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    municol = db[MUNI_COLL]
    outcol = db[OUT_COLL]

    # Chequear índices y versión de servidor
    try:
        idxs = municol.index_information()
        has_geo = any('2dsphere' in str(v.get('key')) for v in idxs.values())
    except Exception:
        has_geo = False
    if not has_geo:
        print("Advertencia: la colección 'municipalities' no parece tener un índice 2dsphere sobre 'geometry'. Las consultas serán lentas.")

    try:
        server_ver = client.server_info().get('version', '0')
        major_ver = int(server_ver.split('.')[0])
    except Exception:
        major_ver = 0

    use_js_function = major_ver >= 4
    if use_js_function:
        print(f"Servidor MongoDB versión {server_ver}: se usará $function para extraer campos dinámicos.")
    else:
        print(f"Servidor MongoDB versión {server_ver}: $function no disponible, se usará proyección por claves conocidas.")

    # preparar pipeline: $match + $project + $out/$merge
    match_stage = { '$match': { 'cod_completo': { '$in': codes_list } } }

    if use_js_function:
        # función JS para extraer departamento a partir de properties
        dept_func = {
            '$function': {
                'body': (
                    "function(props){ if(!props) return ''; "
                    "for(var k in props){ if(!Object.prototype.hasOwnProperty.call(props,k)) continue; var kl=k.toLowerCase(); "
                    "if(kl.includes('dpto') && (kl.includes('nm') || kl.includes('nombre') || kl.includes('cnmbr'))) return props[k]; } return ''; }"
                ),
                'args': ["$properties"],
                'lang': 'js'
            }
        }

        proj_stage = {
            '$project': {
                'codigo_municipio': '$cod_completo',
                'nombre_municipio': '$nombre',
                'departamento': dept_func,
                'pdet': { '$literal': True },
                'geometry': '$geometry'
            }
        }
    else:
        # Fallback: probar claves comunes
        proj_stage = {
            '$project': {
                'codigo_municipio': '$cod_completo',
                'nombre_municipio': '$nombre',
                'departamento': {'$ifNull': ['$properties.DPTO_CNMBR', '$properties.dpto_cnmbr', '$properties.DPTO_CNM', '']},
                'pdet': { '$literal': True },
                'geometry': '$geometry'
            }
        }

    # elegir salida: $out (por defecto) o $merge cuando NO_DROP=1
    if NO_DROP != '1':
        final_stage = { '$out': OUT_COLL }
        print(f"Se sobrescribirá (OUT) la colección destino: {OUT_COLL}")
    else:
        final_stage = { '$merge': { 'into': OUT_COLL, 'whenMatched': 'keepExisting', 'whenNotMatched': 'insert' } }
        print(f"NO_DROP=1: se hará $merge hacia {OUT_COLL} (se preservarán documentos existentes cuando coincidan).")

    pipeline = [ match_stage, proj_stage, final_stage ]

    print("Ejecutando pipeline de agregación en el servidor MongoDB...")
    try:
        # Usar command para asegurar ejecución incluso con $out/$merge
        # El comando 'aggregate' requiere la opción 'cursor' si no se usa explain
        res = db.command('aggregate', MUNI_COLL, pipeline=pipeline, allowDiskUse=True, cursor={})
        print('Pipeline ejecutado correctamente.')
    except Exception as e:
        print('ERROR al ejecutar el pipeline en MongoDB:', e)
        client.close()
        sys.exit(1)

    # Crear índice 2dsphere en la colección destino
    try:
        print("Creando índice 2dsphere sobre 'geometry' en la colección destino...")
        outcol.create_index([('geometry', '2dsphere')])
    except Exception as e:
        print('Warning: no se pudo crear índice en la colección destino:', e)

    # Resumen simple
    try:
        total_out = outcol.count_documents({})
    except Exception:
        total_out = None
    print('Resumen:')
    print(f"  códigos solicitados: {len(codes_list)}")
    if total_out is not None:
        print(f"  documentos en {OUT_COLL}: {total_out}")

    client.close()


if __name__ == '__main__':
    main()
