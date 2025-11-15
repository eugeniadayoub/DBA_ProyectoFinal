#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crear colección `mgn_municipios_pdet` en MongoDB a partir de
`data/MunicipiosPDET.xlsx` y la colección ya cargada `municipalities`.

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
  INPUT_XLSX (default data/MunicipiosPDET.xlsx)
  MUNI_COLL (colección de municipios; default municipalities)
  OUT_COLL (colección de salida; default mgn_municipios_pdet)
  NO_DROP (si '1', no borra la colección destino antes)
  BATCH (batch size para operaciones, default 500)

El script busca en el Excel una columna con el código DANE (int o string).
Si falta una librería (pandas/openpyxl) da instrucciones de instalación.
"""
import os
import sys
from pymongo import MongoClient
from tqdm import tqdm


MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo-upme:27017")
DB_NAME = os.getenv("DB_NAME", "dba_proyectofinal")
INPUT_XLSX = os.getenv("INPUT_XLSX", "/app/data/MunicipiosPDET.xlsx")
MUNI_COLL = os.getenv("MUNI_COLL", "municipalities")
OUT_COLL = os.getenv("OUT_COLL", "mgn_municipios_pdet")
NO_DROP = os.getenv("NO_DROP", "0")
BATCH = int(os.getenv("BATCH", "500"))


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
            # obtener original name
            code_col = df.columns[cols.index(candidate)]
            break

    # si no encuentra, intentar detectar columna numérica con valores 4-6 dígitos
    if code_col is None:
        for c, low in zip(df.columns, cols):
            # heurística: valores enteros y muchos con longitud entre 4 y 6
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
    for v in df[code_col].dropna():
        s = str(v).strip()
        # extraer dígitos
        import re
        digits = re.sub(r"\D", "", s)
        if not digits:
            continue
        # normalizar a 5 dígitos
        digits = digits.zfill(5)
        codes.append(digits)

    return set(codes)


def main():
    print("Creando colección mgn_municipios_pdet desde Excel de PDET")
    print(f"Leyendo Excel: {INPUT_XLSX}")

    codes = read_excel_codes(INPUT_XLSX)
    print(f"Códigos PDET detectados en Excel: {len(codes)}")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    municol = db[MUNI_COLL]
    outcol = db[OUT_COLL]

    # opcional: borrar destino salvo que NO_DROP == '1'
    if NO_DROP != '1':
        print(f"Limpiando colección destino: {OUT_COLL}")
        outcol.drop()
    else:
        print("NO_DROP activado: no se borrará la colección destino")

    # chequear índice 2dsphere en municipalities
    try:
        idxs = municol.index_information()
        has_geo = any('2dsphere' in str(v.get('key')) for v in idxs.values())
    except Exception:
        has_geo = False
    if not has_geo:
        print("Advertencia: la colección 'municipalities' no parece tener un índice 2dsphere sobre 'geometry'. Las consultas geoespaciales serán lentas o fallarán.")

    matched = 0
    missing = []

    # buscar municipios con cod_completo en codes
    cursor = municol.find({ 'cod_completo': { '$in': list(codes) } }, {'cod_completo':1, 'nombre':1, 'geometry':1, 'properties':1})
    ops = []
    for doc in cursor:
        codigo = doc.get('cod_completo')
        nombre = doc.get('nombre') or (doc.get('properties') or {}).get('MPIO_CNMBR') or (doc.get('properties') or {}).get('mpio_cnmbr') or ''
        # obtener departamento desde properties si existe
        props = doc.get('properties') or {}
        departamento = props.get('DPTO_CNMBR') or props.get('dpto_cnmbr') or ''
        # fallback: intentar extraer dpto nombre de otra key
        if not departamento:
            for k in props.keys():
                if 'dpto' in k.lower() and ('nm' in k.lower() or 'nombre' in k.lower()):
                    departamento = props.get(k)
                    break

        newdoc = {
            'codigo_municipio': codigo,
            'nombre_municipio': nombre,
            'departamento': departamento,
            'pdet': True,
            'geometry': doc.get('geometry')
        }
        ops.append(newdoc)

    matched = len(ops)
    # find missing codes
    found_codes = {d['codigo_municipio'] for d in ops}
    missing = sorted(list(codes - found_codes))

    if ops:
        print(f"Insertando {len(ops)} documentos en {OUT_COLL} ...")
        outcol.insert_many(ops)

    # crear índice 2dsphere
    print("Creando índice 2dsphere sobre 'geometry' en la colección destino...")
    outcol.create_index([('geometry', '2dsphere')])

    print("Resumen:")
    print(f"  códigos solicitados: {len(codes)}")
    print(f"  encontrados e insertados: {matched}")
    print(f"  no encontrados: {len(missing)}")
    if missing:
        print("  Ejemplos de códigos no encontrados:", missing[:10])

    client.close()


if __name__ == '__main__':
    main()
