import fiona
import json
import sys

SHAPE = r"C:\temp\mgn\ADMINISTRATIVO\MGN_ADM_MPIO_GRAFICO.shp"

try:
    with fiona.open(SHAPE, "r") as src:
        print("=" * 60)
        print("INSPECCIÓN MGN DANE - MUNICIPIOS")
        print("=" * 60)
        print(f"CRS: {src.crs}")
        total = len(src)
        print(f"Total features: {total}")
        
        # Mostrar hasta 3 ejemplos
        sample = []
        for i, feat in enumerate(src):
            if i >= 3:
                break
            sample.append(feat)
        
        if sample:
            print("\n" + "=" * 60)
            print("CAMPOS (keys del primer feature):")
            print("=" * 60)
            keys = list(sample[0].get("properties", {}).keys())
            for k in keys:
                print(f"  - {k}")
            
            print("\n" + "=" * 60)
            print("EJEMPLOS DE DATOS (3 municipios):")
            print("=" * 60)
            for i, s in enumerate(sample, 1):
                props = s.get("properties", {})
                print(f"\n--- Municipio {i} ---")
                print(json.dumps(props, ensure_ascii=False, indent=2))
        else:
            print("ERROR: No se encontraron features en el shapefile")
            sys.exit(1)
            
except Exception as e:
    print(f"ERROR al leer shapefile: {e}")
    sys.exit(1)
