# Requisitos:
#   Opción rápida (recomendado): pip install pyogrio geopandas
#   Fallback: pip install geopandas fiona shapely
import gc
import os
import sys
import glob
import argparse
import sqlite3
import geopandas as gpd

try:
    import pyogrio
    HAS_PYOGRIO = True
except Exception:
    HAS_PYOGRIO = False

from fiona import listlayers as fiona_listlayers

# Mapea carpetas de entrada -> salida
DIRS = [
    (r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_100_spatialite", r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_100_gpkg"),
    (r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_200_spatialite", r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_200_gpkg"),
    (r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_300_spatialite", r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_300_gpkg"),
    (r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_400_spatialite", r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_400_gpkg"),
    (r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_500_spatialite", r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_500_gpkg"),
    (r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_600_spatialite", r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_600_gpkg"),
    (r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_700_spatialite", r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_700_gpkg"),
    (r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_800_spatialite", r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments\vpu_800_gpkg"),
]

def vpu_group(vpu_int: int) -> int:
    return (vpu_int // 100) * 100

def list_layers(sp_path):
    """Lista nombres de capas dentro del .spatialite."""
    try:
        if HAS_PYOGRIO:
            # [(name, gtype, count, fields, crs), ...]
            return [rec[0] for rec in pyogrio.list_layers(sp_path)]
        else:
            return list(fiona_listlayers(sp_path))
    except Exception as e:
        print(f"   ⚠️ No se pudieron listar capas en {sp_path}: {e}")
        return []

def check_spatialite_health(sp_path: str) -> bool:
    try:
        con = sqlite3.connect(sp_path)
        cur = con.cursor()
        cur.execute("PRAGMA integrity_check;")
        res = cur.fetchone()
        con.close()
        ok = (res and res[0] == "ok")
        if not ok:
            print(f"   ❌ integrity_check: {res[0] if res else 'sin resultado'}")
        return ok
    except Exception as e:
        print(f"   ❌ No se pudo abrir {sp_path} como SQLite: {e}")
        return False

def convert_spatialite_to_gpkg(sp_path, gpkg_path):
    """Convierte todas las capas del .spatialite a un .gpkg (mismo nombre de capas)."""
    layers = list_layers(sp_path)
    if not layers:
        print(f"   ⚠️ {os.path.basename(sp_path)}: sin capas detectadas, se omite.")
        return

    # Si ya existe el .gpkg, lo borramos para evitar conflictos de capas antiguas
    if os.path.exists(gpkg_path):
        try:
            os.remove(gpkg_path)
        except Exception as e:
            print(f"   ⚠️ No se pudo eliminar {gpkg_path}: {e}")
            return

    for i, layer in enumerate(layers, 1):
        try:
            if HAS_PYOGRIO:
                # Lectura y escritura rápidas con pyogrio
                df = pyogrio.read_dataframe(sp_path, layer=layer)
                # append=False para la primera capa, luego append=True
                pyogrio.write_dataframe(
                    df, gpkg_path, layer=layer, driver="GPKG",
                    append=(i > 1)
                )
            else:
                # Fallback con GeoPandas/Fiona
                gdf = gpd.read_file(sp_path, layer=layer)
                # GeoPandas crea/cuela capas en GPKG automáticamente si cambias "layer"
                gdf.to_file(gpkg_path, layer=layer, driver="GPKG")
            print(f"      ✓ Capa '{layer}' convertida.")
        except Exception as e:
            print(f"      ⚠️ Error convirtiendo capa '{layer}' desde {os.path.basename(sp_path)}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Convertir .spatialite -> .gpkg (una o varias).")
    parser.add_argument("--one", type=str, help="Ruta a un .spatialite específico a convertir.")
    parser.add_argument("--vpu", type=int, help="Número de VPU (p.ej. 105) para convertir solo ese archivo.")
    args = parser.parse_args()

    total_files = 0
    converted = 0

    if args.one:
        sp_path = args.one
        if not os.path.exists(sp_path):
            print(f"❌ No existe: {sp_path}")
            sys.exit(1)
        # deducir carpeta de salida según centenas
        try:
            base = os.path.splitext(os.path.basename(sp_path))[0]  # catchments_105
            vpu_num = int(base.split("_")[-1])
            group = vpu_group(vpu_num)
        except Exception:
            print("⚠️ No se pudo deducir VPU desde el nombre. Se usará carpeta 100 por defecto.")
            group = 100
        out_dir = os.path.join(os.path.dirname(os.path.dirname(sp_path)), f"vpu_{group}_gpkg")
        os.makedirs(out_dir, exist_ok=True)
        gpkg_path = os.path.join(out_dir, f"{os.path.splitext(os.path.basename(sp_path))[0]}.gpkg")
        print(f"\n📄 Único archivo: {sp_path}\n   → {gpkg_path}")
        convert_spatialite_to_gpkg(sp_path, gpkg_path)
        return

    if args.vpu:
        vpu_num = args.vpu
        group = vpu_group(vpu_num)
        in_dir = None
        out_dir = None
        # localizar pares in/out en DIRS por grupo
        for _in, _out in DIRS:
            if _in.endswith(f"vpu_{group}_spatialite"):
                in_dir = _in
                out_dir = _out
                break
        if not in_dir:
            print(f"❌ No se encontró carpeta de entrada para el grupo {group}.")
            sys.exit(1)
        os.makedirs(out_dir, exist_ok=True)
        sp_path = os.path.join(in_dir, f"catchments_{vpu_num}.spatialite")
        if not os.path.exists(sp_path):
            print(f"❌ No existe: {sp_path}")
            sys.exit(1)
        gpkg_path = os.path.join(out_dir, f"catchments_{vpu_num}.gpkg")
        print(f"\n📄 VPU {vpu_num}: {sp_path}\n   → {gpkg_path}")
        convert_spatialite_to_gpkg(sp_path, gpkg_path)
        return


    for in_dir, out_dir in DIRS:
        if not os.path.isdir(in_dir):
            print(f"⚠️ Carpeta de entrada no existe: {in_dir}")
            continue
        os.makedirs(out_dir, exist_ok=True)

        sp_files = glob.glob(os.path.join(in_dir, "*.spatialite"))
        print(f"\n📂 {in_dir} -> {out_dir}  |  {len(sp_files)} archivos .spatialite encontrados")
        for sp_path in sp_files:
            total_files += 1
            base = os.path.splitext(os.path.basename(sp_path))[0]
            gpkg_path = os.path.join(out_dir, f"{base}.gpkg")
            print(f"   → Convirtiendo {os.path.basename(sp_path)} → {os.path.basename(gpkg_path)}")
            convert_spatialite_to_gpkg(sp_path, gpkg_path)
            converted += 1
            gc.collect()

    print(f"\n✅ Listo. Procesados: {converted}/{total_files} archivos .spatialite.")

if __name__ == "__main__":
    main()
