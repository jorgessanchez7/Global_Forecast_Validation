import os
import re
import fiona
import geopandas as gpd
from shapely.errors import GEOSException


import warnings
warnings.filterwarnings("ignore")

# =========================
# RUTAS (ajusta si hace falta)
# =========================
CATCHMENTS_ROOT = r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments"   # contiene vpu_100_gpkg, vpu_200_gpkg, ...
SWORD_PATH = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_region_and_vpu.shp"
OUTPUT_ROOT = r"D:\GEOGloWS\SWOT_Intersection\v2"

# =========================
# UTILIDADES
# =========================
def vpu_hundreds(vpu: int) -> int:
    return (int(vpu) // 100) * 100

def group_folder_for_vpu(vpu: int) -> str:
    return os.path.join(CATCHMENTS_ROOT, f"vpu_{vpu_hundreds(vpu)}_gpkg")

def find_gpkg_and_layer_for_vpu(vpu: int, group_dir: str):
    """
    Busca el .gpkg y la capa que contienen las subcuencas del VPU dado.
    Heurísticas:
      1) archivos cuyo nombre contenga el VPU (como número “palabra”)
      2) si hay 1 solo gpkg en la carpeta, usar ese y buscar una capa que contenga el VPU
      3) si varias capas: prioriza las que contengan 'catch'/'basin'/'sub' + VPU; si no, toma la primera
    """
    if not os.path.isdir(group_dir):
        raise FileNotFoundError(f"No existe carpeta: {group_dir}")

    gpkg_files = [os.path.join(group_dir, fn) for fn in os.listdir(group_dir) if fn.lower().endswith(".gpkg")]
    if not gpkg_files:
        raise FileNotFoundError(f"No hay .gpkg en {group_dir}")

    pat = re.compile(rf"(^|[^0-9]){int(vpu)}([^0-9]|$)")
    candidates = [p for p in gpkg_files if pat.search(os.path.basename(p))]
    search_files = candidates if candidates else gpkg_files

    for gpkg in search_files:
        try:
            layers = fiona.listlayers(gpkg)
        except Exception:
            continue

        preferred = []
        for lyr in layers:
            name_low = lyr.lower()
            if str(vpu) in name_low and ("catch" in name_low or "basin" in name_low or "sub" in name_low):
                preferred.append(lyr)
        if preferred:
            return gpkg, preferred[0]

        generic = [lyr for lyr in layers if ("catch" in lyr.lower() or "basin" in lyr.lower() or "sub" in lyr.lower())]
        if generic:
            return gpkg, generic[0]

        if len(layers) == 1:
            return gpkg, layers[0]

    gpkg = gpkg_files[0]
    layers = fiona.listlayers(gpkg)
    return gpkg, layers[0]

def fix_invalid_polygons(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Arregla geom inválidas solo donde haga falta (buffer(0))."""
    try:
        bad = ~gdf.geometry.is_valid
        if bad.any():
            gdf.loc[bad, "geometry"] = gdf.loc[bad, "geometry"].buffer(0)
    except GEOSException:
        pass
    return gdf

def ensure_same_crs(a: gpd.GeoDataFrame, b: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if a.crs != b.crs:
        b = b.to_crs(a.crs)
    return a, b

# =========================
# MAIN
# =========================
def main():
    # 1) Cargar SWORD y preparar VPU
    rivers = gpd.read_file(SWORD_PATH)
    if "VPU" not in rivers.columns:
        raise KeyError("El shapefile de SWORD no tiene el campo 'VPU'.")

    # Convertir a entero (maneja strings/NaN); VPU==0 = “sin VPU asignado”
    rivers["VPU"] = (
        gpd.pd.to_numeric(rivers["VPU"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    vpu_zero_count = int((rivers["VPU"] == 0).sum())
    if vpu_zero_count > 0:
        print(f"Aviso: {vpu_zero_count} tramos con VPU==0 (sin VPU asignado). Se ignorarán.")

    # Filtrar solo VPU > 0 para el procesamiento
    rivers = rivers[rivers["VPU"] > 0].copy()
    if rivers.empty:
        raise RuntimeError("No quedan tramos con VPU>0 para procesar.")

    vpus = sorted(rivers["VPU"].unique().tolist())
    print(f"VPUs únicos en SWORD (excluyendo 0): {len(vpus)} -> {vpus[:10]}{'...' if len(vpus)>10 else ''}")

    # 2) Procesar VPU por VPU
    for vpu in vpus:
        vpu_int = int(vpu)
        print(f"\n=== VPU {vpu_int} ===")
        group_dir = group_folder_for_vpu(vpu_int)
        out_dir = os.path.join(OUTPUT_ROOT, str(vpu_hundreds(vpu_int)))
        os.makedirs(out_dir, exist_ok=True)
        out_shp = os.path.join(out_dir, f"catchments_vpu_{vpu_int}.shp")
        if os.path.exists(out_shp):
            print(f"   • Ya existe: {out_shp} (saltando)")
            continue

        # Ríos del VPU (ya VPU>0)
        rivers_vpu = rivers[rivers["VPU"] == vpu_int].copy()
        if rivers_vpu.empty:
            print("   • Sin ríos en SWORD para este VPU. Saltando.")
            continue

        # 3) Localizar GPKG/capa de subcuencas
        gpkg_path, layer_name = find_gpkg_and_layer_for_vpu(vpu_int, group_dir)
        print(f"   • GPKG: {gpkg_path} | capa: {layer_name}")

        # 4) Leer subcuencas y preparar
        catch = gpd.read_file(gpkg_path, layer=layer_name)
        if catch.empty:
            print("   • GPKG sin subcuencas. Saltando.")
            continue

        catch, rivers_vpu = ensure_same_crs(catch, rivers_vpu)
        catch = fix_invalid_polygons(catch)
        rivers_vpu = rivers_vpu[rivers_vpu.geometry.notna()].copy()

        # 5) Intersección espacial (subcuencas que tocan ríos)
        try:
            joined = gpd.sjoin(catch, rivers_vpu[["geometry"]], how="inner", predicate="intersects")
        except TypeError:
            joined = gpd.sjoin(catch, rivers_vpu[["geometry"]], how="inner", op="intersects")

        if joined.empty:
            print("   • No hay intersección (ninguna subcuenca toca la red).")
            continue

        subcatch_ix = joined.index.unique()
        subcatch = catch.loc[subcatch_ix].copy()

        # 6) Guardar shapefile
        subcatch.to_file(out_shp, driver="ESRI Shapefile", encoding="utf-8")
        print(f"   • Guardado: {out_shp} ({len(subcatch)} subcuencas)")

if __name__ == "__main__":
    main()
