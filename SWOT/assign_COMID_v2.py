import os
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.errors import GEOSException

import warnings
warnings.filterwarnings("ignore")

# =========================
# RUTAS (ajusta si hace falta)
# =========================
SWORD_PATH   = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_COMID_v1.shp"
CATCH_V2_ROOT = r"D:\GEOGloWS\SWOT_Intersection\v2"  # contiene subcarpetas 100, 200, 300, ...
OUTPUT_SWORD = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_COMIDs.shp"

# CRS métrico para medir longitudes de intersección
METRIC_CRS = "EPSG:3857"

# =========================
# UTILIDADES
# =========================
def vpu_hundreds(vpu: int) -> int:
    return (int(vpu) // 100) * 100

def path_v2_catch_for_vpu(vpu: int) -> str:
    """
    Devuelve: D:\GEOGloWS\SWOT_Intersection\v2\{hundreds}\catchments_vpu_{vpu}.shp
    """
    grp = vpu_hundreds(vpu)
    return os.path.join(CATCH_V2_ROOT, str(grp), f"catchments_vpu_{int(vpu)}.shp")

def ensure_same_crs(a: gpd.GeoDataFrame, b: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if a.crs is None or b.crs is None:
        raise ValueError("Alguna capa no tiene CRS definido. Asigna CRS antes de continuar.")
    if a.crs != b.crs:
        b = b.to_crs(a.crs)
    return a, b

def fix_invalid(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    try:
        bad = ~gdf.geometry.is_valid
        if bad.any():
            gdf.loc[bad, "geometry"] = gdf.loc[bad, "geometry"].buffer(0)
    except GEOSException:
        pass
    return gdf

def get_linkno_col(gdf: gpd.GeoDataFrame) -> str:
    # Acepta 'linkno' (insensible a mayúsculas)
    for c in gdf.columns:
        if c.lower() == "linkno":
            return c
    raise KeyError("No se encontró columna 'linkno' en el shapefile de subcuencas v2.")

def assign_comid_v2_for_vpu(riv_vpu: gpd.GeoDataFrame, catch_vpu: gpd.GeoDataFrame) -> pd.Series:
    """
    Devuelve una Serie indexada por riv_vpu.index con COMID_v2 asignado.
    Reglas:
      - 0 si no intersecta ninguna subcuenca
      - linkno si intersecta 1
      - linkno de mayor longitud de intersección si intersecta 2+
    """
    # Alinear CRS
    riv_vpu, catch_vpu = ensure_same_crs(riv_vpu, catch_vpu)

    # Normalizar geoms
    riv_vpu = riv_vpu[riv_vpu.geometry.notna()].copy()
    catch_vpu = fix_invalid(catch_vpu[catch_vpu.geometry.notna()].copy())

    link_col = get_linkno_col(catch_vpu)

    # Prefiltro espacial: parejas tramo-subcuenca que "intersects"
    left = riv_vpu[["geometry"]].reset_index().rename(columns={"index": "rid"})
    right = catch_vpu[[link_col, "geometry"]].rename(columns={link_col: "linkno"})
    try:
        pairs = gpd.sjoin(left, right, how="left", predicate="intersects")
    except TypeError:
        pairs = gpd.sjoin(left, right, how="left", op="intersects")

    # Mapa de salida (entero anulable para tolerar vacíos intermedios)
    comid_map = pd.Series(0, index=riv_vpu.index, dtype="Int64")

    # rids con al menos una candidata
    has_any = pairs[~pairs["linkno"].isna()]["rid"].unique()
    # Conteo por tramo
    counts = pairs.groupby("rid")["linkno"].count()
    one_rids   = counts[counts == 1].index.values
    multi_rids = counts[counts >= 2].index.values

    # Caso 1 candidata → usar linkno directamente
    if len(one_rids) > 0:
        one_rows = pairs[pairs["rid"].isin(one_rids)]
        # Serie por rid (numérica robusta)
        vals = pd.to_numeric(one_rows.set_index("rid")["linkno"], errors="coerce").fillna(0).astype("Int64")
        comid_map.loc[vals.index] = vals.values

    # Caso múltiples → elegir la mayor intersección por longitud
    if len(multi_rids) > 0:
        riv_m   = riv_vpu.to_crs(METRIC_CRS)
        catch_m = catch_vpu.to_crs(METRIC_CRS)

        riv_geom   = riv_m["geometry"].to_dict()      # rid -> LineString/MultiLineString
        catch_geom = catch_m["geometry"].to_dict()    # index_right -> Polygon/MultiPolygon

        cand = pairs[pairs["rid"].isin(multi_rids)][["rid", "linkno", "index_right"]].copy()

        lengths = []
        for rid, lno, cid in cand.itertuples(index=False):
            inter = riv_geom[rid].intersection(catch_geom[cid])
            lengths.append(inter.length if inter and not inter.is_empty else 0.0)
        cand["inter_len"] = lengths

        best = cand.sort_values(["rid", "inter_len"], ascending=[True, False]).groupby("rid", as_index=True).first()
        best_id = pd.to_numeric(best["linkno"], errors="coerce").fillna(0).astype("Int64")
        comid_map.loc[best_id.index] = best_id.values

    # Tramos sin candidatas explíticas (no aparecen en 'pairs' con linkno) ya quedan en 0
    return comid_map

# =========================
# MAIN
# =========================
def main():
    # 1) Leer SWORD con VPU
    rivers = gpd.read_file(SWORD_PATH)
    if rivers.crs is None:
        raise ValueError("El shapefile SWORD no tiene CRS definido.")
    if "VPU" not in rivers.columns:
        raise KeyError("El shapefile SWORD no tiene el campo 'VPU'.")

    # Normalizar VPU y columna de salida
    rivers["VPU"] = pd.to_numeric(rivers["VPU"], errors="coerce").fillna(0).astype(int)
    if "COMID_v2" in rivers.columns:
        rivers["COMID_v2"] = pd.to_numeric(rivers["COMID_v2"], errors="coerce").fillna(0).astype("Int64")
    else:
        rivers["COMID_v2"] = pd.Series(0, index=rivers.index, dtype="Int64")

    # 2) Asignar 0 directo a VPU==0 y excluirlos del procesamiento
    zero_mask = (rivers["VPU"] == 0)
    if zero_mask.any():
        rivers.loc[zero_mask, "COMID_v2"] = 0

    # VPUs válidos
    vpus = sorted(rivers.loc[~zero_mask, "VPU"].unique().tolist())
    print(f"VPUs a procesar (VPU>0): {len(vpus)}")

    # 3) Procesar por VPU
    for vpu in vpus:
        vpu = int(vpu)
        print(f"\n=== VPU {vpu} ===")

        shp_catch = path_v2_catch_for_vpu(vpu)
        if not os.path.exists(shp_catch):
            print(f"  • No existe shapefile v2 para VPU {vpu}: {shp_catch}. Tramos de este VPU quedarán con COMID_v2=0.")
            continue

        # Subconjunto de ríos para este VPU
        riv_vpu = rivers[rivers["VPU"] == vpu].copy()
        if riv_vpu.empty:
            continue

        # Leer subcuencas v2
        catch_vpu = gpd.read_file(shp_catch)
        if catch_vpu.empty:
            print("  • Shapefile de subcuencas vacío. Tramos de este VPU quedarán en 0.")
            continue

        # Asignar COMID_v2 para este VPU
        comid_series = assign_comid_v2_for_vpu(riv_vpu, catch_vpu)

        # Escribir de vuelta en el GeoDataFrame completo
        rivers.loc[comid_series.index, "COMID_v2"] = comid_series.values
        print(f"  • Asignados COMID_v2 para {len(comid_series)} tramos (VPU {vpu})")

    # 4) Guardar resultado
    # Shapefile limita tipos/tamaño de campo; 'COMID_v2' (<=10 chars) está OK.
    rivers.to_file(OUTPUT_SWORD, driver="ESRI Shapefile", encoding="utf-8")
    print(f"\nListo. Guardado:\n  {OUTPUT_SWORD}")

if __name__ == "__main__":
    main()
