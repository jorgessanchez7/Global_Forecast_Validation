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
SWORD_PATH   = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_region_and_vpu.shp"
CATCH_ROOT   = r"D:\GEOGloWS\SWOT_Intersection\v1"  # dentro habrá carpetas {region}\{region}-geoglows-swot-catchments.shp
OUTPUT_SWORD = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_COMID_v1.shp"

# CRS métrico para medir longitudes de intersección (genérico y simple)
METRIC_CRS = "EPSG:3857"

# =========================
# UTILIDADES
# =========================
def ensure_same_crs(a: gpd.GeoDataFrame, b: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if a.crs is None or b.crs is None:
        raise ValueError("Alguna capa no tiene CRS definido. Asignar CRS antes de continuar.")
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

def get_comid_col(gdf: gpd.GeoDataFrame) -> str:
    # Acepta COMID o DrainLnID (insensible a mayúsculas)
    candidates = {"comid", "drainlnid"}
    for c in gdf.columns:
        if c.lower() in candidates:
            return c
    raise KeyError("No se encontró columna 'COMID' ni 'DrainLnID' en el shapefile de subcuencas.")

def path_catch_for_region(region: str) -> str:
    # D:\GEOGloWS\SWOT_Intersection\v1\{region}\{region}-geoglows-swot-catchments.shp
    fn = f"{region}-geoglows-swot-catchments.shp"
    return os.path.join(CATCH_ROOT, str(region), fn)

def assign_comid_v1_for_region(riv_reg: gpd.GeoDataFrame, catch: gpd.GeoDataFrame) -> pd.Series:
    """
    Devuelve una Series indexada por riv_reg.index con el COMID_v1 asignado.
    Reglas:
      - 0 si no intersecta ninguna subcuenca
      - COMID si intersecta 1
      - COMID de mayor longitud de intersección si intersecta 2+
    """
    # Alinear CRS para el sjoin
    riv_reg, catch = ensure_same_crs(riv_reg, catch)

    # Prefiltro: parejas tramo-subcuenca que "intersects"
    try:
        pairs = gpd.sjoin(
            riv_reg[["geometry"]].reset_index().rename(columns={"index": "rid"}),
            catch[[get_comid_col(catch), "geometry"]].rename(columns={get_comid_col(catch): "COMID"}),
            how="left",
            predicate="intersects",
        )
    except TypeError:
        # para geopandas <0.10
        pairs = gpd.sjoin(
            riv_reg[["geometry"]].reset_index().rename(columns={"index": "rid"}),
            catch[[get_comid_col(catch), "geometry"]].rename(columns={get_comid_col(catch): "comid"}),
            how="left",
            op="intersects",
        )

    # rids con y sin candidatas
    has_any   = pairs[~pairs["COMID"].isna()]["rid"].unique()
    no_any    = np.setdiff1d(riv_reg.index.values, has_any)

    # Caso sencillo: exactamente una candidata → tomar COMID directamente
    counts = pairs.groupby("rid")["COMID"].count()
    one_cand_rids = counts[counts == 1].index.values
    multi_rids    = counts[counts >= 2].index.values

    comid_map = pd.Series(0, index=riv_reg.index, dtype="int64")  # default 0

    if len(one_cand_rids) > 0:
        one_rows = pairs[pairs["rid"].isin(one_cand_rids)]
        # como hay exactamente una, el COMID por 'rid' es único
        comid_one = one_rows.set_index("rid")["COMID"].astype("int64")
        comid_map.loc[comid_one.index] = comid_one.values

    if len(multi_rids) > 0:
        # Para los multi, calculamos longitudes de intersección y nos quedamos con el máximo por rid
        # Prepara geometrías en CRS métrico:
        riv_m   = riv_reg.to_crs(METRIC_CRS)
        catch_m = catch.to_crs(METRIC_CRS)

        # Trae geometrías por índice (más eficiente que fusionar toda la tabla)
        riv_geom_dict   = riv_m["geometry"].to_dict()
        catch_geom_dict = catch_m["geometry"].to_dict()

        # En pairs, 'index_right' es el índice de la subcuenca
        cand = pairs[pairs["rid"].isin(multi_rids)][["rid", "COMID", "index_right"]].copy()

        # Calcula longitudes de intersección
        lengths = []
        for rid, comid, cid in cand.itertuples(index=False):
            g_line = riv_geom_dict[rid]
            g_poly = catch_geom_dict[cid]
            inter  = g_line.intersection(g_poly)
            # inter puede ser LineString/MultiLineString/Point/GeometryCollection
            lengths.append(inter.length if inter and not inter.is_empty else 0.0)

        cand["inter_len"] = lengths

        # Elegir por rid la fila con mayor inter_len
        cand_sorted = cand.sort_values(["rid", "inter_len"], ascending=[True, False])
        best = cand_sorted.groupby("rid", as_index=True).first()  # toma la de mayor inter_len
        # Si la mayor longitud es 0 (solo punto/tocar borde), podrías decidir 0; pero la especificación dice
        # que si hay ≥2, elegir la mayor intersección; mantenemos esa lógica aunque sea 0.
        best["COMID"] = best["COMID"].astype("int64")
        comid_map.loc[best.index] = best["COMID"].values

    # rids en no_any se quedan con 0 (ya definido)
    return comid_map

# =========================
# MAIN
# =========================
def main():
    # 1) Leer SWORD
    rivers = gpd.read_file(SWORD_PATH)
    if rivers.crs is None:
        raise ValueError("El shapefile SWORD no tiene CRS definido.")
    if "region" not in rivers.columns:
        raise KeyError("El shapefile SWORD no tiene el campo 'region'.")

    # Prepara columna de salida
    if "COMID_v1" not in rivers.columns:
        rivers["COMID_v1"] = 0
    if "COMID_v1" in rivers.columns:
        rivers["COMID_v1"] = pd.to_numeric(rivers["COMID_v1"], errors="coerce").fillna(0).astype("Int64")
    else:
        rivers["COMID_v1"] = pd.Series(0, index=rivers.index, dtype="Int64")

    # 2) Procesar por región
    regions = sorted(rivers["region"].dropna().unique().tolist())
    print(f"Regiones únicas en SWORD: {len(regions)}")

    for reg in regions:
        print(f"\n=== Región {reg} ===")
        shp_catch = path_catch_for_region(reg)
        if not os.path.exists(shp_catch):
            print(f"  • No existe el shapefile de subcuencas para la región: {shp_catch}. Se asignará 0 a todos.")
            # Todos los de esta región quedan en 0
            continue

        # Subconjunto de ríos de la región
        riv_reg = rivers[rivers["region"] == reg].copy()
        if riv_reg.empty:
            continue

        # Leer subcuencas
        catch = gpd.read_file(shp_catch)
        if catch.empty:
            print("  • Shapefile de subcuencas vacío. Se asignará 0 a todos.")
            continue

        # Arreglos de saneamiento
        catch = fix_invalid(catch)
        riv_reg = riv_reg[riv_reg.geometry.notna()].copy()
        catch   = catch[catch.geometry.notna()].copy()

        # 3) Asignar COMID_v1 para la región
        comid_series = assign_comid_v1_for_region(riv_reg, catch)
        # Escribir los resultados en el GDF completo
        rivers.loc[comid_series.index, "COMID_v1"] = comid_series.values

        print(f"  • Asignados COMID_v1 para {len(comid_series)} tramos en región {reg}")

    # 4) Guardar resultado
    # Nota: Shapefile limita nombres de campos y tipos; 'COMID_v1' (<=10 chars) está OK.
    rivers.to_file(OUTPUT_SWORD, driver="ESRI Shapefile", encoding="utf-8")
    print(f"\nListo. Guardado:\n  {OUTPUT_SWORD}")

if __name__ == "__main__":
    main()
