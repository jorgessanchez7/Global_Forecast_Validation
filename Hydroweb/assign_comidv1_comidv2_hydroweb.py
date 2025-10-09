import os
import sys
import fiona
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import warnings
warnings.filterwarnings("ignore")

# =========================
# RUTAS Y NOMBRES DE CAMPOS
# =========================
CSV_PATH = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row.csv"

# v1
V1_BASE = r"D:\GEOGloWS\GEOGloWS_v1.0\Catchments"
V1_FOLDER_FMT = "{region}-geoglows-catchment"
V1_SHP_FMT = "{region}-geoglows-catchment.shp"
V1_ID_FIELDS = ["DrainLnID", "COMID"]  # intentará en este orden, insensible a mayúsculas

# v2
V2_BASE = r"D:\GEOGloWS\GEOGloWS_v2.0\Catchments"
V2_FOLDER_FMT = "vpu_{X}00_gpkg"
V2_GPKG_FMT = "catchments_{vpu}.gpkg"
V2_ID_FIELD = "linkno"  # insensible a mayúsculas

# columnas CSV de entrada
LAT_COL = "Latitude"
LON_COL = "Longitude"
REGION_COL = "Region"
VPU_COL = "VPU"

# salidas (si None, se infieren al lado del CSV)
OUT_CSV = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row.csv"
OUT_UNASSIGNED_CSV = None


def infer_outputs(csv_path, out_csv, out_unassigned_csv):
    base_dir = os.path.dirname(csv_path)
    base_name = os.path.splitext(os.path.basename(csv_path))[0]
    if out_csv is None:
        out_csv = os.path.join(base_dir, f"{base_name}_with_river_ids.csv")
    if out_unassigned_csv is None:
        out_unassigned_csv = os.path.join(base_dir, f"{base_name}_unassigned_river_ids.csv")
    return out_csv, out_unassigned_csv


def load_points(csv_path) -> gpd.GeoDataFrame:
    df = pd.read_csv(csv_path)
    missing = [c for c in [LAT_COL, LON_COL] if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el CSV: {missing}. Columnas: {list(df.columns)}")

    # coerces
    df[LAT_COL] = pd.to_numeric(df[LAT_COL], errors="coerce")
    df[LON_COL] = pd.to_numeric(df[LON_COL], errors="coerce")

    geom = [Point(xy) if pd.notnull(xy[0]) and pd.notnull(xy[1]) else None
            for xy in zip(df[LON_COL], df[LAT_COL])]
    gdf = gpd.GeoDataFrame(df, geometry=geom, crs="EPSG:4326")
    invalid = (~gdf.geometry.notna()).sum()
    if invalid > 0:
        print(f"ADVERTENCIA: {invalid} filas sin geometría válida (lat/lon nulos). Se conservarán como no asignadas.", file=sys.stderr)
    return gdf


def safe_to_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs("EPSG:4326")
    if gdf.crs.to_string().upper() in ("EPSG:4326", "WGS84", "OGC:CRS84"):
        return gdf
    return gdf.to_crs("EPSG:4326")


def fix_geoms(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        gdf = gdf[gdf.geometry.notna()].copy()
        gdf["geometry"] = gdf.geometry.buffer(0)
    return gdf


def pick_field_case_insensitive(gdf: gpd.GeoDataFrame, candidates):
    cols_lower = {c.lower(): c for c in gdf.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def v1_path_from_region(region_val: str):
    """
    Intenta varias variantes de 'region' para construir ruta.
    """
    if pd.isna(region_val) or str(region_val).strip() == "":
        return None

    # variantes por si hay diferencias de mayúsculas/guiones/bajos
    candidates = []
    raw = str(region_val).strip()
    candidates.append(raw)
    candidates.append(raw.lower())
    candidates.append(raw.replace(" ", "_"))
    candidates.append(raw.lower().replace(" ", "_"))
    candidates.append(raw.replace("-", "_"))
    candidates.append(raw.lower().replace("-", "_"))

    for reg in candidates:
        folder = os.path.join(V1_BASE, V1_FOLDER_FMT.format(region=reg))
        shp = os.path.join(folder, V1_SHP_FMT.format(region=reg))
        if os.path.isfile(shp):
            return shp
    return None


def v2_path_from_vpu(vpu_val):
    """
    Construye el camino al GPKG según VPU.
    X = VPU // 100 (entero)
    """
    try:
        vpu_int = int(float(vpu_val))
    except Exception:
        return None
    X = vpu_int // 100
    folder = os.path.join(V2_BASE, V2_FOLDER_FMT.format(X=X))
    gpkg = os.path.join(folder, V2_GPKG_FMT.format(vpu=vpu_int))
    if os.path.isfile(gpkg):
        return gpkg
    return None


def is_zero_like(value) -> bool:
    """
    Devuelve True si el valor representa cero (0, '0', '0.0', 0.0, etc.).
    """
    try:
        return float(value) == 0.0
    except Exception:
        return False


def assign_v1(points: gpd.GeoDataFrame) -> pd.Series:
    """
    Devuelve una Serie con river_id_v1 alineada al índice de points.
    Reglas:
      - region == 'no_covered' => river_id_v1 = 0
      - region vacío/NaN => None
    """
    out = pd.Series(index=points.index, dtype="object")
    # agrupar por region
    for region_val, idx in points.groupby(REGION_COL).groups.items():
        grp = points.loc[idx]

        # --- REGLA ESPECIAL v1: region == 'no_covered' ---
        if isinstance(region_val, str) and region_val.strip().lower() == "no_covered":
            out.loc[idx] = 0
            continue

        if pd.isna(region_val) or str(region_val).strip() == "":
            out.loc[idx] = None
            continue

        shp_path = v1_path_from_region(region_val)
        if not shp_path:
            print(f"ADVERTENCIA v1: no se encontró shapefile para region='{region_val}'", file=sys.stderr)
            out.loc[idx] = None
            continue

        try:
            poly = gpd.read_file(shp_path)
            poly = safe_to_wgs84(fix_geoms(poly))
            id_field = pick_field_case_insensitive(poly, V1_ID_FIELDS)
            if id_field is None:
                print(f"ADVERTENCIA v1: el shapefile '{shp_path}' no tiene {V1_ID_FIELDS}", file=sys.stderr)
                out.loc[idx] = None
                continue

            joined = gpd.sjoin(grp, poly[[id_field, "geometry"]], how="left", predicate="within")
            out.loc[joined.index] = joined[id_field].astype(object)

        except Exception as e:
            print(f"ERROR v1 leyendo/procesando '{shp_path}': {e}", file=sys.stderr)
            out.loc[idx] = None
    return out


def load_gpkg_best_layer(gpkg_path: str, want_field_lower: str):
    """
    Abre el GPKG intentando encontrar una capa que tenga el campo deseado (insensible a mayúsculas).
    """
    try:
        gdf_default = gpd.read_file(gpkg_path)
        fld = pick_field_case_insensitive(gdf_default, [want_field_lower])
        if fld:
            return safe_to_wgs84(fix_geoms(gdf_default)), fld
    except Exception:
        pass

    try:
        layers = fiona.listlayers(gpkg_path)
        for lyr in layers:
            try:
                gdf = gpd.read_file(gpkg_path, layer=lyr)
                fld = pick_field_case_insensitive(gdf, [want_field_lower])
                if fld:
                    return safe_to_wgs84(fix_geoms(gdf)), fld
            except Exception:
                continue
    except Exception as e:
        print(f"ADVERTENCIA v2: no se pudieron listar capas en '{gpkg_path}': {e}", file=sys.stderr)

    return None, None


def assign_v2(points: gpd.GeoDataFrame) -> pd.Series:
    """
    Devuelve una Serie con river_id_v2 alineada al índice de points.
    Reglas:
      - VPU == 0 => river_id_v2 = 0
      - VPU vacío/NaN => None
    """
    out = pd.Series(index=points.index, dtype="object")
    # agrupar por VPU
    for vpu_val, idx in points.groupby(VPU_COL).groups.items():
        grp = points.loc[idx]

        # --- REGLA ESPECIAL v2: VPU == 0 ---
        if is_zero_like(vpu_val):
            out.loc[idx] = 0
            continue

        if pd.isna(vpu_val) or str(vpu_val).strip() == "":
            out.loc[idx] = None
            continue

        gpkg_path = v2_path_from_vpu(vpu_val)
        if not gpkg_path:
            print(f"ADVERTENCIA v2: no se encontró gpkg para VPU='{vpu_val}'", file=sys.stderr)
            out.loc[idx] = None
            continue

        try:
            poly, id_field = load_gpkg_best_layer(gpkg_path, V2_ID_FIELD.lower())
            if poly is None or id_field is None:
                print(f"ADVERTENCIA v2: en '{gpkg_path}' no hay capa con campo '{V2_ID_FIELD}'", file=sys.stderr)
                out.loc[idx] = None
                continue

            joined = gpd.sjoin(grp, poly[[id_field, "geometry"]], how="left", predicate="within")
            out.loc[joined.index] = joined[id_field].astype(object)

        except Exception as e:
            print(f"ERROR v2 leyendo/procesando '{gpkg_path}': {e}", file=sys.stderr)
            out.loc[idx] = None
    return out


def main():
    out_csv, out_unassigned_csv = infer_outputs(CSV_PATH, OUT_CSV, OUT_UNASSIGNED_CSV)
    pts = load_points(CSV_PATH)

    # Asegurar existencia de columnas REGION_COL y VPU_COL (si no están, crearlas vacías)
    if REGION_COL not in pts.columns:
        pts[REGION_COL] = None
        print(f"ADVERTENCIA: columna '{REGION_COL}' no existe en el CSV; se creará vacía.", file=sys.stderr)
    if VPU_COL not in pts.columns:
        pts[VPU_COL] = None
        print(f"ADVERTENCIA: columna '{VPU_COL}' no existe en el CSV; se creará vacía.", file=sys.stderr)

    # Asignaciones
    print("Asignando river_id_v1 (por region)...")
    pts["river_id_v1"] = assign_v1(pts)

    print("Asignando river_id_v2 (por VPU)...")
    pts["river_id_v2"] = assign_v2(pts)

    # Exportar resultados
    out_all = pts.drop(columns=["geometry"], errors="ignore").copy()
    out_all.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # No asignados (si falta v1 o v2). OJO: 0 se considera ASIGNADO.
    unassigned_mask = out_all["river_id_v1"].isna() | out_all["river_id_v2"].isna()
    out_unassigned = out_all.loc[unassigned_mask].copy()

    # Añade un motivo básico
    reasons = []
    for i, row in out_unassigned.iterrows():
        reason = []
        if pd.isna(row.get("river_id_v1")):
            if pd.isna(row.get(REGION_COL)) or str(row.get(REGION_COL)).strip() == "":
                reason.append("v1:sin_region")
            else:
                reason.append("v1:no_hit_or_missing_shp")
        if pd.isna(row.get("river_id_v2")):
            if pd.isna(row.get(VPU_COL)) or str(row.get(VPU_COL)).strip() == "":
                reason.append("v2:sin_vpu")
            else:
                reason.append("v2:no_hit_or_missing_gpkg")
        reasons.append(";".join(reason) if reason else "")
    out_unassigned["unassigned_reason"] = reasons

    out_unassigned.to_csv(out_unassigned_csv, index=False, encoding="utf-8-sig")

    # Reporte
    total = len(out_all)
    v1_assigned = out_all["river_id_v1"].notna().sum()
    v2_assigned = out_all["river_id_v2"].notna().sum()
    any_unassigned = len(out_unassigned)

    print("==== RESUMEN ====")
    print(f"Total estaciones: {total}")
    print(f"v1 asignadas (incluye 0 por 'no_covered'): {v1_assigned}")
    print(f"v2 asignadas (incluye 0 por VPU=0): {v2_assigned}")
    print(f"Filas con faltante (v1 o v2): {any_unassigned}")
    print(f"CSV completo: {out_csv}")
    print(f"CSV no asignados: {out_unassigned_csv}")


if __name__ == "__main__":
    main()
