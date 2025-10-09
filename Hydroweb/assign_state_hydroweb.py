# Requisitos: pip install geopandas shapely fiona unidecode
import os
import sys
import glob
import pandas as pd
import geopandas as gpd
from unidecode import unidecode
from shapely.geometry import Point

import warnings
warnings.filterwarnings("ignore")

# ================
# CONFIGURA RUTAS
# ================
CSV_PATH = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row.csv"
BASE_COUNTRIES_DIR = r"D:\Countries_SHP"

# Nombres de columnas del CSV de entrada
LAT_COL = "Latitude"
LON_COL = "Longitude"
COUNTRY_COL = "country_name"

# Salidas (si None, se infieren al lado del CSV)
OUT_CSV = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row.csv"
OUT_UNRESOLVED_CSV = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row_unsolved.csv"

# Preferencias
EXPORT_UNRESOLVED_GEOJSON = False  # pon True si también quieres GeoJSON de no asignados

def infer_outputs(csv_path, out_csv, out_unresolved_csv):
    base_dir = os.path.dirname(csv_path)
    base_name = os.path.splitext(os.path.basename(csv_path))[0]
    if out_csv is None:
        out_csv = os.path.join(base_dir, f"{base_name}_with_states.csv")
    if out_unresolved_csv is None:
        out_unresolved_csv = os.path.join(base_dir, f"{base_name}_unresolved_states.csv")
    return out_csv, out_unresolved_csv

def sanitize_country_folder(name: str) -> str:
    """
    Aplica las reglas indicadas para nombre de carpeta:
    - romaniza (ASCII)
    - espacios -> _
    - comas y puntos -> -
    """
    if pd.isna(name):
        return None
    s = unidecode(str(name))
    s = s.replace(" ", "_").replace(",", "-").replace(".", "-")
    return s

def load_points_csv(csv_path: str) -> gpd.GeoDataFrame:
    df = pd.read_csv(csv_path)
    missing = [c for c in [LAT_COL, LON_COL, COUNTRY_COL] if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el CSV: {missing}. Columnas: {list(df.columns)}")

    # A numérico
    df[LAT_COL] = pd.to_numeric(df[LAT_COL], errors="coerce")
    df[LON_COL] = pd.to_numeric(df[LON_COL], errors="coerce")

    # Geometría
    geometry = [
        Point(xy) if pd.notnull(xy[0]) and pd.notnull(xy[1]) else None
        for xy in zip(df[LON_COL], df[LAT_COL])
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    invalid = (~gdf.geometry.notna()).sum()
    if invalid > 0:
        print(f"ADVERTENCIA: {invalid} filas sin geometría válida (lat/lon nulos). Se conservarán como no asignadas.", file=sys.stderr)
    return gdf

def find_country_admin1_shapefile(country_folder: str) -> str:
    """
    Busca un shapefile de polígonos con campo NAME_1 dentro de la carpeta del país.
    Si hay varios, intenta priorizar por nombre común. Si no, devuelve el primero válido.
    """
    if not (country_folder and os.path.isdir(country_folder)):
        return None

    shp_files = glob.glob(os.path.join(country_folder, "*.shp"))
    if not shp_files:
        return None

    # Heurística: prioriza nombres típicos
    priority_keywords = ["adm1", "admin1", "gadm", "states", "provin", "depart", "region"]

    def score(name: str) -> int:
        name_low = name.lower()
        return sum(1 for kw in priority_keywords if kw in name_low)

    shp_files = sorted(shp_files, key=lambda p: -score(os.path.basename(p)))

    for shp in shp_files:
        try:
            gdf = gpd.read_file(shp)
            if gdf is None or gdf.empty:
                continue
            if "NAME_1" not in gdf.columns:
                continue
            # Geometría debe ser polígono/multipolígono
            geom_types = set(gdf.geom_type.str.lower().dropna().unique().tolist())
            if not any(gt in geom_types for gt in ["polygon", "multipolygon"]):
                continue
            return shp
        except Exception:
            continue
    return None


def load_admin1_gdf(shp_path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(shp_path)
    # Asegurar CRS WGS84
    if gdf.crs is None:
        print("ADVERTENCIA: shapefile sin CRS; se asume EPSG:4326.", file=sys.stderr)
        gdf = gdf.set_crs("EPSG:4326")
    else:
        if gdf.crs.to_string().upper() not in ("EPSG:4326", "WGS84", "OGC:CRS84"):
            gdf = gdf.to_crs("EPSG:4326")
    # Reparar geometrías simples
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        gdf = gdf[gdf.geometry.notna()].copy()
        gdf["geometry"] = gdf.geometry.buffer(0)
    return gdf[["NAME_1", "geometry"]].copy()


def assign_states_for_country(points_country: gpd.GeoDataFrame, admin1: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    points_country: GDF de puntos de un solo país (CRS: EPSG:4326)
    admin1: polígonos con NAME_1 (CRS: EPSG:4326)
    Retorna un DataFrame con columnas:
      - state_name_original
      - state_name (romanizado)
      - state_source: 'within' | 'nearest'
    """
    # 1) within
    within_join = gpd.sjoin(
        points_country,
        admin1,
        how="left",
        predicate="within",
    ).drop(columns=[c for c in ["index_right"] if c in points_country.columns or c in admin1.columns], errors="ignore")

    result = within_join[[COUNTRY_COL]].copy()
    result.index = within_join.index  # conservar índices

    result["state_name_original"] = within_join["NAME_1"]
    result["state_name"] = within_join["NAME_1"].apply(lambda x: unidecode(str(x)) if pd.notna(x) else None)
    result["state_source"] = within_join["NAME_1"].apply(lambda v: "within" if pd.notna(v) else None)

    # 2) nearest para los que quedaron sin NAME_1
    missing_idx = result[result["state_name_original"].isna()].index
    if len(missing_idx) > 0 and not admin1.empty:
        # sjoin_nearest (1 vecino más cercano)
        nearest_join = gpd.sjoin_nearest(
            points_country.loc[missing_idx],
            admin1,
            how="left",
            max_distance=None,  # sin límite, tomará el más cercano
            distance_col="__dist__",
        ).drop(columns=[c for c in ["index_right"]], errors="ignore")

        # Actualizar solo los que encontraron vecino
        for idx, row in nearest_join.iterrows():
            name1 = row.get("NAME_1", None)
            if pd.notna(name1):
                result.loc[idx, "state_name_original"] = name1
                result.loc[idx, "state_name"] = unidecode(str(name1))
                result.loc[idx, "state_source"] = "nearest"

    return result


def main():
    out_csv, out_unres = infer_outputs(CSV_PATH, OUT_CSV, OUT_UNRESOLVED_CSV)
    points = load_points_csv(CSV_PATH)

    # Preparar columnas de salida
    points["state_name_original"] = None
    points["state_name"] = None
    points["state_source"] = None
    points["__country_folder__"] = points[COUNTRY_COL].apply(sanitize_country_folder)

    # Procesar por país
    unresolved_idx = set()

    for country_val, grp_idx in points.groupby(COUNTRY_COL).groups.items():
        grp = points.loc[grp_idx]
        country_folder = sanitize_country_folder(country_val)
        folder_path = os.path.join(BASE_COUNTRIES_DIR, country_folder) if country_folder else None

        if (not folder_path) or (not os.path.isdir(folder_path)):
            print(f"ADVERTENCIA: carpeta de país no encontrada para '{country_val}' -> {folder_path}", file=sys.stderr)
            unresolved_idx.update(grp.index.tolist())
            continue

        shp_path = find_country_admin1_shapefile(folder_path)
        if not shp_path:
            print(f"ADVERTENCIA: no se encontró shapefile ADM1 válido (con NAME_1) para '{country_val}' en {folder_path}", file=sys.stderr)
            unresolved_idx.update(grp.index.tolist())
            continue

        try:
            admin1 = load_admin1_gdf(shp_path)
            # Alinear CRS (por seguridad)
            if points.crs is None:
                points.set_crs("EPSG:4326", inplace=True)
            elif points.crs.to_string().upper() not in ("EPSG:4326", "WGS84", "OGC:CRS84"):
                grp = grp.to_crs("EPSG:4326")

            assigned = assign_states_for_country(grp, admin1)

            # Escribir resultados sobre el DF principal
            points.loc[assigned.index, "state_name_original"] = assigned["state_name_original"]
            points.loc[assigned.index, "state_name"] = assigned["state_name"]
            points.loc[assigned.index, "state_source"] = assigned["state_source"]

            # Lo no asignado aquí (si quedó) lo consideramos también no resuelto
            still_missing = assigned[assigned["state_name_original"].isna()].index.tolist()
            unresolved_idx.update(still_missing)

        except Exception as e:
            print(f"ERROR procesando '{country_val}' en {folder_path}: {e}", file=sys.stderr)
            unresolved_idx.update(grp.index.tolist())
            continue

    # Exportar resultados
    # Asegurar columnas lat/lon en salida (por si acaso)
    if LON_COL not in points.columns or LAT_COL not in points.columns:
        coords = points.geometry.apply(lambda g: (g.x, g.y) if g is not None else (None, None))
        points["Longitude"] = [c[0] for c in coords]
        points["Latitude"] = [c[1] for c in coords]

    full_out = points.drop(columns=["geometry", "__country_folder__"], errors="ignore").copy()
    full_out.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # No resueltos (sin state_name)
    unresolved_mask = full_out["state_name"].isna()
    unresolved = full_out[unresolved_mask].copy()
    unresolved.to_csv(out_unres, index=False, encoding="utf-8-sig")

    if EXPORT_UNRESOLVED_GEOJSON and unresolved.shape[0] > 0:
        try:
            # reconstruir gdf para exportar geojson (si geometry quedó)
            unresolved_gdf = points.loc[unresolved.index].copy()
            out_geojson = os.path.splitext(out_unres)[0] + ".geojson"
            unresolved_gdf.to_file(out_geojson, driver="GeoJSON")
            print(f"GeoJSON de no asignados: {out_geojson}")
        except Exception as e:
            print(f"ADVERTENCIA: no se pudo exportar GeoJSON de no asignados: {e}", file=sys.stderr)

    # Reporte
    total = len(points)
    n_within = (points["state_source"] == "within").sum()
    n_nearest = (points["state_source"] == "nearest").sum()
    n_unresolved = unresolved.shape[0]

    print("==== RESUMEN ====")
    print(f"Total estaciones: {total}")
    print(f"Asignadas por 'within': {n_within}")
    print(f"Asignadas por 'nearest': {n_nearest}")
    print(f"No resueltas: {n_unresolved}")
    print(f"CSV completo: {out_csv}")
    print(f"CSV no resueltas: {out_unres}")

if __name__ == "__main__":
    main()