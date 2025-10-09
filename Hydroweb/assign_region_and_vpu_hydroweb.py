# Requisitos: pip install geopandas shapely fiona unidecode
import os
import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import warnings
warnings.filterwarnings("ignore")

# =========================
# RUTAS DE ENTRADA / SALIDA
# =========================
CSV_PATH = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row.csv"
REGION_SHP_V1 = r"D:\GEOGloWS\v1\all_boundaries_simplified.shp"   # campo 'Region'
VPU_SHP_V2    = r"D:\GEOGloWS\v2\all_boundaries_simplified.shp"   # campo 'vpu_code'
OUT_CSV       = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row.csv"

# Nombres de columnas en el CSV
LAT_COL = "Latitude"
LON_COL = "Longitude"
COUNTRY_COL = "country"   # ¡OJO! Namibia = "NA", no NaN

def load_points_csv(csv_path: str) -> gpd.GeoDataFrame:
    """
    Lee el CSV asegurando que 'NA' (Namibia) NO se convierta en NaN.
    """
    # keep_default_na=False evita que 'NA' se trate como missing.
    # Aun así, seguimos reconociendo '', 'NaN', 'nan' como nulos.
    df = pd.read_csv(csv_path, keep_default_na=False, na_values=["", "NaN", "nan"])

    # Validaciones mínimas
    missing = [c for c in [LAT_COL, LON_COL] if c not in df.columns]
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
        print(f"ADVERTENCIA: {invalid} filas sin geometría válida (lat/lon nulos). Se conservarán sin asignación.", file=sys.stderr)
    return gdf


def load_poly_layer(shp_path: str, required_field: str) -> gpd.GeoDataFrame:
    """
    Carga un shapefile poligonal, asegura CRS EPSG:4326 y existencia del campo requerido.
    Aplica un buffer(0) suave para reparar geometrías simples.
    """
    if not os.path.isfile(shp_path):
        raise FileNotFoundError(f"No se encontró shapefile: {shp_path}")

    gdf = gpd.read_file(shp_path)

    if required_field not in gdf.columns:
        raise ValueError(f"'{required_field}' no está en {shp_path}. Columnas: {list(gdf.columns)}")

    # Asegurar CRS
    if gdf.crs is None:
        print(f"ADVERTENCIA: {shp_path} no tiene CRS. Se asume EPSG:4326.", file=sys.stderr)
        gdf = gdf.set_crs("EPSG:4326")
    else:
        if gdf.crs.to_string().upper() not in ("EPSG:4326", "WGS84", "OGC:CRS84"):
            gdf = gdf.to_crs("EPSG:4326")

    # Filtrar geometrías válidas y reparar
    gdf = gdf[gdf.geometry.notna()].copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        gdf["geometry"] = gdf.geometry.buffer(0)

    return gdf[[required_field, "geometry"]].copy()

def assign_within(points_gdf: gpd.GeoDataFrame, polys_gdf: gpd.GeoDataFrame, field: str) -> pd.Series:
    """
    Asigna el valor 'field' del polígono a cada punto si está DENTRO (within).
    Devuelve una Serie alineada con el índice de 'points_gdf' (valores NaN si no cae dentro).
    """
    joined = gpd.sjoin(points_gdf, polys_gdf[[field, "geometry"]], how="left", predicate="within", lsuffix="pt", rsuffix="poly",)

    right_field = f"{field}_poly" if f"{field}_poly" in joined.columns else field
    if right_field not in joined.columns:
        raise KeyError(
            f"No se encontró la columna '{field}' ni '{field}_poly' tras el sjoin. "
            f"Columnas disponibles: {list(joined.columns)}"
        )
    return joined[right_field]

def main():
    # 1) Cargar puntos
    points = load_points_csv(CSV_PATH)

    # 2) Cargar capas poligonales (v1 Region, v2 VPU)
    region_v1 = load_poly_layer(REGION_SHP_V1, required_field="Region")
    vpu_v2    = load_poly_layer(VPU_SHP_V2,    required_field="vpu_code")

    # 3) Alinear CRS por si acaso (puntos deberían estar ya en EPSG:4326)
    if points.crs is None:
        points = points.set_crs("EPSG:4326")
    elif points.crs.to_string().upper() not in ("EPSG:4326", "WGS84", "OGC:CRS84"):
        points = points.to_crs("EPSG:4326")

    # 4) Asignaciones estrictas por 'within'
    points["Region_2"] = assign_within(points, region_v1, field="Region")
    points["VPU_2"]    = assign_within(points, vpu_v2,    field="vpu_code")

    # 5) Reporte simple
    total = len(points)
    n_region = points["Region_2"].notna().sum()
    n_vpu = points["VPU_2"].notna().sum()
    print("==== RESUMEN ====")
    print(f"Total estaciones: {total}")
    print(f"Region asignada (within): {n_region} ({n_region/total:.1%})")
    print(f"VPU asignada (within):    {n_vpu} ({n_vpu/total:.1%})")

    # 6) Exportar CSV (sin geometría)
    out_dir = os.path.dirname(OUT_CSV)
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    out_df = points.drop(columns=["geometry"], errors="ignore").copy()
    # Asegurar que 'country' permanezca literal ("NA" no debe volverse NaN)
    # (ya lo cuidamos al leer; al escribir no cambia)
    out_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"CSV con Region y VPU: {OUT_CSV}")

if __name__ == "__main__":
    main()