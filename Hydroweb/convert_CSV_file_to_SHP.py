#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import warnings
warnings.filterwarnings("ignore")

# =========================
# RUTAS
# =========================
CSV_PATH = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row.csv"
OUT_DIR = os.path.dirname(CSV_PATH)
OUT_SHP = os.path.join(OUT_DIR, "Hydroweb_Stations_row_points.shp")
OUT_NOGEO_CSV = os.path.join(OUT_DIR, "Hydroweb_Stations_row_no_geometry.csv")

# =========================
# MAPEO DE NOMBRES (<=10 chars para Shapefile)
# =========================
FIELD_MAP = {
    "ID": "ID",
    "Name": "NAME",
    "Basin": "BASIN",
    "River": "RIVER",
    "Latitude": "LAT",
    "Longitude": "LON",
    "Elevation": "ELEV",
    "Ellipsoid": "ELLIPSOID",
    "Slope (m/m)": "SLOPE",
    "Missions": "MISSIONS",
    "state": "STATE",
    "country_name": "COUNTRY_N",
    "country": "COUNTRY",              # ISO-2 (ojo con "NA" de Namibia)
    "COMID_v1": "COMID_V1",
    "Region": "REGION",
    "COMID_v2": "COMID_V2",
    "VPU": "VPU",
    "First Date of Data": "FIRST_DATE",
    "Last Date of Data": "LAST_DATE",
    "Number of Measurements": "N_MEASURE",
    "Status": "STATUS",
    "Type": "TYPE",
}

# Campos numéricos que deben permitir Null (usar float para que no se conviertan a 0)
NUMERIC_FLOAT_COLS = ["LAT", "LON", "ELEV", "SLOPE_MM", "COMID_V1", "COMID_V2", "VPU", "N_MEASURE"]

def main():
    # Leer CSV sin convertir 'NA' (Namibia) a NaN
    # keep_default_na=False evita que 'NA' se trate como NaN; especificamos nulos comunes manualmente
    df = pd.read_csv(CSV_PATH, keep_default_na=False, na_values=["", "NaN", "nan", "NULL", "null", "-", "--"])

    # Renombrar solo las columnas que existan
    existing_map = {k: v for k, v in FIELD_MAP.items() if k in df.columns}
    df = df.rename(columns=existing_map)

    # Verificación mínima
    for required in ["LAT", "LON", "COUNTRY"]:
        if required not in df.columns:
            print(f"ADVERTENCIA: falta la columna '{required}' tras el renombrado.", file=sys.stderr)

    # A numérico (float) para permitir Nulls verdaderos en el Shapefile
    for col in [c for c in NUMERIC_FLOAT_COLS if c in df.columns]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    # Geometría de puntos (EPSG:4326)
    # Si LAT/LON no existen (por nombre diferente), intentamos con originales
    if "LAT" not in df.columns and "Latitude" in FIELD_MAP:
        df["LAT"] = pd.to_numeric(df.get("Latitude"), errors="coerce")
    if "LON" not in df.columns and "Longitude" in FIELD_MAP:
        df["LON"] = pd.to_numeric(df.get("Longitude"), errors="coerce")

    geometry = [
        Point(xy) if pd.notnull(xy[0]) and pd.notnull(xy[1]) else None
        for xy in zip(df.get("LON", pd.Series([None]*len(df))), df.get("LAT", pd.Series([None]*len(df))))
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    # Separar filas sin geometría válida (no se pueden escribir al Shapefile)
    mask_valid = gdf.geometry.notna()
    gdf_valid = gdf.loc[mask_valid].copy()
    gdf_nogeo = gdf.loc[~mask_valid].copy()

    if len(gdf_nogeo) > 0:
        # Guardar un CSV con las filas que no tienen geometría
        gdf_nogeo.drop(columns=["geometry"], errors="ignore").to_csv(OUT_NOGEO_CSV, index=False, encoding="utf-8-sig")
        print(f"Aviso: {len(gdf_nogeo)} filas sin geometría -> {OUT_NOGEO_CSV}")

    # Escribir Shapefile
    # Nota: Fiona/GDAL manejarán None como Null en DBF.
    # Mantener encoding; si ves caracteres raros, prueba encoding="utf-8"
    gdf_valid.to_file(OUT_SHP, driver="ESRI Shapefile", encoding="utf-8")
    print(f"Shapefile creado: {OUT_SHP}")
    print(f"Total filas: {len(gdf)} | con geometría: {len(gdf_valid)} | sin geometría: {len(gdf_nogeo)}")

if __name__ == "__main__":
    main()
