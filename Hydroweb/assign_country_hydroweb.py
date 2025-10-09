#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Asigna país (NAME e ISO2) a puntos (CSV) usando un shapefile de países (polígonos).
- Añade: country_name (NAME sin acentos) y country (ISO2).
- Mantiene todos los puntos, incluso no asignados en el CSV principal.
- Exporta aparte los no asignados a otro CSV y opcionalmente a GeoJSON.
"""

import os
import sys
import pandas as pd
import geopandas as gpd
from unidecode import unidecode
from shapely.geometry import Point

import warnings
warnings.filterwarnings("ignore")

# =========================
# CONFIGURA AQUÍ TUS RUTAS
# =========================
CSV_PATH = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row.csv"
COUNTRIES_SHP = r"D:\Countries_SHP\mapamundi.shp"

# Opcionales (si los dejas como None, se infieren en la carpeta del CSV)
OUT_CSV = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row.csv"
OUT_UNASSIGNED_CSV = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\hydroweb_metadata_NO_country.csv"
EXPORT_UNASSIGNED_GEOJSON = False  # Cambia a False si no quieres GeoJSON

# Nombres de columnas del CSV de entrada
LAT_COL = "Latitude"
LON_COL = "Longitude"
ELLIPSOID_COL = "Ellipsoid"


def infer_outputs(csv_path, out_csv, out_unassigned_csv):
    base_dir = os.path.dirname(csv_path)
    base_name = os.path.splitext(os.path.basename(csv_path))[0]
    if out_csv is None:
        out_csv = os.path.join(base_dir, f"{base_name}_with_countries.csv")
    if out_unassigned_csv is None:
        out_unassigned_csv = os.path.join(base_dir, f"{base_name}_unassigned.csv")
    return out_csv, out_unassigned_csv


def validate_inputs(df, lat_col, lon_col, ellipsoid_col):
    missing = [c for c in [lat_col, lon_col, ellipsoid_col] if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en el CSV: {missing}. Columnas encontradas: {list(df.columns)}")
    if not df[ellipsoid_col].astype(str).str.upper().str.contains("WGS84").all():
        print("ADVERTENCIA: Ellipsoid no es WGS84 en todas las filas. Se asumirá EPSG:4326.", file=sys.stderr)


def load_points_csv(csv_path, lat_col, lon_col, ellipsoid_col):
    df = pd.read_csv(csv_path)
    validate_inputs(df, lat_col, lon_col, ellipsoid_col)

    # A numérico
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")

    # Geometría
    geometry = [Point(xy) if pd.notnull(xy[0]) and pd.notnull(xy[1]) else None
                for xy in zip(df[lon_col], df[lat_col])]

    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    # Aviso si hay puntos sin geometría
    invalid = (~gdf.geometry.notna()).sum()
    if invalid > 0:
        print(f"ADVERTENCIA: {invalid} filas sin geometría válida (lat/lon nulos). Se conservarán como no asignadas.", file=sys.stderr)
    return gdf


def load_countries_shp(countries_shp):
    countries = gpd.read_file(countries_shp)

    for col in ["NAME", "ISO2"]:
        if col not in countries.columns:
            raise ValueError(f"El shapefile debe tener columnas 'NAME' e 'ISO2'. Columnas: {list(countries.columns)}")

    # Asegurar CRS WGS84
    if countries.crs is None:
        print("ADVERTENCIA: Shapefile sin CRS. Se asumirá EPSG:4326.", file=sys.stderr)
        countries = countries.set_crs("EPSG:4326")
    else:
        if countries.crs.to_string().upper() not in ("EPSG:4326", "WGS84", "OGC:CRS84"):
            countries = countries.to_crs("EPSG:4326")

    # Arreglar geometrías simples
    countries = countries[countries.geometry.notna()].copy()
    countries["geometry"] = countries.geometry.buffer(0)
    return countries


def romance_name(name):
    if pd.isna(name):
        return name
    return unidecode(str(name))


def assign_countries(points_gdf, countries_gdf):
    # Spatial join: puntos dentro de polígonos
    joined = gpd.sjoin(
        points_gdf,
        countries_gdf[["NAME", "ISO2", "geometry"]],
        how="left",
        predicate="within"
    )

    joined["country_name"] = joined["NAME"].apply(romance_name)
    joined["country"] = joined["ISO2"].astype(str).str.upper()

    # Limpiar columnas auxiliares
    drop_cols = [c for c in joined.columns if c.startswith("index_")]
    joined = joined.drop(columns=drop_cols, errors="ignore")
    return joined


def export_results(joined, out_csv, out_unassigned_csv, export_unassigned_geojson):
    # Asegurar columnas lon/lat en el CSV de salida
    if "Longitude" not in joined.columns or "Latitude" not in joined.columns:
        coords = joined.geometry.apply(lambda g: (g.x, g.y) if g is not None else (None, None))
        joined["Longitude"] = [c[0] for c in coords]
        joined["Latitude"] = [c[1] for c in coords]

    joined_no_geom = joined.drop(columns=["geometry"], errors="ignore").copy()
    joined_no_geom.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # No asignados: cuando no hay NAME/ISO2 (ambos NaN)
    mask_unassigned = joined["country"].isna() & joined["country_name"].isna()
    unassigned = joined.loc[mask_unassigned].copy()

    if len(unassigned) > 0:
        unassigned_no_geom = unassigned.drop(columns=["geometry"], errors="ignore").copy()
        unassigned_no_geom.to_csv(out_unassigned_csv, index=False, encoding="utf-8-sig")
        if export_unassigned_geojson:
            base_dir = os.path.dirname(out_unassigned_csv)
            base_name = os.path.splitext(os.path.basename(out_unassigned_csv))[0]
            out_geojson = os.path.join(base_dir, f"{base_name}.geojson")
            try:
                unassigned.to_file(out_geojson, driver="GeoJSON")
                print(f"GeoJSON de no asignados: {out_geojson}")
            except Exception as e:
                print(f"ADVERTENCIA: No se pudo exportar GeoJSON de no asignados: {e}", file=sys.stderr)
    else:
        # Crear CSV vacío con mismas columnas si no hay no asignados
        pd.DataFrame(columns=joined_no_geom.columns).to_csv(out_unassigned_csv, index=False, encoding="utf-8-sig")

    total = len(joined)
    n_unassigned = mask_unassigned.sum()
    n_assigned = total - n_unassigned
    print(f"Total puntos: {total}")
    print(f"Asignados: {n_assigned}")
    print(f"No asignados: {n_unassigned}")
    print(f"CSV con países: {out_csv}")
    print(f"CSV no asignados: {out_unassigned_csv}")


# =====================
# EJECUCIÓN DIRECTA
# =====================
if __name__ == "__main__":
    OUT_CSV, OUT_UNASSIGNED_CSV = infer_outputs(CSV_PATH, OUT_CSV, OUT_UNASSIGNED_CSV)

    points_gdf = load_points_csv(CSV_PATH, LAT_COL, LON_COL, ELLIPSOID_COL)
    countries_gdf = load_countries_shp(COUNTRIES_SHP)

    # Alinear CRS de puntos (por si acaso)
    if points_gdf.crs is None:
        points_gdf = points_gdf.set_crs("EPSG:4326")
    elif points_gdf.crs.to_string().upper() not in ("EPSG:4326", "WGS84", "OGC:CRS84"):
        points_gdf = points_gdf.to_crs("EPSG:4326")

    joined = assign_countries(points_gdf, countries_gdf)
    export_results(joined, OUT_CSV, OUT_UNASSIGNED_CSV, EXPORT_UNASSIGNED_GEOJSON)
