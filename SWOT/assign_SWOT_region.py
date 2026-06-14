import geopandas as gpd
import pandas as pd
from netCDF4 import Dataset

# ==============================
# Archivos NetCDF por región
# ==============================
files = {
    "af": r"E:\SWOT\Discharge\af_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "as": r"E:\SWOT\Discharge\as_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "eu": r"E:\SWOT\Discharge\eu_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "na": r"E:\SWOT\Discharge\na_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "oc": r"E:\SWOT\Discharge\oc_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "sa": r"E:\SWOT\Discharge\sa_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
}

# ==============================
# Crear diccionario reach_id → región
# ==============================
reach_to_region = {}

for region, path in files.items():
    print(f"Leyendo {region}")

    with Dataset(path) as ds:
        reach_ids = ds["reaches"]["reach_id"][:]

    for rid in reach_ids:
        reach_to_region[int(rid)] = region

print(f"\nTotal reach_ids únicos: {len(reach_to_region)}")

# ==============================
# Leer shapefile
# ==============================
shp_path = r"E:\GEOGloWS\SWOT_Intersection\world_SWORD_reaches_total.shp"
gdf = gpd.read_file(shp_path)

# Asegurar tipo consistente
gdf["reach_id"] = gdf["reach_id"].astype("Int64")

# ==============================
# Asignar región
# ==============================
def assign_region(rid):
    if pd.isna(rid):
        return None
    return reach_to_region.get(int(rid), None)

gdf["SWOTRegion"] = gdf["reach_id"].apply(assign_region)

# ⚠️ Importante: evitar que "na" se convierta en NaN
gdf["SWOTRegion"] = gdf["SWOTRegion"].astype(object)

# ==============================
# Guardar shapefile
# ==============================
output_shp = r"E:\GEOGloWS\SWOT_Intersection\world_SWORD_reaches_with_region.shp"
gdf.to_file(output_shp)

print(f"\nShapefile guardado en:\n{output_shp}")

# ==============================
# Reporte de valores sin región
# ==============================
missing = gdf[gdf["SWOTRegion"].isna()].copy()

print(f"\nReach_ids sin región: {len(missing)}")

# Guardar CSV
missing_csv = r"E:\GEOGloWS\SWOT_Intersection\reach_ids_without_region.csv"
missing[["reach_id"]].to_csv(missing_csv, index=False)

print(f"CSV de faltantes guardado en:\n{missing_csv}")

# ==============================
# Resumen por región
# ==============================
summary = gdf["SWOTRegion"].value_counts(dropna=False)
print("\n=== Resumen ===")
print(summary)