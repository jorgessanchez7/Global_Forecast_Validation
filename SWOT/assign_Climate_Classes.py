import geopandas as gpd
import pandas as pd
from pathlib import Path

# ==============================
# Input files
# ==============================
rivers_path = Path(r"E:\GEOGloWS\SWOT_Intersection\world_SWORD_reaches_with_region.shp")

koppen_path = Path(r"E:\GEOGloWS\Koeppen-Geiger\koppen_geiger\1991_2020\1_km\Koppen_Geiger_Class3.shp")

# Recomendado: no sobrescribir el original
output_path = Path(r"E:\GEOGloWS\SWOT_Intersection\world_SWORD_reaches_total.shp")

# ==============================
# Read data
# ==============================
rivers = gpd.read_file(rivers_path)
koppen = gpd.read_file(koppen_path)

# ==============================
# Keep only needed Koppen fields
# ==============================
koppen = koppen[["Class_1", "Class_2", "Class_3", "geometry"]].copy()

# ==============================
# Ensure same CRS
# ==============================
if rivers.crs != koppen.crs:
    koppen = koppen.to_crs(rivers.crs)

# ==============================
# Create unique river ID for joining back
# ==============================
rivers = rivers.reset_index(drop=True)
rivers["river_uid"] = rivers.index

# ==============================
# Use projected CRS for area calculation
# Important: area should not be calculated in geographic degrees
# EPSG:6933 = World Cylindrical Equal Area
# ==============================
area_crs = "EPSG:6933"

rivers_area = rivers.to_crs(area_crs)
koppen_area = koppen.to_crs(area_crs)

# ==============================
# Intersection between rivers and Koppen polygons
# ==============================
intersections = gpd.overlay(
    rivers_area[["river_uid", "geometry"]],
    koppen_area[["Class_1", "Class_2", "Class_3", "geometry"]],
    how="intersection"
)

# ==============================
# Calculate intersection area
# For line geometries, area may be zero.
# If your SWOT reaches are lines, use length instead of area.
# ==============================
geom_types = rivers_area.geometry.geom_type.unique()

if any(gt in ["LineString", "MultiLineString"] for gt in geom_types):
    intersections["intersect_value"] = intersections.geometry.length
    metric_used = "length"
else:
    intersections["intersect_value"] = intersections.geometry.area
    metric_used = "area"

print(f"Metric used for selecting dominant Koppen class: {metric_used}")

# ==============================
# Select dominant Koppen class per river segment
# ==============================
idx = intersections.groupby("river_uid")["intersect_value"].idxmax()

dominant_koppen = intersections.loc[
    idx, ["river_uid", "Class_1", "Class_2", "Class_3", "intersect_value"]
].copy()

dominant_koppen = dominant_koppen.rename(
    columns={
        "Class_1": "KG_Class1",
        "Class_2": "KG_Class2",
        "Class_3": "KG_Class3",
        "intersect_value": "KG_IntVal"
    }
)

# ==============================
# Join back to original rivers
# ==============================
rivers_out = rivers.merge(
    dominant_koppen,
    on="river_uid",
    how="left"
)

# Remove helper field if desired
rivers_out = rivers_out.drop(columns=["river_uid"])

# ==============================
# Save output shapefile
# ==============================
rivers_out.to_file(output_path)

print("Output saved to:")
print(output_path)

# ==============================
# Report missing Koppen assignments
# ==============================
missing = rivers_out[rivers_out["KG_Class3"].isna()].copy()

missing_csv = Path(r"E:\GEOGloWS\SWOT_Intersection\world_SWORD_reaches_without_koppen.csv")

missing.drop(columns="geometry").to_csv(missing_csv, index=False)

print(f"Total river reaches: {len(rivers_out)}")
print(f"Assigned Koppen class: {rivers_out['KG_Class3'].notna().sum()}")
print(f"Without Koppen class: {rivers_out['KG_Class3'].isna().sum()}")
print("Missing report saved to:")
print(missing_csv)

print("\nSummary by KG_Class3:")
print(rivers_out["KG_Class3"].value_counts(dropna=False))