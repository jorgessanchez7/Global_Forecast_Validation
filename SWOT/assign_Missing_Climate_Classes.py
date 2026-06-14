import geopandas as gpd
import pandas as pd
from pathlib import Path

# ==============================
# Inputs
# ==============================
rivers_path = Path(r"E:\GEOGloWS\SWOT_Intersection\world_SWORD_reaches_with_region_koppen.shp")

koppen_11km_path = Path(r"E:\GEOGloWS\Koeppen-Geiger\koppen_geiger\1991_2020\11_km\Koppen_Geiger_Class3.shp")

output_path = Path(r"E:\GEOGloWS\SWOT_Intersection\world_SWORD_reaches_with_region_koppen_filled.shp")

missing_csv = Path(r"E:\GEOGloWS\SWOT_Intersection\world_SWORD_reaches_without_koppen_after_11km.csv")

# ==============================
# Read data
# ==============================
rivers = gpd.read_file(rivers_path)
koppen_11km = gpd.read_file(koppen_11km_path)

koppen_11km = koppen_11km[["Class_1", "Class_2", "Class_3", "geometry"]].copy()

# ==============================
# Identify missing reaches
# ==============================
missing_mask = rivers["KG_Class3"].isna()

print(f"Total reaches: {len(rivers)}")
print(f"Missing before 11 km fill: {missing_mask.sum()}")

if missing_mask.sum() == 0:
    print("No missing reaches. Nothing to fill.")
    rivers.to_file(output_path)
else:
    # Helper ID
    rivers = rivers.reset_index(drop=True)
    rivers["river_uid"] = rivers.index

    missing_rivers = rivers.loc[missing_mask, ["river_uid", "geometry"]].copy()

    # Same CRS
    if missing_rivers.crs != koppen_11km.crs:
        koppen_11km = koppen_11km.to_crs(missing_rivers.crs)

    # Use equal-area/projection for spatial metric
    area_crs = "EPSG:6933"

    missing_area = missing_rivers.to_crs(area_crs)
    koppen_area = koppen_11km.to_crs(area_crs)

    # Intersection
    intersections = gpd.overlay(
        missing_area,
        koppen_area[["Class_1", "Class_2", "Class_3", "geometry"]],
        how="intersection"
    )

    if intersections.empty:
        print("No intersections found with 11 km Koppen shapefile.")
    else:
        geom_types = missing_area.geometry.geom_type.unique()

        if any(gt in ["LineString", "MultiLineString"] for gt in geom_types):
            intersections["intersect_value"] = intersections.geometry.length
            metric_used = "length"
        else:
            intersections["intersect_value"] = intersections.geometry.area
            metric_used = "area"

        print(f"Metric used for 11 km fill: {metric_used}")

        idx = intersections.groupby("river_uid")["intersect_value"].idxmax()

        dominant_11km = intersections.loc[
            idx,
            ["river_uid", "Class_1", "Class_2", "Class_3", "intersect_value"]
        ].copy()

        dominant_11km = dominant_11km.rename(
            columns={
                "Class_1": "KG_Class1_11",
                "Class_2": "KG_Class2_11",
                "Class_3": "KG_Class3_11",
                "intersect_value": "KG_IntVal11"
            }
        )

        # Join 11 km results
        rivers = rivers.merge(dominant_11km, on="river_uid", how="left")

        # Fill only missing original values
        fill_mask = rivers["KG_Class3"].isna() & rivers["KG_Class3_11"].notna()

        rivers.loc[fill_mask, "KG_Class1"] = rivers.loc[fill_mask, "KG_Class1_11"]
        rivers.loc[fill_mask, "KG_Class2"] = rivers.loc[fill_mask, "KG_Class2_11"]
        rivers.loc[fill_mask, "KG_Class3"] = rivers.loc[fill_mask, "KG_Class3_11"]

        # Optional: field to know source resolution
        if "KG_Source" not in rivers.columns:
            rivers["KG_Source"] = "1km"

        rivers.loc[fill_mask, "KG_Source"] = "11km"
        rivers.loc[rivers["KG_Class3"].isna(), "KG_Source"] = None

        # Optional: keep 11 km intersection metric
        rivers["KG_IntVal"] = rivers.get("KG_IntVal", pd.NA)
        rivers.loc[fill_mask, "KG_IntVal"] = rivers.loc[fill_mask, "KG_IntVal11"]

        # Drop temporary 11 km columns
        drop_cols = [
            "KG_Class1_11",
            "KG_Class2_11",
            "KG_Class3_11",
            "KG_IntVal11"
        ]
        rivers = rivers.drop(columns=[c for c in drop_cols if c in rivers.columns])

    # Drop helper ID
    if "river_uid" in rivers.columns:
        rivers = rivers.drop(columns=["river_uid"])

    # Save output
    rivers.to_file(output_path)

    # Missing report after 11 km fill
    missing_after = rivers[rivers["KG_Class3"].isna()].copy()
    missing_after.drop(columns="geometry").to_csv(missing_csv, index=False)

    print(f"Filled with 11 km: {missing_mask.sum() - len(missing_after)}")
    print(f"Missing after 11 km fill: {len(missing_after)}")

print("Output saved to:")
print(output_path)

print("Missing report saved to:")
print(missing_csv)