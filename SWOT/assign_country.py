import os
import pandas as pd
import geopandas as gpd

# =======================
rivers_path     = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches.shp"
countries_path  = r"D:\Countries_SHP\mapamundi.shp"
out_path  = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_country.shp"
# =======================

rivers    = gpd.read_file(rivers_path)
countries = gpd.read_file(countries_path)[["NAME","geometry"]]

if rivers.crs != countries.crs:
    rivers = rivers.to_crs(countries.crs)

centroids = rivers.copy()
centroids["geometry"] = centroids.geometry.representative_point()

joined = gpd.sjoin(centroids, countries, how="left", predicate="within")

rivers["Country"] = joined["NAME"]

rivers.to_file(out_path)
print(f"✅ Guardado: {out_path} con {len(rivers)} ríos y campo Country")