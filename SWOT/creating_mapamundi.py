import os
import glob
import pycountry
import pandas as pd
import geopandas as gpd
from unidecode import unidecode
from shapely.geometry import Point
from shapely.geometry import Polygon
from shapely.geometry import MultiPoint
from shapely.geometry import LineString
from shapely.geometry import MultiPolygon
from shapely.geometry import MultiLineString

root_dir = r"D:\Countries_SHP"
output = r"D:\Countries_SHP\mapamundi.shp"

def geom_family(g):
    if g is None:
        return None
    if isinstance(g, (Point, MultiPoint)):         return "point"
    if isinstance(g, (LineString, MultiLineString)):return "line"
    if isinstance(g, (Polygon, MultiPolygon)):      return "polygon"
    return "other"

paths = glob.glob(os.path.join(root_dir, "**", "*_0.shp"), recursive=True)
if not paths:
    raise FileNotFoundError("No se encontraron shapefiles que terminen en '_0.shp'.")

gdfs, crs_list = [], []
for p in paths:
    try:
        g = gpd.read_file(p)
        if g.empty:
            continue
        g["source"] = p  # rastreo de origen
        gdfs.append(g)
        crs_list.append(str(g.crs) if g.crs is not None else None)
    except Exception as e:
        print(f"Advertencia: no pude leer {p}: {e}")

if not gdfs:
    raise RuntimeError("Se leyeron 0 capas válidas.")

crs_set = set(crs_list)
if len(crs_set) != 1:
    raise ValueError(f"CRS distintos detectados: {crs_set}. "
                     f"Unifica CRS antes de concatenar o indica uno objetivo.")

base_crs = gdfs[0].crs

all_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True, sort=True), geometry="geometry", crs=base_crs)


if "COUNTRY" in all_gdf.columns:
    all_gdf["NAME"] = all_gdf["COUNTRY"].astype(str).apply(lambda x: unidecode(x))
else:
    raise KeyError("El campo 'COUNTRY' no existe en los shapefiles.")

# ISO2: código ISO-3166-1 alpha-2
def get_iso2(country_name):
    try:
        c = pycountry.countries.lookup(country_name)
        return c.alpha_2
    except:
        return None

all_gdf["ISO2"] = all_gdf["COUNTRY"].astype(str).apply(get_iso2)


families = set(all_gdf.geometry.apply(geom_family).dropna())

os.makedirs(os.path.dirname(output), exist_ok=True)

if families.issubset({"point"}) or families.issubset({"line"}) or families.issubset({"polygon"}):
    # Todos del mismo tipo (o subtipos Multi/Single compatibles): exportar único shapefile
    all_gdf = all_gdf[~all_gdf.geometry.isna()].copy()
    all_gdf.to_file(output)
    print(f"✅ Shapefile único creado: {output} ({len(all_gdf)} features) CRS={base_crs}")
else:
    # Mezcla de tipos: exportar uno por tipo (limitación del .shp)
    out_dir = os.path.dirname(output)

    sel = all_gdf.geometry.apply(lambda g: isinstance(g, (Point, MultiPoint)))
    if sel.any():
        pth = os.path.join(out_dir, "combined_0_points.shp")
        all_gdf[sel].to_file(pth)
        print(f"ℹ️ Exportado: {pth} ({sel.sum()} features)")

    sel = all_gdf.geometry.apply(lambda g: isinstance(g, (LineString, MultiLineString)))
    if sel.any():
        pth = os.path.join(out_dir, "combined_0_lines.shp")
        all_gdf[sel].to_file(pth)
        print(f"ℹ️ Exportado: {pth} ({sel.sum()} features)")

    sel = all_gdf.geometry.apply(lambda g: isinstance(g, (Polygon, MultiPolygon)))
    if sel.any():
        pth = os.path.join(out_dir, "combined_0_polygons.shp")
        all_gdf[sel].to_file(pth)
        print(f"ℹ️ Exportado: {pth} ({sel.sum()} features)")

    print("⚠️ Había mezcla de tipos geométricos; se crearon shapefiles separados por tipo.")
