import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon

# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

ascii_file = r"E:\GEOGloWS\Koeppen-Geiger\Koeppen-Geiger.txt"
output_shp = r"E:\GEOGloWS\Koeppen-Geiger\Koeppen-Geiger_regions.shp"
cls_col = "Cls"


# ============================================================
# 2. LEER ASCII Y CALCULAR RESOLUCIÓN
# ============================================================

print("Leyendo archivo ASCII...")
df = pd.read_csv(ascii_file, delim_whitespace=True)

# Comprobar columnas
esperadas = {"Lat", "Lon", cls_col}
if not esperadas.issubset(df.columns):
    raise ValueError(
        f"Las columnas {esperadas} deben existir en el archivo. "
        f"Columnas encontradas: {df.columns.tolist()}"
    )

print("Infiriendo resolución de la grilla...")
res_lon = df["Lon"].sort_values().drop_duplicates().diff().abs().median()
res_lat = df["Lat"].sort_values().drop_duplicates().diff().abs().median()
half_x, half_y = res_lon / 2.0, res_lat / 2.0
print(f"Resolución: dLon = {res_lon}, dLat = {res_lat}")

# ============================================================
# 3. CONSTRUIR POLÍGONOS DE CELDA
# ============================================================

print("Construyendo polígonos de celdas...")
polygons = []
for _, row in df.iterrows():
    lon = row["Lon"]
    lat = row["Lat"]
    poly = Polygon([
        (lon - half_x, lat - half_y),
        (lon + half_x, lat - half_y),
        (lon + half_x, lat + half_y),
        (lon - half_x, lat + half_y)
    ])
    polygons.append(poly)

gdf = gpd.GeoDataFrame(df[[cls_col]].copy(), geometry=polygons, crs="EPSG:4326")

# Normalizar clasificación a mayúsculas
gdf[cls_col] = gdf[cls_col].astype(str).str.upper()

# ============================================================
# 4. DISOLVER POR CLASE (Cls)
# ============================================================

print("Disolviendo por clase climática...")
# Esto crea un registro por valor único de Cls,
# con un MultiPolygon que contiene todas las regiones de esa clase
regions = gdf.dissolve(by=cls_col, as_index=False)

# (Opcional) pequeño buffer para limpiar geometrías y unir bordes pegados
regions["geometry"] = regions.buffer(0)

print("Número de clases climáticas:", len(regions))

# ============================================================
# 5. GUARDAR COMO SHAPEFILE
# ============================================================

print("Guardando shapefile final de regiones...")
regions.to_file(output_shp, driver="ESRI Shapefile")

print("✅ Listo.")
print("   Archivo:", output_shp)
print(regions.head())
