import os
import zipfile
import tempfile
import geopandas as gpd

# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

kmz_path = r"E:\GEOGloWS\Koeppen-Geiger\Global_1986-2010_KG_5m.kmz"   # <-- tu KMZ
output_shp = r"E:\GEOGloWS\Koeppen-Geiger\Koeppen-Geiger.shp"  # <-- shapefile de salida


# ============================================================
# 2. EXTRAER EL KML DESDE EL KMZ
# ============================================================

with tempfile.TemporaryDirectory() as tmpdir:
    print("Descomprimiendo KMZ en:", tmpdir)
    with zipfile.ZipFile(kmz_path, 'r') as z:
        # listar todos los archivos .kml dentro del kmz
        kml_files = [f for f in z.namelist() if f.lower().endswith(".kml")]
        if not kml_files:
            raise RuntimeError("No se encontró ningún archivo .kml dentro del KMZ.")

        # tomamos el primero (si hay varios, luego se puede refinar)
        kml_name = kml_files[0]
        print("Usando KML:", kml_name)

        z.extract(kml_name, tmpdir)
        kml_path = os.path.join(tmpdir, kml_name)

        # ============================================================
        # 3. LEER EL KML CON GEOPANDAS (GDAL/Fiona)
        # ============================================================

        print("Leyendo KML con GeoPandas...")
        gdf = gpd.read_file(kml_path)

        print("Geometrías leídas:", gdf.geometry.type.unique())

        # Si solo quieres polígonos, descomenta esto:
        # gdf = gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])].copy()

        # ============================================================
        # 4. GUARDAR COMO SHAPEFILE
        # ============================================================

        print("Guardando shapefile...")
        gdf.to_file(output_shp, driver="ESRI Shapefile")

print("✅ Shapefile creado en:", output_shp)
