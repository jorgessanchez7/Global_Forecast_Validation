# Requisitos: pip install geopandas fiona shapely
import geopandas as gpd

# === Rutas ===
rivers_path   = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_state.shp"
regions_path  = r"D:\GEOGloWS\v1\all_boundaries_simplified.shp"  # campo 'Region'
vpu_path      = r"D:\GEOGloWS\v2\all_boundaries_simplified.shp"  # campo 'vpu_code'
out_path      = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_region_and_vpu.shp"
# =============

# 1) Leer capas
rivers  = gpd.read_file(rivers_path)
regions = gpd.read_file(regions_path)[["Region", "geometry"]]
vpu     = gpd.read_file(vpu_path)[["vpu_code", "geometry"]]

# Crear columnas si no existen
if "region" not in rivers.columns: rivers["region"] = None
if "VPU" not in rivers.columns:    rivers["VPU"]    = None

# 2) Asignar REGION (tramo COMPLETAMENTE contenido en polígono de regiones)
rivers_r = rivers.copy()
if rivers_r.crs != regions.crs:
    rivers_r = rivers_r.to_crs(regions.crs)

# sjoin con predicado 'within' (línea dentro de polígono)
join_r = gpd.sjoin(rivers_r[["geometry"]], regions, how="left", predicate="within")
# Mapear resultado a la capa original por índice
rivers.loc[join_r.index, "region"] = join_r["Region"].values

# 3) Asignar VPU (tramo COMPLETAMENTE contenido en polígono VPU)
rivers_v = rivers.copy()
if rivers_v.crs != vpu.crs:
    rivers_v = rivers_v.to_crs(vpu.crs)

join_v = gpd.sjoin(rivers_v[["geometry"]], vpu, how="left", predicate="within")
rivers.loc[ join_v.index, "VPU"] = join_v["vpu_code"].values

# 4) Guardar a un archivo NUEVO (no sobrescribir el original)
rivers.to_file(out_path)
print(f"✅ Guardado: {out_path}")
print(f"   Total ríos: {len(rivers)}")
print(f"   Con 'region' asignada: {(rivers['region'].notna()).sum()}")
print(f"   Con 'VPU' asignada: {(rivers['VPU'].notna()).sum()}")
