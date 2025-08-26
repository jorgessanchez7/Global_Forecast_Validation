# Requisitos: pip install geopandas shapely fiona unidecode
import os
import glob
import pandas as pd
import geopandas as gpd
from unidecode import unidecode

# =======================
rivers_path    = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_country.shp"
countries_dir  = r"D:\Countries_SHP"   # dentro hay carpetas por país
out_path       = r"G:\My Drive\Personal_Files\Post_Doc\SWOT\SWOT_Total_Streams\world_SWORD_reaches_with_state.shp"
# =======================


def normalize_country_folder(name: str) -> str:
    """Aplica reglas para mapear el nombre del país (campo Country) a la carpeta."""
    if name is None:
        return None
    # casos especiales
    specials = {
        "South Georgia and the South Sand": "South_Georgia_and_the_South_Sandwich_Islands",
        "United States Minor Outlying Isl": "United_States_Minor_Outlying_Islands",
    }
    if name in specials:
        return specials[name]
    # reglas generales
    s = name.replace(" ", "_")
    s = s.replace(",", "-").replace(".", "-")
    return s

def ensure_metric_crs(crs):
    """Devuelve un CRS métrico global razonable si el actual no es proyectado."""
    # Usaremos World Mercator (EPSG:3395) para medir distancias (metros)
    return "EPSG:3395"

# --- Leer ríos ---
rivers = gpd.read_file(rivers_path)
if "Country" not in rivers.columns:
    raise KeyError("El shapefile de ríos no tiene el campo 'Country'.")

# Crear campo State si no existe
if "State" not in rivers.columns:
    rivers["State"] = None

# Guardar CRS original para devolver luego
crs_rivers_orig = rivers.crs

# Trabajaremos con un punto representativo por tramo para el 'within'
points = rivers.copy()
points["geometry"] = points.geometry.representative_point()

# Solo países conocidos (no nulos)
countries_list = sorted(pd.unique(rivers["Country"].dropna()))

print(f"Países a procesar: {len(countries_list)}")

for country_name in countries_list:
    folder = normalize_country_folder(country_name)
    if not folder:
        print(f"  - {country_name}: nombre de carpeta no válido -> se omite")
        continue

    country_path = os.path.join(countries_dir, folder)
    if not os.path.isdir(country_path):
        print(f"  - {country_name}: carpeta no existe: {country_path} -> se omite")
        continue

    # Buscar shapefile *_1.shp de estados
    shp_candidates = glob.glob(os.path.join(country_path, "*_1.shp"))
    if not shp_candidates:
        print(f"  - {country_name}: no se encontró *_1.shp en {country_path} -> se omite")
        continue
    if len(shp_candidates) > 1:
        print(f"  - {country_name}: múltiples *_1.shp encontrados, se usa el primero: {os.path.basename(shp_candidates[0])}")

    states_path = shp_candidates[0]
    states = gpd.read_file(states_path)

    if "NAME_1" not in states.columns:
        print(f"  - {country_name}: el shape de estados no tiene campo 'NAME_1' -> se omite")
        continue

    # Alinear CRS para 'within'
    pts = points[points["Country"] == country_name].copy()
    if pts.empty:
        continue

    if pts.crs != states.crs:
        pts = pts.to_crs(states.crs)

    # --- 1) Asignación por 'within' (punto representativo dentro del estado) ---
    join_within = gpd.sjoin(pts[["geometry"]], states[["NAME_1", "geometry"]], how="left", predicate="within")
    # join_within index = índices originales de rivers (porque copiamos de rivers)
    assigned_within = join_within["NAME_1"]

    # Aplicar romanización
    assigned_within = assigned_within.apply(lambda x: unidecode(x) if pd.notna(x) else x)

    # Escribir a la columna State solo donde todavía esté vacío
    mask_country = (rivers["Country"] == country_name)
    mask_empty_state = rivers["State"].isna() | (rivers["State"].astype(str).str.len() == 0)
    idx_to_fill = mask_country & mask_empty_state
    rivers.loc[idx_to_fill, "State"] = rivers.loc[idx_to_fill].index.to_series().map(assigned_within)

    # --- 2) Para los que quedaron sin asignar, usar el estado MÁS CERCANO ≤ 10 km ---
    # Filtrar pendientes
    pending_idx = rivers.index[mask_country & (rivers["State"].isna() | (rivers["State"].astype(str).str.len() == 0))]
    if len(pending_idx) > 0:
        # Preparar copias en CRS métrico
        metric_crs = ensure_metric_crs(states.crs)
        pts_pending = points.loc[pending_idx, ["geometry"]].copy()
        st_geoms   = states[["NAME_1", "geometry"]].copy()

        if pts_pending.crs != metric_crs:
            pts_pending = pts_pending.to_crs(metric_crs)
        if st_geoms.crs != metric_crs:
            st_geoms = st_geoms.to_crs(metric_crs)

        # Join por cercanía con umbral 10 km (10,000 m)
        join_near = gpd.sjoin_nearest(
            pts_pending,
            st_geoms,
            how="left",
            distance_col="dist_m",
            max_distance=10_000
        )

        # Romanizar y asignar solo si hubo match dentro del límite
        near_vals = join_near["NAME_1"].apply(lambda x: unidecode(x) if pd.notna(x) else x)
        rivers.loc[pending_idx, "State"] = rivers.loc[pending_idx].index.to_series().map(near_vals)

    print(f"  ✓ {country_name}: asignado 'State' por within/nearest (≤10 km)")

# Devolver CRS original de ríos (por si se tocó en algún paso)
if rivers.crs != crs_rivers_orig:
    rivers = rivers.to_crs(crs_rivers_orig)

# Guardar en archivo NUEVO
rivers.to_file(out_path)
print(f"\n✅ Guardado: {out_path}\n   Features: {len(rivers)} | CRS: {rivers.crs}")
# Pequeño resumen
n_assigned = rivers["State"].notna().sum()
print(f"   'State' asignado en {n_assigned} de {len(rivers)} tramos.")
