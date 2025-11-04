# ===============================================
# MAPA DE MÉTRICAS KGE (GEOGloWS v2 vs SWOT)
# ===============================================
import numpy as np
import pandas as pd
import geopandas as gpd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import cartopy.feature as cfeature
from matplotlib.lines import Line2D
import matplotlib.patheffects as pe
from cartopy.io.img_tiles import OSM
import matplotlib.patches as mpatches

plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.preamble'] = r'\usepackage{amsmath}'

# -----------------------------
# ARCHIVOS DE ENTRADA Y SALIDA
# -----------------------------
csv_path = r"E:\GEOGloWS\Error_Metrics\Metrics\Metrics_GEOGloWS_v2_WL_SWOT.csv"
shp_path = r"E:\GEOGloWS\SWOT_Intersection\world_SWORD_reaches_total.shp"
out_png = r"E:\GEOGloWS\Error_Metrics\Maps\Metrics_GEOGloWS_v2_WL_SWOT.png"
out_pdf = r"E:\GEOGloWS\Error_Metrics\Maps\Metrics_GEOGloWS_v2_WL_SWOT.pdf"

# -----------------------------
# LECTURA DE DATOS
# -----------------------------
df = pd.read_csv(csv_path)
shp = gpd.read_file(shp_path)

# Asegurar que las llaves sean del mismo tipo
df["ID"] = df["ID"].astype(str)
shp["reach_id"] = shp["reach_id"].astype(str)

# Merge por reach_id = ID
merged = shp.merge(df, how="left", left_on="reach_id", right_on="ID")

# -----------------------------
# ESTADÍSTICAS DE KGE
# -----------------------------
kge_vals = merged["Corrected KGE"].dropna()
number_of_points = len(kge_vals)
median_kge = np.round(kge_vals.median(), 2)
p25, p75 = np.round(kge_vals.quantile([0.25, 0.75]), 2)

# -----------------------------
# CONFIGURACIÓN DEL MAPA
# -----------------------------
#fig, ax = plt.subplots(figsize=(16, 8), subplot_kw={'projection': proj})
fig, ax = plt.subplots(figsize=(16, 8), subplot_kw={'projection': OSM().crs})
ax.add_image(OSM(), 6, alpha=0.5)


borders = cfeature.NaturalEarthFeature('cultural', 'admin_0_countries', '10m', edgecolor='black', facecolor='none')
ax.add_feature(borders, linewidth=0.25)

# Extensión automática con buffer
minx, miny, maxx, maxy = merged.total_bounds
buffer = 0.75  # grados
ax.set_extent([minx - buffer, maxx + buffer, miny - buffer, maxy + buffer], crs=ccrs.PlateCarree())

# -----------------------------
# CLASIFICACIÓN DE COLORES
# -----------------------------
def classify_kge(value):
    if pd.isna(value):
        return '#96b7ff'  # No Data
    elif value < -0.41:
        return 'red'
    elif -0.41 <= value < 0.0:
        return 'orange'
    elif 0.0 <= value < 0.5:
        return 'yellow'
    elif 0.5 <= value < 0.75:
        return 'lightgreen'
    else:
        return 'green'

merged["color"] = merged["Corrected KGE"].apply(classify_kge)

# -----------------------------
# DIBUJAR RED DE RÍOS
# -----------------------------

for color, group in merged.groupby("color"):
    group.plot(ax=ax, color='black', linewidth=0.25, transform=ccrs.PlateCarree(), zorder=1)
    group.plot(ax=ax, color=color, linewidth=0.15, transform=ccrs.PlateCarree(), zorder=2)

# -----------------------------
# LEYENDA DE LÍNEAS
# -----------------------------
legend_colors = ['red', 'orange', 'yellow', 'lightgreen', 'green', '#96b7ff']
legend_labels = [
    r'$KGE < -0.41$',
    r'$-0.41 \leq KGE < 0.00$',
    r'$0.00 \leq KGE < 0.50$',
    r'$0.50 \leq KGE < 0.75$',
    r'$0.75 \leq KGE \leq 1.00$',
    'No Data'
]

legend_lines = [Line2D([0], [0], color=c, lw=2, path_effects=[pe.Stroke(linewidth=3, foreground='black'), pe.Normal()]) for c in legend_colors]
ax.legend(legend_lines, legend_labels, title=r'$\textbf{KGE Categories}$',
          loc='lower left', fontsize=9, frameon=True, facecolor='white')

# -----------------------------
# TEXTO ESTADÍSTICO
# -----------------------------
text_latex = (rf'n: {number_of_points}\\'
              rf'\text{{Median KGE}}: {median_kge}\\'
              rf'\text{{IQR KGE}}: ({p25}, {p75})')

ax.text(-40, -42, f'${text_latex}$', fontsize=11, color='white',
        bbox=dict(facecolor='black', alpha=0.5),
        transform=ccrs.Geodetic())

# -----------------------------
# BARRA DE ESCALA
# -----------------------------
def draw_scale_bar(ax, location, length_km, projection):
    x, y = location
    scale_length_deg = length_km / (111.32 * np.cos(np.radians(y)))
    ax.plot([x, x + scale_length_deg], [y, y], transform=projection,
            color='black', linewidth=3)
    ax.text(x + scale_length_deg / 2, y - 1.5, f'{length_km} km',
            ha='center', va='top', transform=projection, color='black', fontsize=11)

draw_scale_bar(ax, [-160, 0], 7000, ccrs.PlateCarree())

# -----------------------------
# FLECHA DEL NORTE
# -----------------------------
def add_north_arrow(ax, location, arrow_length=5, arrow_color='black', text_color='blue'):
    arrow = mpatches.FancyArrowPatch(
        (location[0], location[1]),
        (location[0], location[1] + arrow_length),
        transform=ccrs.PlateCarree(),
        arrowstyle='-|>', mutation_scale=40,
        color=arrow_color, linewidth=4)
    ax.add_patch(arrow)
    ax.text(location[0], location[1] + arrow_length + 0.5, r'$\textbf{N}$',
            ha='center', va='bottom', fontsize=16, fontweight='bold',
            color=text_color, transform=ccrs.PlateCarree())

def find_north_arrow_location(ax):
    xmin, xmax, ymin, ymax = ax.get_extent(ccrs.PlateCarree())
    xloc = xmin + (xmax - xmin) * 0.35
    yloc = ymin + (ymax - ymin) * 0.55
    return (xloc, yloc)

#north_arrow_location = find_north_arrow_location(ax)
#add_north_arrow(ax, north_arrow_location)


# -----------------------------
# GUARDAR FIGURA
# -----------------------------
#plt.savefig(out_png, dpi=1500, bbox_inches='tight', pad_inches=0)
plt.savefig(out_pdf, format='pdf', dpi=1500, bbox_inches='tight', pad_inches=0)

#plt.show()