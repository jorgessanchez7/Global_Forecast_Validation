"""
Bias correction comparison map for Amazonia (Brazil, Colombia, Ecuador, Peru).
Side-by-side: Before vs. After bias correction.
Matches the aesthetic of the user's original code (OSM base tiles, LaTeX legend,
dark stats box, custom scale bar).
"""
import time
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.io.img_tiles import OSM

# ---------- LaTeX rendering (matches original code) ----------
plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.preamble'] = r'\usepackage{amsmath}'

# ---------- Load and filter data ----------
before = gpd.read_file(r"E:\PhD\2022_Fall\Dissertation_v14\Shapes\South_America\Amazonia\Metrics_Before_Bias_Correction_Amazonia.shp")
after = gpd.read_file(r"E:\PhD\2022_Fall\Dissertation_v14\Shapes\South_America\Amazonia\Metrics_After_Bias_Correction_Amazonia.shp")

selected_countries = ['Brazil', 'Colombia', 'Ecuador', 'Peru']
before = before[before['Country'].isin(selected_countries)].copy()
after  = after[after['Country'].isin(selected_countries)].copy()

kge_col = 'KGE__2012_'

# Cap at 1.0 (KGE is bounded above by 1; tiny numerical drift can exceed it)
before['kge_capped'] = before[kge_col].clip(upper=1.0)
after['kge_capped']  = after[kge_col].clip(upper=1.0)

# ---------- Helper: KGE -> color ----------
def kge_color(k):
    if k < -0.41:        return 'red'
    elif k < 0.0:        return 'orange'
    elif k < 0.5:        return 'yellow'
    elif k < 0.75:       return 'lightgreen'
    else:                return 'green'

# ---------- Helper: stats ----------
def panel_stats(gdf):
    k = gdf['kge_capped'].values
    return {
        'n':      len(k),
        'median': round(float(np.median(k)), 2),
        'p25':    round(float(np.percentile(k, 25)), 2),
        'p75':    round(float(np.percentile(k, 75)), 2),
    }

stats_before = panel_stats(before)
stats_after  = panel_stats(after)

# ---------- Custom scale bar ----------
def draw_scale_bar(ax, location, length_km, projection):
    """Scale bar in km at given (lon, lat) location."""
    x, y = location
    scale_length_deg = length_km / (111.32 * np.cos(np.radians(y)))
    ax.plot([x, x + scale_length_deg], [y, y],
            transform=projection, color='black', linewidth=3,
            solid_capstyle='butt')
    # End ticks
    for xt in (x, x + scale_length_deg):
        ax.plot([xt, xt], [y - 0.4, y + 0.4],
                transform=projection, color='black', linewidth=2)
    ax.text(x + scale_length_deg / 2, y - 1.5, f'{length_km} km',
            horizontalalignment='center', verticalalignment='top',
            transform=projection, color='black', fontsize=11)

# ---------- North arrow ----------
def north_arrow(ax):
    ax.annotate('N', xy=(0.95, 0.95), xytext=(0.95, 0.85),
                xycoords='axes fraction',
                ha='center', va='bottom',
                fontsize=20, fontweight='bold',
                arrowprops=dict(arrowstyle='-|>',
                                facecolor='black', edgecolor='black',
                                lw=5.5, mutation_scale=35),
                zorder=30)

# ---------- Plot one panel ----------
OSM_ZOOM = 5    # whole Amazon region; tile count is manageable here

# Map extent matching the four target countries
extent = [-82, -34, -34, 13]   # lon_min, lon_max, lat_min, lat_max

def plot_panel(ax, gdf, stats, title):
    ax.set_extent(extent, crs=ccrs.PlateCarree())

    # OSM base map (zoom level 5 covers the continental extent at low cost)
    ax.add_image(OSM(), OSM_ZOOM)

    # Country borders
    borders = cfeature.NaturalEarthFeature(
        'cultural', 'admin_0_countries', '10m',
        edgecolor='black', facecolor='none')
    ax.add_feature(borders, linewidth=0.5, zorder=2)

    # Plot stations one-by-one (mirrors the original loop / marker style)
    geo = ccrs.Geodetic()
    for lon, lat, k in zip(gdf.geometry.x, gdf.geometry.y, gdf['kge_capped']):
        ax.plot(lon, lat, marker='o', color=kge_color(k),
                markersize=3.0, markeredgewidth=0.5,
                markeredgecolor='black', transform=geo, zorder=3)

    # Title
    ax.set_title(title, fontsize=14, pad=8)

    # Scale bar (placed in lower-left area, in PlateCarree coords)
    draw_scale_bar(ax, [-80, -31], 1500, ccrs.PlateCarree())

    # Stats box (lower-right of the panel) - LaTeX styled to match original
    text_latex = (
        rf"\begin{{array}}{{l}}"
        rf"n: {stats['n']} \\"
        rf"\text{{Median KGE}}: {stats['median']} \\"
        rf"\text{{IQR KGE}}: ({stats['p25']},\, {stats['p75']})"
        rf"\end{{array}}"
    )
    ax.text(0.97, 0.04, f'${text_latex}$',
            transform=ax.transAxes,
            ha='right', va='bottom',
            fontsize=11, color='white',
            bbox=dict(facecolor='black', alpha=0.55,
                      edgecolor='none', pad=6),
            zorder=10)

    # North arrow
    north_arrow(ax)

# ---------- Build figure ----------
fig, axes = plt.subplots(
    1, 2, figsize=(15, 8.5),
    subplot_kw={'projection': OSM().crs},
)

time.sleep(1)   # gentle throttle before tile requests

plot_panel(axes[0], before, stats_before, 'Original Simulation')
plot_panel(axes[1], after,  stats_after,  'Bias Corrected Simulation')

# ---------- Shared legend (LaTeX) ----------
legend_labels = [
    r'$KGE < -0.41$',
    r'$-0.41 \leq KGE < 0.00$',
    r'$0.00 \leq KGE < 0.50$',
    r'$0.50 \leq KGE < 0.75$',
    r'$0.75 \leq KGE \leq 1.00$',
]
legend_colors = ['red', 'orange', 'yellow', 'lightgreen', 'green']

handles = [
    plt.Line2D([], [], marker='o', color=c, label=l, linestyle='None',
               markersize=7, markeredgewidth=0.5, markeredgecolor='black')
    for c, l in zip(legend_colors, legend_labels)
]
leg = fig.legend(handles=handles,
                 title=r'$\textbf{KGE Categories}$',
                 loc='lower center', ncol=5,
                 bbox_to_anchor=(0.5, 0.0),
                 frameon=True, fontsize=10, title_fontsize=11,
                 handletextpad=0.4, columnspacing=1.2)
leg.get_frame().set_edgecolor('#444')
leg.get_frame().set_linewidth(0.6)

plt.subplots_adjust(left=0.02, right=0.98, top=0.96,
                    bottom=0.10, wspace=0.04)

out = r'E:\PhD\2022_Fall\Dissertation_v14\Figures\South_America\Bias_Correction_Map_Amazonia.png'
plt.savefig(out, dpi=300, facecolor='white')
print('Saved:', out)
print('Before:', stats_before)
print('After:',  stats_after)