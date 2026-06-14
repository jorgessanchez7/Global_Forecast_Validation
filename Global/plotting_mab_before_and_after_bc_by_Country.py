"""
Bias correction comparison maps - ONE INDEPENDENT FIGURE PER COUNTRY.
Reads the v2 CSV and writes 4 separate PNGs (Brazil, Colombia, Ecuador, Peru),
each with its own Before / After pair, scale bar, north arrow, stats box,
and KGE-categories legend.
"""
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.io.img_tiles import OSM

# ---------- LaTeX rendering ----------
plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.preamble'] = r'\usepackage{amsmath}'

# ---------- Load and filter data ----------
csv_path = r"Metrics_GEOGloWS_v1_Q.csv"
df = pd.read_csv(csv_path)

selected_countries = ['Brazil', 'Colombia', 'Ecuador', 'Peru', 'Togo', 'Czech Republic', 'Uruguay', 'Nigeria', 'Vietnam']
df = df[df['country_name'].isin(selected_countries)].copy()
df = df.dropna(subset=['Latitude', 'Longitude', 'KGE', 'Corrected KGE'])

# ---------- Per-country configuration ----------
# Extents: (lon_min, lon_max, lat_min, lat_max)
# Colombia and Peru are extended east into the Amazon so the stats box has
# empty area to sit on without covering stations.

COUNTRY_CFG = {
    # --- Sudamérica (las que ya teníamos) ---
    'Brazil':         {'extent': (-74, -34, -34,   6), 'scale_km': 1000, 'zoom': 6},
    'Colombia':       {'extent': (-80, -62,  -5,  13), 'scale_km':  400, 'zoom': 7},
    'Ecuador':        {'extent': (-81, -75,  -5,   2), 'scale_km':  200, 'zoom': 8},
    'Peru':           {'extent': (-82, -62, -19,   0), 'scale_km':  400, 'zoom': 7},
    'Uruguay':        {'extent': (-59, -52, -36, -29), 'scale_km':  200, 'zoom': 7},

    # --- África ---
    'Togo':           {'extent': ( -1,   4,   5,  12), 'scale_km':  200, 'zoom': 8},
    'Nigeria':        {'extent': (  2,  15,   3,  14), 'scale_km':  400, 'zoom': 6},

    # --- Europa ---
    'Czech Republic': {'extent': ( 11,  20,  47,  52), 'scale_km':  200, 'zoom': 7},

    # --- Asia ---
    'Vietnam':        {'extent': (101, 115,   8,  24), 'scale_km':  400, 'zoom': 6},
}

# ---------- KGE -> color ----------
def kge_color(k):
    if k < -0.41:        return 'red'
    elif k < 0.0:        return 'orange'
    elif k < 0.5:        return 'yellow'
    elif k < 0.75:       return 'lightgreen'
    else:                return 'green'

# ---------- Stats helper ----------
def panel_stats(d):
    k = d['kge_capped'].values
    return {
        'n':      len(k),
        'median': round(float(np.median(k)), 2),
        'p25':    round(float(np.percentile(k, 25)), 2),
        'p75':    round(float(np.percentile(k, 75)), 2),
    }

# ---------- Scale bar (adaptive to the panel's extent) ----------
def draw_scale_bar(ax, extent, length_km):
    lon_min, lon_max, lat_min, lat_max = extent
    lat_range = lat_max - lat_min
    x = lon_min + 0.05 * (lon_max - lon_min)
    y = lat_min + 0.08 * lat_range
    scale_length_deg = length_km / (111.32 * np.cos(np.radians(y)))
    projection = ccrs.PlateCarree()
    ax.plot([x, x + scale_length_deg], [y, y],
            transform=projection, color='black', linewidth=3,
            solid_capstyle='butt')
    tick_h = 0.015 * lat_range
    for xt in (x, x + scale_length_deg):
        ax.plot([xt, xt], [y - tick_h, y + tick_h],
                transform=projection, color='black', linewidth=2)
    ax.text(x + scale_length_deg / 2, y - 0.04 * lat_range,
            f'{length_km} km',
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

# ---------- Plot a single panel (before OR after) for one country ----------
def plot_panel(ax, data, stats, title, cfg):
    extent = cfg['extent']
    zoom   = cfg['zoom']
    sb_len = cfg['scale_km']

    ax.set_extent(extent, crs=ccrs.PlateCarree())
    ax.add_image(OSM(), zoom)

    borders = cfeature.NaturalEarthFeature(
        'cultural', 'admin_0_countries', '10m',
        edgecolor='black', facecolor='none')
    ax.add_feature(borders, linewidth=0.5, zorder=2)

    geo = ccrs.Geodetic()
    for lon, lat, k in zip(data['Longitude'], data['Latitude'], data['kge_capped']):
        ax.plot(lon, lat, marker='o', color=kge_color(k),
                markersize=5.0, markeredgewidth=0.5,
                markeredgecolor='black', transform=geo, zorder=3)

    ax.set_title(title, fontsize=14, pad=8)
    draw_scale_bar(ax, extent, sb_len)

    # Dark stats box, lower-right corner, LaTeX styled
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

    north_arrow(ax)

# ---------- One full figure (two panels: before/after) for one country ----------
def make_country_figure(country):
    cfg = COUNTRY_CFG[country]

    sub = df[df['country_name'] == country]
    before = sub[['Latitude', 'Longitude', 'KGE']].rename(columns={'KGE': 'kge'}).copy()
    after  = sub[['Latitude', 'Longitude', 'Corrected KGE']].rename(columns={'Corrected KGE': 'kge'}).copy()
    before['kge_capped'] = before['kge'].clip(upper=1.0)
    after['kge_capped']  = after['kge'].clip(upper=1.0)

    s_before = panel_stats(before)
    s_after  = panel_stats(after)

    fig, axes = plt.subplots(
        1, 2, figsize=(14, 8),
        subplot_kw={'projection': OSM().crs},
    )

    time.sleep(1)   # gentle throttle before tile requests

    plot_panel(axes[0], before, s_before, 'Original Simulation',      cfg)
    plot_panel(axes[1], after,  s_after,  'Bias Corrected Simulation', cfg)

    # Shared legend (LaTeX) for this figure
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

    out = rf'E:\PhD\2022_Fall\Dissertation_v14\Figures\World\Bias_Correction_Map_{country}_v2.png'
    plt.savefig(out, dpi=300, facecolor='white')
    plt.close(fig)
    print(f'{country}: saved -> {out}')
    print(f'  Before: {s_before}')
    print(f'  After:  {s_after}')

# ---------- Run for all four countries ----------
for country in selected_countries:
    make_country_figure(country)