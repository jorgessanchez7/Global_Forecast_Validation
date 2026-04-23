import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.patches as mpatches

# ============================================================
# INPUT
# ============================================================
CSV_PATH = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Error_Metrics\Metrics_GEOGloWS_v1.csv"
OUTPUT_FIG = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_6_KGE_components.png"

# ============================================================
# VARIABLES
# ============================================================
models = [
    ("Baseline model", "or"),
    ("Pre-routing bias-corrected model", "pre"),
    ("MFDC-QM model", "mfdc"),
]

components = [
    ("Bias ratio (β)", "bias"),
    ("Variability ratio (γ)", "variability"),
    ("Pearson correlation (r)", "correlation"),
]

# ============================================================
# CLASSIFICATION FUNCTIONS (UNCHANGED)
# ============================================================
def classify_bias(bias):
    if pd.isna(bias):
        return "No data"
    if bias < 0.20:
        return "0.0 - 0.2"
    elif 0.20 <= bias < 0.60:
        return "0.2 – 0.6"
    elif 0.60 <= bias < 0.80:
        return "0.6 – 0.8"
    elif 0.80 <= bias < 0.90:
        return "0.8 – 0.9"
    elif 0.90 <= bias < 1.10:
        return "0.9 – 1.1"
    elif 1.10 <= bias < 1.20:
        return "1.1 – 1.2"
    elif 1.20 <= bias < 1.40:
        return "1.2 – 1.4"
    elif 1.4 <= bias < 2.0:
        return "1.4 – 2.0"
    else:
        return "≥ 2.0"

color_map_bias = {
    "0.0 - 0.2": "red",
    "0.2 – 0.6": "orange",
    "0.6 – 0.8": "yellow",
    "0.8 – 0.9": "lightgreen",
    "0.9 – 1.1": "green",
    "1.1 – 1.2": "lightgreen",
    "1.2 – 1.4": "yellow",
    "1.4 – 2.0": "orange",
    "≥ 2.0": "red",
    "No data": "lightgrey",
}

def classify_variability(v):
    return classify_bias(v)

color_map_variability = color_map_bias

def classify_correlation(r):
    if pd.isna(r):
        return "No data"
    if r < 0.0:
        return "< 0.0"
    elif 0.00 <= r < 0.20:
        return "0.0 – 0.2"
    elif 0.20 <= r < 0.40:
        return "0.2 – 0.4"
    elif 0.40 <= r < 0.60:
        return "0.4 – 0.6"
    elif 0.60 <= r < 0.80:
        return "0.6 – 0.8"
    else:
        return "0.8 – 1.0"

color_map_correlation = {
    "< 0.0": "black",
    "0.0 – 0.2": "red",
    "0.2 – 0.4": "orange",
    "0.4 – 0.6": "yellow",
    "0.6 – 0.8": "lightgreen",
    "0.8 – 1.0": "green",
    "No data": "lightgrey",
}

# ============================================================
# LOAD DATA
# ============================================================
df = pd.read_csv(CSV_PATH)

gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df["Longitude"], df["Latitude"]),
    crs="EPSG:4326"
)

# ============================================================
# FIGURE SETUP (3 x 3)
# ============================================================
fig, axes = plt.subplots(
    3, 3,
    figsize=(25 / 2.54, 33 / 2.54),
    subplot_kw={"projection": ccrs.PlateCarree()},
)

# ============================================================
# PLOTTING
# ============================================================
for i, (model_name, suf) in enumerate(models):
    for j, (comp_name, comp) in enumerate(components):

        ax = axes[i, j]
        var = f"{comp}_{suf}"

        gdf_plot = gdf.copy()

        if comp == "bias":
            gdf_plot["class"] = gdf_plot[var].apply(classify_bias)
            gdf_plot["color"] = gdf_plot["class"].map(color_map_bias)
            gdf_plot = gdf_plot.assign(dist=1.0 - np.abs(gdf_plot[var])).sort_values("dist")

        elif comp == "variability":
            gdf_plot["class"] = gdf_plot[var].apply(classify_variability)
            gdf_plot["color"] = gdf_plot["class"].map(color_map_variability)
            gdf_plot = gdf_plot.assign(dist=1.0 - np.abs(gdf_plot[var])).sort_values("dist")

        else:  # correlation
            gdf_plot["class"] = gdf_plot[var].apply(classify_correlation)
            gdf_plot["color"] = gdf_plot["class"].map(color_map_correlation)
            gdf_plot = gdf_plot.sort_values(var)

        # ---- MAP STYLE (UNCHANGED)
        ax.set_extent([-180, 180, -60, 80])
        ax.coastlines()
        ax.add_feature(cfeature.BORDERS, linewidth=0.5)
        ax.add_feature(cfeature.LAND, alpha=0.1)
        ax.add_feature(cfeature.OCEAN, alpha=0.1)

        gdf_plot.plot(
            ax=ax,
            color=gdf_plot["color"],
            edgecolor=gdf_plot["color"],
            markersize=5,
            linewidth=0.05,
            transform=ccrs.PlateCarree(),
        )

        if i == 0:
            ax.set_title(comp_name, fontsize=13)
        if j == 0:
            ax.text(
                -0.08, 0.5, model_name,
                transform=ax.transAxes,
                rotation=90,
                va="center",
                ha="right",
                fontsize=13,
            )

# ============================================================
# SAVE
# ============================================================
plt.tight_layout()
fig.savefig(OUTPUT_FIG, dpi=300, bbox_inches="tight")
plt.close(fig)

print(f"✔ Figure saved: {OUTPUT_FIG}")
