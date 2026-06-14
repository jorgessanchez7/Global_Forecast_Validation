import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.patches as mpatches

# ============================================================
# INPUT — change component by uncommenting the right lines
# ============================================================
CSV_PATH = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Error_Metrics\Metrics_GEOGloWS_v1.csv"

# Uncomment the component you want to generate:

# --- BIAS RATIO ---
#OUTPUT_FIG = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_7a_bias_Maps.png"
#var_list   = ["bias_or", "bias_pre", "bias_mfdc"]
#titles     = ["(a1) Baseline model", "(b1) Pre-routing bias-corrected model", "(c1) MFDC-QM model"]
#comp_type  = "bias"

# --- VARIABILITY RATIO ---
#OUTPUT_FIG = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_7b_variability_Maps.png"
#var_list   = ["variability_or", "variability_pre", "variability_mfdc"]
#titles     = ["(a2) Baseline model", "(b2) Pre-routing bias-corrected model", "(c2) MFDC-QM model"]
#comp_type  = "variability"

# --- PEARSON CORRELATION ---
OUTPUT_FIG = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_7c_correlation_Maps.png"
var_list   = ["correlation_or", "correlation_pre", "correlation_mfdc"]
titles     = ["(a3) Baseline model", "(b3) Pre-routing bias-corrected model", "(c3) MFDC-QM model"]
comp_type  = "correlation"

# ============================================================
# CLASSIFICATION FUNCTIONS
# Change 1: all ranges now use en dash (–) instead of hyphen (-)
# ============================================================
def classify_bias(bias):
    if pd.isna(bias):
        return "No data"
    if bias < 0.20:
        return "0.0 – 0.2"
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
    "0.0 – 0.2": "red",
    "0.2 – 0.6": "orange",
    "0.6 – 0.8": "yellow",
    "0.8 – 0.9": "lightgreen",
    "0.9 – 1.1": "green",
    "1.1 – 1.2": "lightgreen",
    "1.2 – 1.4": "yellow",
    "1.4 – 2.0": "orange",
    "≥ 2.0":     "red",
    "No data":   "lightgrey",
}

legend_patches_bias = [
    mpatches.Patch(color="red",        label="0.0 – 0.2"),
    mpatches.Patch(color="orange",     label="0.2 – 0.6"),
    mpatches.Patch(color="yellow",     label="0.6 – 0.8"),
    mpatches.Patch(color="lightgreen", label="0.8 – 0.9"),
    mpatches.Patch(color="green",      label="0.9 – 1.1"),
    mpatches.Patch(color="lightgreen", label="1.1 – 1.2"),
    mpatches.Patch(color="yellow",     label="1.2 – 1.4"),
    mpatches.Patch(color="orange",     label="1.4 – 2.0"),
    mpatches.Patch(color="red",        label="≥ 2.0"),
]

def classify_variability(v):
    return classify_bias(v)

color_map_variability  = color_map_bias
legend_patches_variability = legend_patches_bias

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
    "< 0.0":    "#722F37",
    "0.0 – 0.2": "red",
    "0.2 – 0.4": "orange",
    "0.4 – 0.6": "yellow",
    "0.6 – 0.8": "lightgreen",
    "0.8 – 1.0": "green",
    "No data":   "lightgrey",
}

legend_patches_correlation = [
    mpatches.Patch(color="#722F37",    label="< 0.0"),
    mpatches.Patch(color="red",        label="0.0 – 0.2"),
    mpatches.Patch(color="orange",     label="0.2 – 0.4"),
    mpatches.Patch(color="yellow",     label="0.4 – 0.6"),
    mpatches.Patch(color="lightgreen", label="0.6 – 0.8"),
    mpatches.Patch(color="green",      label="0.8 – 1.0"),
]

# ============================================================
# LOAD DATA
# ============================================================
df  = pd.read_csv(CSV_PATH)
gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df["Longitude"], df["Latitude"]),
    crs="EPSG:4326"
)

# ============================================================
# FIGURE SETUP (25 x 33 cm)
# ============================================================
fig, axes = plt.subplots(
    len(var_list), 1,
    figsize=(25 / 2.54, 33 / 2.54),
    subplot_kw={"projection": ccrs.PlateCarree()},
)

# ============================================================
# PLOT PANELS
# ============================================================
stats = []

for ax, var, title in zip(axes, var_list, titles):

    gdf_plot = gdf.copy()

    if comp_type == "bias":
        gdf_plot["class"] = gdf_plot[var].apply(classify_bias)
        gdf_plot["color"] = gdf_plot["class"].map(color_map_bias)
        gdf_plot = gdf_plot.assign(dist=1.0 - np.abs(gdf_plot[var])).sort_values("dist")
        legend_patches = legend_patches_bias
        legend_title   = "bias ratio"
        stat_label     = "Median bias ratio"

    elif comp_type == "variability":
        gdf_plot["class"] = gdf_plot[var].apply(classify_variability)
        gdf_plot["color"] = gdf_plot["class"].map(color_map_variability)
        gdf_plot = gdf_plot.assign(dist=1.0 - np.abs(gdf_plot[var])).sort_values("dist")
        legend_patches = legend_patches_variability
        legend_title   = "variability ratio"
        stat_label     = "Median variability ratio"

    else:  # correlation
        gdf_plot["class"] = gdf_plot[var].apply(classify_correlation)
        gdf_plot["color"] = gdf_plot["class"].map(color_map_correlation)
        gdf_plot = gdf_plot.sort_values(var)
        legend_patches = legend_patches_correlation
        legend_title   = "Pearson Correlation"
        stat_label     = "Median Pearson correlation"

    vals = gdf_plot[var].dropna()
    stats.append({
        "n":   len(vals),
        "med": round(np.median(vals), 2),
        "p25": round(np.percentile(vals, 25), 2),
        "p75": round(np.percentile(vals, 75), 2),
    })

    ax.set_extent([-180, 180, -60, 80], crs=ccrs.PlateCarree())
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linewidth=0.5)
    ax.add_feature(cfeature.LAND,  alpha=0.1)
    ax.add_feature(cfeature.OCEAN, alpha=0.1)

    gdf_plot.plot(
        ax=ax,
        color=gdf_plot["color"],
        edgecolor=gdf_plot["color"],
        markersize=5,
        linewidth=0.05,
        transform=ccrs.PlateCarree(),
    )

    ax.set_title(title, fontsize=14, loc="left")

    ax.legend(
        handles=legend_patches,
        title=legend_title,
        loc="lower left",
        frameon=True,
    )

    # Change 2: thousands separator {:,}
    s = stats[-1]
    txt = (
        f"n = {s['n']:,}\n"
        f"{stat_label} = {s['med']}\n"
        f"IQR = ({s['p25']}, {s['p75']})"
    )
    ax.text(
        -35, -55, txt,
        fontsize=10,
        color="white",
        bbox=dict(facecolor="black", alpha=0.5),
        transform=ccrs.PlateCarree()
    )

# ============================================================
# SAVE
# ============================================================
plt.tight_layout()
fig.savefig(OUTPUT_FIG, dpi=300, bbox_inches="tight")
plt.close(fig)

print(f"✔ Figure saved: {OUTPUT_FIG}")