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
#OUTPUT_FIG = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_5_KGE_Maps.png"
#OUTPUT_FIG = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_6a_bias_Maps.png"
OUTPUT_FIG = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_6b_variability_Maps.png"
#OUTPUT_FIG = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_6c_correlation_Maps.png"

# Variables to plot
#var_list = ["kge_or", "kge_pre", "kge_mfdc"]
#var_list = ["bias_or", "bias_pre", "bias_mfdc"]
var_list = ["variability_or", "variability_pre", "variability_mfdc"]
#var_list = ["correlation_or", "correlation_pre", "correlation_mfdc"]
titles = [
    "(a) Baseline model",
    "(b) Pre-routing bias-corrected model",
    "(c) MFDC-QM model",
]

# ============================================================
# KGE CLASSIFICATION
# ============================================================
def classify_kge(kge):
    if pd.isna(kge):
        return "No data"
    if kge < -0.41:
        return "< -0.41"
    elif -0.41 <= kge < 0.0:
        return "-0.41 – 0.00"
    elif 0.0 <= kge < 0.5:
        return "0.00 – 0.50"
    elif 0.5 <= kge < 0.75:
        return "0.50 – 0.75"
    else:
        return "≥ 0.75"

color_map_kge = {
    "< -0.41": "red",
    "-0.41 – 0.00": "orange",
    "0.00 – 0.50": "yellow",
    "0.50 – 0.75": "lightgreen",
    "≥ 0.75": "green",
    "No data": "lightgrey",
}

# ============================================================
# BIAS RATIO CLASSIFICATION
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

# ============================================================
# VARIABILITY RATIO CLASSIFICATION
# ============================================================
def classify_variability(variability):
    if pd.isna(variability):
        return "No data"
    if variability < 0.20:
        return "0.0 - 0.2"
    elif 0.20 <= variability < 0.60:
        return "0.2 – 0.6"
    elif 0.60 <= variability < 0.80:
        return "0.6 – 0.8"
    elif 0.80 <= variability < 0.90:
        return "0.8 – 0.9"
    elif 0.90 <= variability < 1.10:
        return "0.9 – 1.1"
    elif 1.10 <= variability < 1.20:
        return "1.1 – 1.2"
    elif 1.20 <= variability < 1.40:
        return "1.2 – 1.4"
    elif 1.4 <= variability < 2.0:
        return "1.4 – 2.0"
    else:
        return "≥ 2.0"

color_map_variability = {
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

# ============================================================
# PEARSON CORRELATION CLASSIFICATION
# ============================================================
def classify_correlation(correlation):
    if pd.isna(correlation):
        return "No data"
    if correlation < 0.0:
        return "< 0.0"
    elif 0.00 <= correlation < 0.20:
        return "0.0 – 0.2"
    elif 0.20 <= correlation < 0.40:
        return "0.2 – 0.4"
    elif 0.40 <= correlation < 0.60:
        return "0.4 – 0.6"
    elif 0.60 <= correlation < 0.80:
        return "0.6 – 0.8"
    elif 0.80 <= correlation <= 1.00:
        return "0.8 – 1.0"

color_map_correlation = {
    "< 0.0": "#722F37",
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
# FIGURE SETUP (25 x 33 cm)
# ============================================================
width_cm, height_cm = 25, 33
fig, axes = plt.subplots(
    len(var_list), 1,
    figsize=(width_cm / 2.54, height_cm / 2.54),
    subplot_kw={"projection": ccrs.PlateCarree()},
)

# ============================================================
# PLOT PANELS
# ============================================================
stats = []

for ax, var, title in zip(axes, var_list, titles):

    gdf_plot = gdf.copy()
    #gdf_plot["kge_class"] = gdf_plot[var].apply(classify_kge)
    #gdf_plot["color"] = gdf_plot["kge_class"].map(color_map_kge)
    #gdf_plot["bias_class"] = gdf_plot[var].apply(classify_bias)
    #gdf_plot["color"] = gdf_plot["bias_class"].map(color_map_bias)
    gdf_plot["variability_class"] = gdf_plot[var].apply(classify_variability)
    gdf_plot["color"] = gdf_plot["variability_class"].map(color_map_variability)
    #gdf_plot["correlation_class"] = gdf_plot[var].apply(classify_correlation)
    #gdf_plot["color"] = gdf_plot["correlation_class"].map(color_map_correlation)
    # --- Correct sorting depending on metric ---
    if var in ["bias_or", "bias_pre", "bias_mfdc", "variability_or", "variability_pre", "variability_mfdc"]:
        gdf_plot = gdf_plot.assign(dist_to_opt=1.0 - np.abs(gdf_plot[var])).sort_values("dist_to_opt")
    else:
        gdf_plot = gdf_plot.sort_values(var)

    #kge_vals = gdf_plot[var].dropna()
    #stats.append({
    #    "n": len(kge_vals),
    #    "median": round(np.median(kge_vals), 2),
    #    "p25": round(np.percentile(kge_vals, 25), 2),
    #    "p75": round(np.percentile(kge_vals, 75), 2),
    #})

    #bias_vals = gdf_plot[var].dropna()
    #stats.append({
    #    "n": len(bias_vals),
    #    "median": round(np.median(bias_vals), 2),
    #    "p25": round(np.percentile(bias_vals, 25), 2),
    #    "p75": round(np.percentile(bias_vals, 75), 2),
    #})

    variability_vals = gdf_plot[var].dropna()
    stats.append({
        "n": len(variability_vals),
        "median": round(np.median(variability_vals), 2),
        "p25": round(np.percentile(variability_vals, 25), 2),
        "p75": round(np.percentile(variability_vals, 75), 2),
    })

    #correlation_vals = gdf_plot[var].dropna()
    #stats.append({
    #    "n": len(correlation_vals),
    #    "median": round(np.median(correlation_vals), 2),
    #    "p25": round(np.percentile(correlation_vals, 25), 2),
    #    "p75": round(np.percentile(correlation_vals, 75), 2),
    #})

    ax.set_extent([-180, 180, -60, 80], crs=ccrs.PlateCarree())
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

    ax.set_title(title, fontsize=14, loc="left")

# ============================================================
# LEGEND + STATS
# ============================================================
#legend_patches = [
#    mpatches.Patch(color="red",        label="< -0.41"),
#    mpatches.Patch(color="orange",     label="-0.41 – 0.00"),
#    mpatches.Patch(color="yellow",     label="0.00 – 0.50"),
#    mpatches.Patch(color="lightgreen", label="0.50 – 0.75"),
#    mpatches.Patch(color="green",      label="≥ 0.75"),
#]

legend_patches = [
    mpatches.Patch(color="red",        label="0.0 - 0.2"),
    mpatches.Patch(color="orange",     label="0.2 – 0.6"),
    mpatches.Patch(color="yellow",     label="0.6 – 0.8"),
    mpatches.Patch(color="lightgreen", label="0.8 – 0.9"),
    mpatches.Patch(color="green",      label="0.9 – 1.1"),
    mpatches.Patch(color="lightgreen", label="1.1 – 1.2"),
    mpatches.Patch(color="yellow",     label="1.2 – 1.4"),
    mpatches.Patch(color="orange",     label="1.4 – 2.0"),
    mpatches.Patch(color="red",        label="≥ 2.0"),
]

#legend_patches = [
#    mpatches.Patch(color="red",        label="0.0 - 0.2"),
#    mpatches.Patch(color="orange",     label="0.2 – 0.4"),
#    mpatches.Patch(color="yellow",     label="0.4 – 0.6"),
#    mpatches.Patch(color="lightgreen", label="0.6 – 0.8"),
#    mpatches.Patch(color="green",      label="0.8 – 1.0"),
#]

for i, ax in enumerate(axes):
    #ax.legend(
    #    handles=legend_patches,
    #    title="KGE",
    #    loc="lower left",
    #    frameon=True,
    #)

    #ax.legend(
    #    handles=legend_patches,
    #    title="bias ratio",
    #    loc="lower left",
    #    frameon=True,
    #)

    ax.legend(
        handles=legend_patches,
        title="variability ratio",
        loc="lower left",
        frameon=True,
    )

    #ax.legend(
    #    handles=legend_patches,
    #    title="Pearson Correlation",
    #    loc="lower left",
    #    frameon=True,
    #)

    #txt = (
    #    f"n = {stats[i]['n']}\n"
    #    f"Median KGE = {stats[i]['median']}\n"
    #    f"IQR = ({stats[i]['p25']}, {stats[i]['p75']})"
    #)

    #txt = (
    #    f"n = {stats[i]['n']}\n"
    #    f"Median bias ratio = {stats[i]['median']}\n"
    #    f"IQR = ({stats[i]['p25']}, {stats[i]['p75']})"
    #)

    txt = (
        f"n = {stats[i]['n']}\n"
        f"Median variability ratio = {stats[i]['median']}\n"
        f"IQR = ({stats[i]['p25']}, {stats[i]['p75']})"
    )

    #txt = (
    #    f"n = {stats[i]['n']}\n"
    #    f"Median Pearson correlation = {stats[i]['median']}\n"
    #    f"IQR = ({stats[i]['p25']}, {stats[i]['p75']})"
    #)

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
