import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from matplotlib.patches import FancyArrowPatch

# -------------------------------
# Paths
# -------------------------------
CSV_METRICS = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Error_Metrics\Metrics_GEOGloWS_v1.csv"

# -------------------------------
# Load data
# -------------------------------
df = pd.read_csv(CSV_METRICS)
df = df.dropna(subset=["kge_or", "kge_pre", "kge_mfdc"])
print(f"Total stations: {len(df)}")

# -------------------------------
# KGE categories
# -------------------------------
def classify_kge(kge):
    if kge < -0.41:
        return "Very Poor"
    elif kge < 0:
        return "Poor"
    elif kge < 0.50:
        return "Satisfactory"
    else:
        return "Good"

cat_order  = ["Very Poor", "Poor", "Satisfactory", "Good"]
cat_colors = ["#d7191c", "#fdae61", "#ffffbf", "#1a9641"]

df["cat_or"]   = df["kge_or"].apply(classify_kge)
df["cat_pre"]  = df["kge_pre"].apply(classify_kge)
df["cat_mfdc"] = df["kge_mfdc"].apply(classify_kge)

# -------------------------------
# Transition label per station
# -------------------------------
def transition_label(cat_from, cat_to):
    i_from = cat_order.index(cat_from)
    i_to   = cat_order.index(cat_to)
    if i_to > i_from:
        return "Improved"
    elif i_to < i_from:
        return "Degraded"
    else:
        return "No change"

df["trans_pre"]  = df.apply(lambda r: transition_label(r["cat_or"], r["cat_pre"]),  axis=1)
df["trans_mfdc"] = df.apply(lambda r: transition_label(r["cat_or"], r["cat_mfdc"]), axis=1)

# -------------------------------
# Summary statistics
# -------------------------------
print("\n--- Transition summary: Baseline → Pre-routing ---")
print(df["trans_pre"].value_counts())
print("\n--- Transition summary: Baseline → MFDC-QM ---")
print(df["trans_mfdc"].value_counts())

# -------------------------------
# Transition matrices
# -------------------------------
def build_matrix(df, col_from, col_to):
    mat = pd.DataFrame(0, index=cat_order, columns=cat_order)
    for _, row in df.iterrows():
        mat.loc[row[col_from], row[col_to]] += 1
    return mat

mat_pre  = build_matrix(df, "cat_or", "cat_pre")
mat_mfdc = build_matrix(df, "cat_or", "cat_mfdc")

print("\n--- Transition matrix: Baseline → Pre-routing ---")
print(mat_pre)
print("\n--- Transition matrix: Baseline → MFDC-QM ---")
print(mat_mfdc)

mat_pre.to_csv("transition_matrix_pre.csv")
mat_mfdc.to_csv("transition_matrix_mfdc.csv")

# -------------------------------
# Figure 1: Transition matrices (heatmaps)
# -------------------------------
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

for ax, mat, title in zip(
    axes,
    [mat_pre, mat_mfdc],
    ["Baseline → Pre-routing", "Baseline → MFDC-QM"]
):
    # Normalize by row (% of stations in each baseline category)
    mat_pct = mat.div(mat.sum(axis=1), axis=0) * 100

    im = ax.imshow(mat_pct.values, cmap="YlOrRd", vmin=0, vmax=100, aspect="auto")

    # Annotate cells with count and percentage
    for i in range(len(cat_order)):
        for j in range(len(cat_order)):
            count = mat.values[i, j]
            pct   = mat_pct.values[i, j]
            color = "white" if pct > 60 else "black"
            ax.text(j, i, f"{count}\n({pct:.1f}%)",
                    ha="center", va="center", fontsize=9,
                    color=color, fontweight="bold")

    ax.set_xticks(range(len(cat_order)))
    ax.set_yticks(range(len(cat_order)))
    ax.set_xticklabels(cat_order, fontsize=10)
    ax.set_yticklabels(cat_order, fontsize=10)
    ax.set_xlabel("After correction", fontsize=11)
    ax.set_ylabel("Baseline (before correction)", fontsize=11)
    ax.set_title(title, fontsize=12, fontweight="bold")

    # Highlight diagonal (no change)
    for k in range(len(cat_order)):
        ax.add_patch(plt.Rectangle(
            (k - 0.5, k - 0.5), 1, 1,
            fill=False, edgecolor="black", linewidth=2
        ))

    plt.colorbar(im, ax=ax, label="% of baseline category", shrink=0.8)

plt.tight_layout()
fig.savefig("kge_transition_matrices.png", dpi=300, bbox_inches="tight")
print("\nSaved: kge_transition_matrices.png")
plt.close(fig)

# -------------------------------
# Figure 2: Spatial maps of transitions
# -------------------------------
trans_colors = {
    "Improved":  "#1a9641",
    "No change": "#d3d3d3",
    "Degraded":  "#d7191c",
}

fig, axes = plt.subplots(
    2, 1, figsize=(18, 12),
    subplot_kw={"projection": ccrs.PlateCarree()}
)

for ax, trans_col, title in zip(
    axes,
    ["trans_pre", "trans_mfdc"],
    ["Baseline → Pre-routing", "Baseline → MFDC-QM"]
):
    ax.set_extent([-180, 180, -60, 80], crs=ccrs.PlateCarree())
    ax.coastlines(linewidth=0.5)
    ax.add_feature(cfeature.BORDERS, linewidth=0.3)
    ax.add_feature(cfeature.LAND, alpha=0.1)
    ax.add_feature(cfeature.OCEAN, alpha=0.15)

    for trans, color in trans_colors.items():
        subset = df[df[trans_col] == trans]
        ax.scatter(
            subset["Longitude"], subset["Latitude"],
            c=color, s=4, alpha=0.7,
            transform=ccrs.PlateCarree(),
            label=f"{trans} (n={len(subset)})",
            zorder=5
        )

    ax.legend(loc="lower left", fontsize=9, markerscale=3)
    ax.set_title(title, fontsize=12, fontweight="bold", loc="left")

plt.tight_layout()
fig.savefig("kge_transition_maps.png", dpi=300, bbox_inches="tight")
print("Saved: kge_transition_maps.png")
plt.close(fig)

# -------------------------------
# Figure 3: Transition summary by region
# -------------------------------
region_map = {
    "A": "A",
    "B": "B",
    "C": "C",
    "D": "D",
    "E": "E",
    "F": "F",
}

trans_palette = {
    "Improved":  "#1a9641",
    "No change": "#d3d3d3",
    "Degraded":  "#d7191c",
}

for trans_col, title in zip(
    ["trans_pre", "trans_mfdc"],
    ["Baseline → Pre-routing", "Baseline → MFDC-QM"]
):
    # Build regional summary
    rows = []
    for cls, reg in region_map.items():
        sub = df[df["class"] == cls]
        total = len(sub)
        if total == 0:
            continue
        for trans in ["Improved", "No change", "Degraded"]:
            n   = (sub[trans_col] == trans).sum()
            pct = 100 * n / total
            rows.append({"Region": reg, "Transition": trans, "n": n, "pct": pct, "total": total})

    df_reg = pd.DataFrame(rows)
    print(f"\n--- Regional summary: {title} ---")
    print(df_reg.pivot_table(index="Region", columns="Transition", values="pct").round(1))

    # Stacked bar chart
    fig, ax = plt.subplots(figsize=(12, 6))
    regions_ordered = list(region_map.values())
    x = np.arange(len(regions_ordered))
    width = 0.5
    bottom = np.zeros(len(regions_ordered))

    for trans in ["Improved", "No change", "Degraded"]:
        vals = [
            df_reg[(df_reg["Region"] == reg) & (df_reg["Transition"] == trans)]["pct"].values[0]
            if len(df_reg[(df_reg["Region"] == reg) & (df_reg["Transition"] == trans)]) > 0 else 0
            for reg in regions_ordered
        ]
        bars = ax.bar(x, vals, width=width, bottom=bottom,
                      color=trans_palette[trans], label=trans,
                      edgecolor="white", linewidth=0.5)
        # Annotate percentage inside bar if > 5%
        for xi, (v, b) in enumerate(zip(vals, bottom)):
            if v > 5:
                ax.text(xi, b + v / 2, f"{v:.1f}%",
                        ha="center", va="center", fontsize=8,
                        color="black", fontweight="bold")
        bottom += np.array(vals)

    # Annotate total n above each bar
    for xi, reg in enumerate(regions_ordered):
        total = df_reg[df_reg["Region"] == reg]["total"].max()
        ax.text(xi, 101, f"n={total}", ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(regions_ordered, fontsize=10)
    ax.set_ylabel("Percentage of stations (%)", fontsize=11)
    ax.set_ylim(0, 110)
    ax.set_title(f"KGE category transitions by region: {title}",
                 fontsize=12, fontweight="bold", loc="left")
    ax.legend(loc="upper right", fontsize=10)
    ax.axhline(100, color="black", linewidth=0.5, linestyle="--")

    plt.tight_layout()
    fname = f"kge_transition_by_region_{'pre' if 'Pre' in title else 'mfdc'}.png"
    fig.savefig(fname, dpi=300, bbox_inches="tight")
    print(f"Saved: {fname}")
    plt.close(fig)