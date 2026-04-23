import os
import pandas as pd
import matplotlib.pyplot as plt

# ============================================================
# INPUTS
# ============================================================
BASE_DIR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"

SEASONAL = os.path.join(BASE_DIR, "seasonal_metrics_by_station.csv")
GLOBAL = os.path.join(BASE_DIR, "global_metrics_by_station.csv")

OUT_FIG = os.path.join(BASE_DIR, "Fig_Classes_x_Metrics_Comparison.png")

# Orders
class_order = ["A", "B", "C", "D", "E", "F"]
class_labels = {
    "A": "North America",
    "B": "South America",
    "C": "Europe",
    "D": "Africa",
    "E": "Asia",
    "F": "Oceania",
}

group_order = ["dry", "shoulder", "wet", "global"]
method_order = ["or", "pre", "mfdc"]

method_labels = {"or": "Original", "pre": "Pre-routing BC", "mfdc": "MFDC-QM"}
method_colors = {"or": "tab:blue", "pre": "tab:orange", "mfdc": "tab:green"}

metric_info = [
    ("kge", "KGE"),
    ("beta", "Bias ratio (β)"),
    ("gamma", "Variability ratio (γ)"),
    ("r", "Correlation coefficient (r)"),
]

# ============================================================
# LOAD & COMBINE
# ============================================================
seasonal_df = pd.read_csv(SEASONAL)
global_df = pd.read_csv(GLOBAL)
global_df["group"] = "global"

df = pd.concat([seasonal_df, global_df], ignore_index=True)

df["group"] = pd.Categorical(df["group"], categories=group_order, ordered=True)
df["method"] = pd.Categorical(df["method"], categories=method_order, ordered=True)
df["class"] = pd.Categorical(df["class"], categories=class_order, ordered=True)

# ============================================================
# PLOTTING STYLE
# ============================================================
median_style = dict(color="darkred", linewidth=1.4)
box_width = 0.18

n_rows = len(class_order)
n_cols = len(metric_info)

fig, axes = plt.subplots(
    n_rows,
    n_cols,
    figsize=(4.5 * n_cols, 2.8 * n_rows),
    sharex=True
)

# ============================================================
# MAIN LOOP
# ============================================================
for i, cls in enumerate(class_order):
    cls_df = df[df["class"] == cls]

    for j, (metric, metric_label) in enumerate(metric_info):
        ax = axes[i, j]
        base_positions = list(range(len(group_order)))

        for k, method in enumerate(method_order):
            data = [
                cls_df[
                    (cls_df["group"] == g) &
                    (cls_df["method"] == method)
                ][metric].dropna()
                for g in group_order
            ]

            positions = [p + (k - 1) * box_width for p in base_positions]

            bp = ax.boxplot(
                data,
                positions=positions,
                widths=box_width,
                patch_artist=True,
                showfliers=False,
                medianprops=median_style
            )

            for patch in bp["boxes"]:
                patch.set_facecolor(method_colors[method])
                patch.set_alpha(0.9)

        # Axis formatting
        if i == 0:
            ax.set_title(metric_label, fontsize=11)

        if j == 0:
            ax.set_ylabel(class_labels[cls], fontsize=11)

        if i == n_rows - 1:
            ax.set_xticks(base_positions)
            ax.set_xticklabels(group_order, fontsize=9)
        else:
            ax.set_xticks([])

        ax.grid(axis="y", linestyle=":", alpha=0.6)

        if metric == "kge":
            ax.axhline(0, linestyle="--", color="gray", linewidth=0.8)
        else:
            ax.axhline(0, linestyle="--", color="gray", linewidth=0.5)

# ============================================================
# LEGEND
# ============================================================
handles = [
    plt.Line2D([0], [0], color=method_colors[m], lw=8)
    for m in method_order
]
labels = [method_labels[m] for m in method_order]

fig.legend(
    handles,
    labels,
    loc="upper center",
    ncol=3,
    frameon=False,
    fontsize=11
)

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig(OUT_FIG, dpi=300)
plt.close()

print(f"✔ Figure saved: {OUT_FIG}")
