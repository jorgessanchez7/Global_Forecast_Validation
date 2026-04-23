import os
import pandas as pd
import matplotlib.pyplot as plt

# ============================================================
# INPUTS
# ============================================================
BASE_DIR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"

SEASONAL = os.path.join(BASE_DIR, "seasonal_metrics_by_station.csv")
GLOBAL = os.path.join(BASE_DIR, "global_metrics_by_station.csv")

#OUT_FIG = os.path.join(BASE_DIR, "Fig_Groups_Comparison_AllMetrics.png")
#OUT_FIG = os.path.join(BASE_DIR, "Fig_Groups_Comparison_AllMetrics_classA.png")
#OUT_FIG = os.path.join(BASE_DIR, "Fig_Groups_Comparison_AllMetrics_classB.png")
#OUT_FIG = os.path.join(BASE_DIR, "Fig_Groups_Comparison_AllMetrics_classC.png")
#OUT_FIG = os.path.join(BASE_DIR, "Fig_Groups_Comparison_AllMetrics_classD.png")
#OUT_FIG = os.path.join(BASE_DIR, "Fig_Groups_Comparison_AllMetrics_classE.png")
OUT_FIG = os.path.join(BASE_DIR, "Fig_Groups_Comparison_AllMetrics_classF.png")

group_order = ["dry", "shoulder", "wet", "global"]
method_order = ["or", "pre", "mfdc"]

method_labels = {"or": "Original", "pre": "Pre-routing BC", "mfdc": "MFDC-QM"}
method_colors = {"or": "tab:blue", "pre": "tab:orange", "mfdc": "tab:green"}

metric_info = [
    ("kge", "KGE Groups Comparison", "KGE"),
    ("r", "r Groups Comparison", "r (correlation)"),
    ("beta", "β Groups Comparison", "β (bias ratio)"),
    ("gamma", "γ Groups Comparison", "γ (variability ratio)"),
]

# ============================================================
# LOAD & COMBINE (two sources)
# ============================================================
seasonal_df = pd.read_csv(SEASONAL)
#seasonal_df = seasonal_df[seasonal_df['class'] == 'A']
#seasonal_df = seasonal_df[seasonal_df['class'] == 'B']
#seasonal_df = seasonal_df[seasonal_df['class'] == 'C']
#seasonal_df = seasonal_df[seasonal_df['class'] == 'D']
#seasonal_df = seasonal_df[seasonal_df['class'] == 'E']
seasonal_df = seasonal_df[seasonal_df['class'] == 'F']

global_df = pd.read_csv(GLOBAL)
#global_df = global_df[global_df['class'] == 'A']
#global_df = global_df[global_df['class'] == 'B']
#global_df = global_df[global_df['class'] == 'C']
#global_df = global_df[global_df['class'] == 'D']
#global_df = global_df[global_df['class'] == 'E']
global_df = global_df[global_df['class'] == 'F']
global_df["group"] = "global"

# Minimal safety checks
needed_cols = {"station_code", "comid", "region", "group", "method", "kge", "r", "beta", "gamma"}
missing_s = needed_cols - set(seasonal_df.columns)
missing_g = needed_cols - set(global_df.columns)
if missing_s:
    raise ValueError(f"seasonal file missing columns: {sorted(missing_s)}")
if missing_g:
    raise ValueError(f"global file missing columns: {sorted(missing_g)}")

df = pd.concat([seasonal_df, global_df], ignore_index=True)

# Enforce ordering
df["group"] = pd.Categorical(df["group"], categories=group_order, ordered=True)
df["method"] = pd.Categorical(df["method"], categories=method_order, ordered=True)

# ============================================================
# PLOTTING STYLE
# ============================================================
median_style = dict(color="darkred", linewidth=1.6)
box_width = 0.22

fig, axes = plt.subplots(2, 2, figsize=(15, 10))
axes = axes.flatten()

# ============================================================
# PLOT LOOP
# ============================================================
for ax, (metric, title, ylabel) in zip(axes, metric_info):

    base_positions = list(range(len(group_order)))

    # Plot 3 methods per group (offset)
    for j, method in enumerate(method_order):
        data = [
            df[(df["group"] == g) & (df["method"] == method)][metric].dropna()
            for g in group_order
        ]

        # positions: centered at group index, offset by method
        positions = [p + (j - 1) * box_width for p in base_positions]

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

    ax.set_title(title, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_xticks(base_positions)
    ax.set_xticklabels(group_order, fontsize=10)
    ax.grid(axis="y", linestyle=":", alpha=0.6)

    # Only KGE panel benefits strongly from 0-line; harmless for others too
    ax.axhline(0, linestyle="--", linewidth=1, color="gray")

# ============================================================
# LEGEND
# ============================================================
handles = [plt.Line2D([0], [0], color=method_colors[m], lw=8) for m in method_order]
labels = [method_labels[m] for m in method_order]

fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, fontsize=11)

plt.tight_layout(rect=[0, 0, 1, 0.94])
plt.savefig(OUT_FIG, dpi=300)
plt.close()

print(f"✔ Figure saved: {OUT_FIG}")
