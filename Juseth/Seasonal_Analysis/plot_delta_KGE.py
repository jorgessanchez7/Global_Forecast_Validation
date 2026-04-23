import os
import pandas as pd
import matplotlib.pyplot as plt

# ============================================================
# INPUT
# ============================================================
BASE_DIR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"
DATA = os.path.join(BASE_DIR, "delta_kge_tradeoff_by_group.csv")

OUT_FIG = os.path.join(BASE_DIR, "Fig_tradeoff_deltaKGE_boxplot.png")

# ============================================================
# LOAD DATA
# ============================================================
df = pd.read_csv(DATA)

# Order groups explicitly
group_order = ["global", "dry", "shoulder", "wet"]
method_order = ["pre", "mfdc"]

df["group"] = pd.Categorical(df["group"], categories=group_order, ordered=True)
df["method"] = pd.Categorical(df["method"], categories=method_order, ordered=True)

# ============================================================
# PREPARE DATA FOR BOXPLOTS
# ============================================================
data_pre = [
    df[(df["group"] == g) & (df["method"] == "pre")]["delta_kge"].dropna()
    for g in group_order
]

data_mfdc = [df[(df["group"] == g) & (df["method"] == "mfdc")]["delta_kge"].dropna() for g in group_order]

# ============================================================
# PLOT
# ============================================================
fig, ax = plt.subplots(figsize=(10, 5))

positions_pre = [i - 0.2 for i in range(len(group_order))]
positions_mfdc = [i + 0.2 for i in range(len(group_order))]

bp_pre = ax.boxplot(
    data_pre,
    positions=positions_pre,
    widths=0.35,
    patch_artist=True,
    showfliers=False,
    medianprops=dict(linewidth=1.5),
)

bp_mfdc = ax.boxplot(
    data_mfdc,
    positions=positions_mfdc,
    widths=0.35,
    patch_artist=True,
    showfliers=False,
    medianprops=dict(linewidth=1.5),
)

# Colors
for patch in bp_pre["boxes"]:
    patch.set_facecolor("lightgray")

for patch in bp_mfdc["boxes"]:
    patch.set_facecolor("cornflowerblue")

# ============================================================
# AXES & LABELS
# ============================================================
ax.axhline(0, linestyle="--", linewidth=1)

ax.set_xticks(range(len(group_order)))
ax.set_xticklabels(group_order, fontsize=11)

ax.set_ylabel("ΔKGE (relative to OR)", fontsize=12)
ax.set_title("Seasonal tradeoff in skill improvement", fontsize=13)

# Legend
ax.legend(
    [bp_pre["boxes"][0], bp_mfdc["boxes"][0]],
    ["Pre-routing BC", "MFDC-QM"],
    loc="upper right",
    frameon=False,
)

ax.grid(axis="y", linestyle=":", alpha=0.6)

plt.tight_layout()
plt.savefig(OUT_FIG, dpi=300)
plt.close()

print("✔ Figure saved to:")
print(OUT_FIG)
