import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import mannwhitneyu

# -------------------------------
# Load data
# -------------------------------
df = pd.read_csv("stations_pbias_deltakge.csv")
df["deltaKGE"]   = df["kge_pre"] - df["kge_or"]
df["PBIAS_abs"]  = df["PBIAS_local"].abs()

# -------------------------------
# No filtering - use all stations
# -------------------------------
df_clean = df.copy()
print(f"Total stations: {len(df_clean)}")

# -------------------------------
# Classify |PBIAS| into groups
# -------------------------------
bins   = [0, 25, 50, 75, 100, np.inf]
labels = [
    "|PBIAS| < 25%",
    "25% ≤ |PBIAS| < 50%",
    "50% ≤ |PBIAS| < 75%",
    "75% ≤ |PBIAS| < 100%",
    "|PBIAS| ≥ 100%",
]

df_clean["PBIAS_group"] = pd.cut(
    df_clean["PBIAS_abs"],
    bins=bins,
    labels=labels,
    include_lowest=True
)

# -------------------------------
# Summary statistics per group
# -------------------------------
summary = df_clean.groupby("PBIAS_group", observed=True)["deltaKGE"].agg(
    count="count",
    median="median",
    q25=lambda x: x.quantile(0.25),
    q75=lambda x: x.quantile(0.75),
    frac_improving=lambda x: (x > 0).mean()
).reset_index()

print("\n--- Summary per |PBIAS| group ---")
print(summary.to_string(index=False))
summary.to_csv("pbias_abs_deltakge_summary.csv", index=False)

# -------------------------------
# Mann-Whitney U test vs |PBIAS| < 25%
# -------------------------------
ref = df_clean[df_clean["PBIAS_group"] == "|PBIAS| < 25%"]["deltaKGE"]
print("\n--- Mann-Whitney U test vs |PBIAS| < 25% ---")
for grp in labels[1:]:
    grp_data = df_clean[df_clean["PBIAS_group"] == grp]["deltaKGE"]
    if len(grp_data) < 5:
        continue
    stat, pval = mannwhitneyu(grp_data, ref, alternative="two-sided")
    sig = "***" if pval < 0.001 else ("**" if pval < 0.01 else ("*" if pval < 0.05 else "ns"))
    print(f"  {grp:35s} p={pval:.4f} {sig}")

# -------------------------------
# Colors per group
# -------------------------------
colors = ["#d3d3d3", "#bdd7e7", "#6baed6", "#2171b5", "#08519c"]

# -------------------------------
# Figure
# -------------------------------
fig, axes = plt.subplots(2, 1, figsize=(10, 10))

# --- Panel A: Boxplot ---
ax = axes[0]
group_data = [
    df_clean[df_clean["PBIAS_group"] == lbl]["deltaKGE"].dropna().values
    for lbl in labels
]

bp = ax.boxplot(
    group_data,
    patch_artist=True,
    notch=False,
    showfliers=False,
    medianprops=dict(color="black", linewidth=2),
    whiskerprops=dict(linewidth=1.2),
    capprops=dict(linewidth=1.2),
)

for patch, color in zip(bp["boxes"], colors):
    patch.set_facecolor(color)
    patch.set_alpha(0.85)

ax.axhline(0, color="black", linewidth=1,
           linestyle="--", label="No change (ΔKGEpre = 0)")

# Annotate n and median
for i, lbl in enumerate(labels):
    med = summary.loc[summary["PBIAS_group"] == lbl, "median"].values[0]
    n   = summary.loc[summary["PBIAS_group"] == lbl, "count"].values[0]
    ax.text(i + 1, -4.7, f"n={n}\nm={med:.2f}",
            ha="center", va="bottom", fontsize=8)

ax.set_xticks(range(1, len(labels) + 1))
ax.set_xticklabels(labels, fontsize=9)
ax.set_ylabel("ΔKGEpre (kge_pre − kge_or)", fontsize=10)
ax.set_title(
    "(a) Distribution of ΔKGEpre by |PBIAS| group (all stations, no filtering)",
    loc="left", fontsize=9, fontweight="bold"
)
ax.legend(fontsize=9)
ax.set_ylim(-5, 5)

# --- Panel B: Fraction improving + median ---
ax2 = axes[1]
x = np.arange(len(labels))

bars = ax2.bar(x, summary["frac_improving"], width=0.5,
               color=colors, alpha=0.85,
               edgecolor="black", linewidth=0.5)
ax2.axhline(0.5, color="black", linewidth=1,
            linestyle="--", label="50% of stations improving")

# Overlay median deltaKGE
ax2b = ax2.twinx()
ax2b.plot(x, summary["median"], color="navy",
          marker="o", linewidth=2, markersize=7,
          label="Median ΔKGEpre")
ax2b.axhline(0, color="navy", linewidth=0.8, linestyle=":")
ax2b.set_ylabel("Median ΔKGEpre", fontsize=10, color="navy")
ax2b.tick_params(axis="y", labelcolor="navy")

ax2.set_xticks(x)
ax2.set_xticklabels(labels, fontsize=9)
ax2.set_ylabel("Fraction of stations with ΔKGEpre > 0", fontsize=10)
ax2.set_title(
    "(b) Fraction of improving stations and median ΔKGEpre by |PBIAS| group",
    loc="left", fontsize=9, fontweight="bold"
)
ax2.set_ylim(0, 1)
ax2.legend(loc="upper left", fontsize=9)
ax2b.legend(loc="upper right", fontsize=9)

plt.tight_layout()
fig.savefig("pbias_abs_vs_deltakge_nofilter.png", dpi=300, bbox_inches="tight")
print("\nSaved: pbias_abs_vs_deltakge_nofilter.png")
plt.close(fig)