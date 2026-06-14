import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import mannwhitneyu

# -------------------------------
# Load data
# -------------------------------
df = pd.read_csv("stations_pbias_deltakge.csv")
df["deltaKGE"] = df["kge_pre"] - df["kge_or"]
df["PBIAS_abs"] = df["PBIAS_local"].abs()
df = df.dropna(subset=["PBIAS_abs", "deltaKGE"])
print(f"Total stations: {len(df)}")

# -------------------------------
# Thresholds to compare
# -------------------------------
thresholds = [25, 50, 75, 100]

# -------------------------------
# Summary and Mann-Whitney per threshold
# -------------------------------
results = []
for thr in thresholds:
    below = df[df["PBIAS_abs"] <  thr]["deltaKGE"]
    above = df[df["PBIAS_abs"] >= thr]["deltaKGE"]

    stat, pval = mannwhitneyu(above, below, alternative="two-sided")
    sig = "***" if pval < 0.001 else ("**" if pval < 0.01 else ("*" if pval < 0.05 else "ns"))

    results.append({
        "Threshold": f"|PBIAS| = {thr}%",
        "n_below": len(below),
        "median_below": below.median(),
        "frac_improving_below": (below > 0).mean(),
        "n_above": len(above),
        "median_above": above.median(),
        "frac_improving_above": (above > 0).mean(),
        "delta_median": above.median() - below.median(),
        "p_value": pval,
        "significance": sig,
    })

df_results = pd.DataFrame(results)
print("\n--- Threshold Sensitivity Summary ---")
print(df_results.to_string(index=False))
df_results.to_csv("pbias_threshold_sensitivity.csv", index=False)

# -------------------------------
# Figure
# -------------------------------
fig, axes = plt.subplots(len(thresholds), 2,
                          figsize=(12, 4 * len(thresholds)))

colors_below = "#bdd7e7"
colors_above = "#08519c"

for i, thr in enumerate(thresholds):
    below = df[df["PBIAS_abs"] <  thr]["deltaKGE"].values
    above = df[df["PBIAS_abs"] >= thr]["deltaKGE"].values
    row   = df_results[df_results["Threshold"] == f"|PBIAS| = {thr}%"].iloc[0]

    # --- Panel left: Boxplot ---
    ax = axes[i, 0]
    bp = ax.boxplot(
        [below, above],
        patch_artist=True,
        notch=False,
        showfliers=False,
        medianprops=dict(color="black", linewidth=2),
        whiskerprops=dict(linewidth=1.2),
        capprops=dict(linewidth=1.2),
    )
    bp["boxes"][0].set_facecolor(colors_below)
    bp["boxes"][1].set_facecolor(colors_above)
    bp["boxes"][0].set_alpha(0.85)
    bp["boxes"][1].set_alpha(0.85)

    ax.axhline(0, color="black", linewidth=1, linestyle="--")
    ax.set_xticks([1, 2])
    ax.set_xticklabels([
        f"|PBIAS| < {thr}%\nn={len(below)}\nmedian={np.median(below):.2f}\nIQR=({np.percentile(below,25):.2f}, {np.percentile(below,75):.2f})",
        f"|PBIAS| ≥ {thr}%\nn={len(above)}\nmedian={np.median(above):.2f}\nIQR=({np.percentile(above,25):.2f}, {np.percentile(above,75):.2f})",
    ], fontsize=9)
    ax.set_ylabel("ΔKGEpre", fontsize=9)
    ax.set_ylim(-3, 5)
    ax.set_title(
        f"Comparison {i+1}: |PBIAS| = {thr}%  "
        f"(p={row['p_value']:.4f} {row['significance']})",
        loc="left", fontsize=9, fontweight="bold"
    )

    # --- Panel right: Fraction improving ---
    ax2 = axes[i, 1]
    fracs = [row["frac_improving_below"], row["frac_improving_above"]]
    meds  = [row["median_below"], row["median_above"]]
    bars  = ax2.bar([0, 1], fracs, color=[colors_below, colors_above],
                    alpha=0.85, edgecolor="black", linewidth=0.5, width=0.4)
    ax2.axhline(0.5, color="black", linewidth=1, linestyle="--",
                label="50% stations improving")
    ax2.set_xticks([0, 1])
    ax2.set_xticklabels([
        f"|PBIAS| < {thr}%",
        f"|PBIAS| ≥ {thr}%"
    ], fontsize=9)
    ax2.set_ylabel("Fraction ΔKGEpre > 0", fontsize=9)
    ax2.set_ylim(0, 1)
    ax2.legend(fontsize=8)

    # Total fraction improving per bar
    for xi, f in enumerate(fracs):
        ax2.text(xi, f + 0.02, f"{f:.1%}",
                 ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax2.set_title(
        f"Fraction improving: {thr}% threshold",
        loc="left", fontsize=9, fontweight="bold"
    )

plt.tight_layout()
fig.savefig("pbias_threshold_sensitivity.png", dpi=300, bbox_inches="tight")
print("\nSaved: pbias_threshold_sensitivity.png")
plt.close(fig)