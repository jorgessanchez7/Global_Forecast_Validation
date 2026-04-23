import pandas as pd
import matplotlib.pyplot as plt
import os

# ============================================================
# INPUT
# ============================================================
BASE_DIR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"
DATA = os.path.join(BASE_DIR, "delta_components_tradeoff_by_group.csv")

OUT_DIR = os.path.join(BASE_DIR, "fig_tradeoff_components")
os.makedirs(OUT_DIR, exist_ok=True)

group_order = ["global", "dry", "shoulder", "wet"]
method_order = ["pre", "mfdc"]

# ============================================================
# LOAD
# ============================================================
df = pd.read_csv(DATA)

df["group"] = pd.Categorical(df["group"], categories=group_order, ordered=True)
df["method"] = pd.Categorical(df["method"], categories=method_order, ordered=True)

# ============================================================
# PLOT LOOP
# ============================================================
for var, ylab in zip(
    ["beta", "gamma", "r"],
    ["Δβ (bias ratio)", "Δγ (variability ratio)", "Δr (correlation)"]
):

    fig, ax = plt.subplots(figsize=(10, 5))

    data_pre = [
        df[(df["group"] == g) & (df["method"] == "pre") & (df["variable"] == var)]["delta"]
        for g in group_order
    ]

    data_mfdc = [
        df[(df["group"] == g) & (df["method"] == "mfdc") & (df["variable"] == var)]["delta"]
        for g in group_order
    ]

    positions_pre = [i - 0.2 for i in range(len(group_order))]
    positions_mfdc = [i + 0.2 for i in range(len(group_order))]

    bp_pre = ax.boxplot(
        data_pre,
        positions=positions_pre,
        widths=0.35,
        patch_artist=True,
        showfliers=False
    )

    bp_mfdc = ax.boxplot(
        data_mfdc,
        positions=positions_mfdc,
        widths=0.35,
        patch_artist=True,
        showfliers=False
    )

    for patch in bp_pre["boxes"]:
        patch.set_facecolor("lightgray")

    for patch in bp_mfdc["boxes"]:
        patch.set_facecolor("cornflowerblue")

    ax.axhline(0, linestyle="--", linewidth=1)
    ax.set_xticks(range(len(group_order)))
    ax.set_xticklabels(group_order, fontsize=11)
    ax.set_ylabel(ylab, fontsize=12)
    ax.set_title(f"Seasonal tradeoff in {var}", fontsize=13)

    ax.legend(
        [bp_pre["boxes"][0], bp_mfdc["boxes"][0]],
        ["Pre-routing BC", "MFDC-QM"],
        frameon=False
    )

    ax.grid(axis="y", linestyle=":", alpha=0.6)

    plt.tight_layout()
    out_fig = os.path.join(OUT_DIR, f"Fig_tradeoff_{var}.png")
    plt.savefig(out_fig, dpi=300)
    plt.close()

    print(f"✔ Saved {out_fig}")
