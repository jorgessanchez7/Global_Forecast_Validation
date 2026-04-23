import pandas as pd
import matplotlib.pyplot as plt

# Load data
df = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Reviewing_Papers_and_Proposals\\Paper_06\\Review_3\\Metrics_Summary\\Metrics_Summary_ALL_STATIONS.csv")

# Map calibration period to number of years
period_to_years = {
    "2012": 1,
    "2012-2013": 2,
    "2012-2014": 3,
    "2012-2015": 4,
    "2012-2016": 5,
}
df["n_years"] = df["Observed_Data_Used_For_Bias_Correction"].map(period_to_years)

# Make sure ordering is consistent
df = df.sort_values(["Station", "n_years"])

# Helper: make a clean legend outside the plot
def legend_outside(ax):
    ax.legend(
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        borderaxespad=0.0,
        frameon=True,
        fontsize=9,
        title="Series"
    )

# ---------------------------
# Figure 1: BC Validation KGE12 vs calibration length
# ---------------------------
fig, ax = plt.subplots()

# Plot each station as a line
for station, g in df.groupby("Station", sort=True):
    ax.plot(g["n_years"], g["BC_Val_KGE12"], marker="o", linewidth=1.5, alpha=0.9, label=station)

# Median line across stations
median = df.groupby("n_years")["BC_Val_KGE12"].median().sort_index()
ax.plot(median.index, median.values, linewidth=3, marker="o", label="Median")

ax.set_xlabel("Calibration record length (years)")
ax.set_ylabel("Validation KGE′ (KGE12)")
ax.set_title("Bias-corrected validation performance vs calibration record length")

legend_outside(ax)
fig.tight_layout()
plt.show()


# ---------------------------
# Figure 2: Improvement relative to original model
# ---------------------------
df["Delta_KGE12"] = df["BC_Val_KGE12"] - df["Orig_Val_KGE12"]

fig, ax = plt.subplots()

for station, g in df.groupby("Station", sort=True):
    ax.plot(g["n_years"], g["Delta_KGE12"], marker="o", linewidth=1.5, alpha=0.9, label=station)

median = df.groupby("n_years")["Delta_KGE12"].median().sort_index()
ax.plot(median.index, median.values, linewidth=3, marker="o", label="Median")

ax.set_xlabel("Calibration record length (years)")
ax.set_ylabel("ΔKGE′ (Bias-corrected − Original)")
ax.set_title("Performance gain from bias correction (validation period)")

legend_outside(ax)
fig.tight_layout()
plt.show()


# ---------------------------
# Figure 3: Calibration vs validation KGE12 (points grouped by n_years)
# ---------------------------
# We will plot one scatter "group" per n_years so the legend tells you the record length.
# To avoid manually setting colors, we vary marker style by n_years.
markers = {1: "o", 2: "s", 3: "^", 4: "D", 5: "P"}

fig, ax = plt.subplots()

for ny in sorted(df["n_years"].dropna().unique()):
    g = df[df["n_years"] == ny]
    ax.scatter(
        g["BC_Cal_KGE12"],
        g["BC_Val_KGE12"],
        marker=markers.get(ny, "o"),
        alpha=0.85,
        label=f"{int(ny)} year(s)"
    )

# 1:1 line
xmin = min(df["BC_Cal_KGE12"].min(), df["BC_Val_KGE12"].min())
xmax = max(df["BC_Cal_KGE12"].max(), df["BC_Val_KGE12"].max())
ax.plot([xmin, xmax], [xmin, xmax], linestyle="--", label="1:1 line")

ax.set_xlabel("Calibration KGE′ (KGE12)")
ax.set_ylabel("Validation KGE′ (KGE12)")
ax.set_title("Calibration vs validation performance by calibration record length")

legend_outside(ax)
fig.tight_layout()
plt.show()