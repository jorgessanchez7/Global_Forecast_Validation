import rioxarray
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from scipy.stats import binned_statistic
from scipy.ndimage import uniform_filter1d

# -------------------------------
# Paths
# -------------------------------
FDC_SIM_ZARR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\gridded_data\fdc\fdc_era5.zarr"
FDC_OBS_ZARR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\gridded_data\fdc\fdc_gscd.zarr"
CSV_METRICS   = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Error_Metrics\Metrics_GEOGloWS_v1_Q.csv"
GEOTIFF_OUT   = r"pbias_era5_vs_gscd.tif"

# -------------------------------
# Step 1: Compute PBIAS raster
# -------------------------------
print("Computing PBIAS raster...")
fdc_sim = xr.open_zarr(FDC_SIM_ZARR, consolidated=False)
fdc_obs = xr.open_zarr(FDC_OBS_ZARR, consolidated=False)

p = np.linspace(0.01, 0.99, 99)
fdc_obs = fdc_obs.rename({"flow_distribution": "flow_duration"})
fdc_obs = fdc_obs.assign_coords(percentile_new=("percentile_new", p))
fdc_obs = fdc_obs.rename({"percentile_new": "percentile"})

sim = fdc_sim["flow_duration"].transpose("latitude", "longitude", "percentile")
obs = fdc_obs["flow_duration"]
sim, obs = xr.align(sim, obs, join="inner")
sim = sim.where(obs.notnull())

sim_sum   = sim.sum(dim="percentile")
obs_sum   = obs.sum(dim="percentile")
pbias_da  = 100.0 * (sim_sum - obs_sum) / obs_sum  # (latitude, longitude)

# -------------------------------
# Step 2: Export PBIAS as GeoTIFF
# -------------------------------
print("Exporting PBIAS GeoTIFF...")
pbias_da.name = "PBIAS"
pbias_da = pbias_da.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")
pbias_da = pbias_da.rio.write_crs("EPSG:4326")
pbias_da.rio.to_raster(GEOTIFF_OUT)
print(f"  Saved: {GEOTIFF_OUT}")

# -------------------------------
# Step 3: Load station metrics and compute deltaKGE
# -------------------------------
print("Loading station metrics...")
df = pd.read_csv(CSV_METRICS)

# deltaKGE: improvement from pre-routing correction vs baseline
df["deltaKGE"] = df["kge_pre"] - df["kge_or"]

# Keep only valid rows
df = df.dropna(subset=["Latitude", "Longitude", "deltaKGE"])
print(f"  Stations loaded: {len(df)}")

# -------------------------------
# Step 4: Extract PBIAS at each station location
# -------------------------------
print("Extracting PBIAS at station locations...")

# Convert pbias to numpy for fast lookup
pbias_vals  = pbias_da.values          # (lat, lon)
lat_grid    = pbias_da["latitude"].values
lon_grid    = pbias_da["longitude"].values

def extract_nearest(pbias_arr, lats, lons, lat_grid, lon_grid):
    """Extract nearest grid value for each station."""
    results = []
    for slat, slon in zip(lats, lons):
        i = np.argmin(np.abs(lat_grid - slat))
        j = np.argmin(np.abs(lon_grid - slon))
        results.append(float(pbias_arr[i, j]))
    return results

df["PBIAS_local"] = extract_nearest(
    pbias_vals,
    df["Latitude"].values,
    df["Longitude"].values,
    lat_grid, lon_grid
)

# Drop stations where PBIAS extraction failed (NaN grid cells)
df = df.dropna(subset=["PBIAS_local"])
print(f"  Stations with valid PBIAS: {len(df)}")

# Save merged dataset
df.to_csv("stations_pbias_deltakge.csv", index=False)

# -------------------------------
# Step 5: PBIAS vs deltaKGE analysis
# -------------------------------
print("Analyzing PBIAS vs deltaKGE...")

# Clip PBIAS to [-200, 200] to remove extreme outliers
df_plot = df[(df["PBIAS_local"] >= -200) & (df["PBIAS_local"] <= 200)].copy()

# Bin statistics: median deltaKGE per PBIAS bin
bins     = np.arange(-200, 201, 10)   # 10% bins
bin_mid  = (bins[:-1] + bins[1:]) / 2

med_dkge, _, _ = binned_statistic(df_plot["PBIAS_local"], df_plot["deltaKGE"],
                                   statistic="median", bins=bins)
q25_dkge, _, _ = binned_statistic(df_plot["PBIAS_local"], df_plot["deltaKGE"],
                                   statistic=lambda x: np.percentile(x, 25), bins=bins)
q75_dkge, _, _ = binned_statistic(df_plot["PBIAS_local"], df_plot["deltaKGE"],
                                   statistic=lambda x: np.percentile(x, 75), bins=bins)
count,    _, _ = binned_statistic(df_plot["PBIAS_local"], df_plot["deltaKGE"],
                                   statistic="count", bins=bins)

# Smooth median for visual clarity
med_smooth = uniform_filter1d(np.nan_to_num(med_dkge), size=3)

# -------------------------------
# Step 6: Figures
# -------------------------------

# --- Figure 1: Scatter + binned median ---
fig, axes = plt.subplots(2, 1, figsize=(12, 10))

# Panel A: scatter
ax = axes[0]
sc = ax.scatter(
    df_plot["PBIAS_local"], df_plot["deltaKGE"],
    c=df_plot["kge_or"], cmap="RdYlGn",
    vmin=-0.5, vmax=0.8, alpha=0.3, s=8, rasterized=True
)
cb = plt.colorbar(sc, ax=ax)
cb.set_label("Baseline KGE")
ax.axhline(0, color="black", linewidth=1, linestyle="--", label="No change")
ax.axvline(0, color="grey",  linewidth=0.8, linestyle=":")

# Threshold lines
for thr, col, ls in zip([25, 50, 75],
                         ["#1a9641", "#fdae61", "#d7191c"],
                         ["--", "-", ":"]):
    ax.axvline( thr, color=col, linewidth=1.5, linestyle=ls, label=f"|PBIAS|={thr}%")
    ax.axvline(-thr, color=col, linewidth=1.5, linestyle=ls)

ax.set_xlabel("PBIAS local (%)")
ax.set_ylabel("ΔKGEpre (kge_pre − kge_or)")
ax.set_title("(a) Station-wise PBIAS vs ΔKGEpre", loc="left", fontweight="bold")
ax.legend(fontsize=8, loc="upper left")
ax.set_xlim(-200, 200)
ax.set_ylim(-1.5, 1.5)

# Panel B: binned median + IQR
ax2 = axes[1]
valid = ~np.isnan(med_dkge) & (count >= 5)
ax2.fill_between(bin_mid[valid], q25_dkge[valid], q75_dkge[valid],
                  alpha=0.3, color="steelblue", label="IQR (25–75%)")
ax2.plot(bin_mid[valid], med_dkge[valid],
          color="steelblue", linewidth=1.5, alpha=0.5, label="Median (raw)")
ax2.plot(bin_mid[valid], med_smooth[valid],
          color="navy", linewidth=2, label="Median (smoothed)")
ax2.axhline(0, color="black", linewidth=1, linestyle="--")
ax2.axvline(0, color="grey",  linewidth=0.8, linestyle=":")

for thr, col, ls, lab in zip([25, 50, 75],
                               ["#1a9641", "#fdae61", "#d7191c"],
                               ["--", "-", ":"],
                               ["25%", "50%", "75%"]):
    ax2.axvline( thr, color=col, linewidth=1.5, linestyle=ls, label=f"|PBIAS|={lab}")
    ax2.axvline(-thr, color=col, linewidth=1.5, linestyle=ls)

ax2.set_xlabel("PBIAS local (%)")
ax2.set_ylabel("Median ΔKGEpre")
ax2.set_title("(b) Binned median ΔKGEpre by PBIAS (bin width = 10%)", loc="left", fontweight="bold")
ax2.legend(fontsize=8, loc="upper left")
ax2.set_xlim(-200, 200)

plt.tight_layout()
fig.savefig("pbias_vs_deltakge.png", dpi=300, bbox_inches="tight")
print("  Saved: pbias_vs_deltakge.png")
plt.close(fig)

# --- Figure 2: Fraction of stations with deltaKGE > 0 per PBIAS bin ---
frac_pos, _, _ = binned_statistic(df_plot["PBIAS_local"], df_plot["deltaKGE"],
                                   statistic=lambda x: np.mean(x > 0), bins=bins)

fig3, ax3 = plt.subplots(figsize=(12, 5))
ax3.bar(bin_mid, frac_pos, width=8, color="steelblue", alpha=0.7, label="Fraction ΔKGEpre > 0")
ax3.axhline(0.5, color="black", linewidth=1, linestyle="--", label="50% stations improving")

for thr, col, ls, lab in zip([25, 50, 75],
                               ["#1a9641", "#fdae61", "#d7191c"],
                               ["--", "-", ":"],
                               ["25%", "50%", "75%"]):
    ax3.axvline( thr, color=col, linewidth=1.5, linestyle=ls, label=f"|PBIAS|={lab}")
    ax3.axvline(-thr, color=col, linewidth=1.5, linestyle=ls)

ax3.set_xlabel("PBIAS local (%)")
ax3.set_ylabel("Fraction of stations with ΔKGEpre > 0")
ax3.set_title("(c) Fraction of improving stations by PBIAS bin", loc="left", fontweight="bold")
ax3.legend(fontsize=8)
ax3.set_xlim(-200, 200)
ax3.set_ylim(0, 1)

plt.tight_layout()
fig3.savefig("pbias_fraction_improving.png", dpi=300, bbox_inches="tight")
print("  Saved: pbias_fraction_improving.png")
plt.close(fig3)

print("\nDone. Summary statistics:")
print(df_plot.groupby(
    pd.cut(df_plot["PBIAS_local"], bins=[-200, -75, -50, -25, 0, 25, 50, 75, 200])
)["deltaKGE"].agg(["count", "median", lambda x: np.mean(x > 0)]).rename(
    columns={"<lambda_0>": "frac_improving"}
))