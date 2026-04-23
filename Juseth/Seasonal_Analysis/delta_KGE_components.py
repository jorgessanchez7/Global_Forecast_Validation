import pandas as pd
import os

# ============================================================
# INPUTS
# ============================================================
BASE_DIR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"

GLOBAL_METRICS = os.path.join(BASE_DIR, "global_metrics_by_station.csv")
SEASONAL_METRICS = os.path.join(BASE_DIR, "seasonal_metrics_by_station.csv")

OUT_TABLE = os.path.join(BASE_DIR, "delta_components_tradeoff_by_group.csv")

# ============================================================
# LOAD
# ============================================================
global_df = pd.read_csv(GLOBAL_METRICS)
seasonal_df = pd.read_csv(SEASONAL_METRICS)

global_df["group"] = "global"

cols = ["station_code", "comid", "region", "group", "method", "r", "beta", "gamma"]
global_df = global_df[cols]
seasonal_df = seasonal_df[cols]

all_df = pd.concat([global_df, seasonal_df], ignore_index=True)

# ============================================================
# PIVOT
# ============================================================
pivot = all_df.pivot_table(
    index=["station_code", "comid", "region", "group"],
    columns="method",
    values=["r", "beta", "gamma"]
).reset_index()

# Remove rows without OR
pivot = pivot.dropna(subset=[("r", "or")])

# ============================================================
# BUILD TIDY DELTA TABLE
# ============================================================
records = []

for var in ["r", "beta", "gamma"]:
    for method in ["pre", "mfdc"]:
        delta = pivot[(var, method)] - pivot[(var, "or")]

        tmp = pivot[["station_code", "comid", "region", "group"]].copy()
        tmp["variable"] = var
        tmp["method"] = method
        tmp["delta"] = delta.values

        records.append(tmp)

delta_df = pd.concat(records, ignore_index=True)

delta_df.to_csv(OUT_TABLE, index=False)

print("✔ Fixed delta components table written to:")
print(OUT_TABLE)
