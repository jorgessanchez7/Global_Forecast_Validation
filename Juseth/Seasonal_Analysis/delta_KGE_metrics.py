import pandas as pd
import os

# ============================================================
# INPUTS
# ============================================================
BASE_DIR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"

GLOBAL_METRICS = os.path.join(BASE_DIR, "global_metrics_by_station.csv")
SEASONAL_METRICS = os.path.join(BASE_DIR, "seasonal_metrics_by_station.csv")

OUT_TABLE = os.path.join(BASE_DIR, "delta_kge_tradeoff_by_group.csv")

# ============================================================
# LOAD DATA
# ============================================================
global_df = pd.read_csv(GLOBAL_METRICS)
seasonal_df = pd.read_csv(SEASONAL_METRICS)

# Standardize
global_df["group"] = "global"

# Keep only what we need
global_df = global_df[
    ["station_code", "comid", "region", "group", "method", "kge"]
]

seasonal_df = seasonal_df[
    ["station_code", "comid", "region", "group", "method", "kge"]
]

# ============================================================
# COMBINE GLOBAL + SEASONAL
# ============================================================
all_metrics = pd.concat([global_df, seasonal_df], ignore_index=True)

# ============================================================
# COMPUTE ΔKGE RELATIVE TO OR
# ============================================================
# Pivot so that OR / PRE / MFDC are columns
pivot = all_metrics.pivot_table(
    index=["station_code", "comid", "region", "group"],
    columns="method",
    values="kge"
).reset_index()

# Remove cases where baseline OR is missing
pivot = pivot.dropna(subset=["or"])

# Compute deltas
pivot["delta_kge_pre"] = pivot["pre"] - pivot["or"]
pivot["delta_kge_mfdc"] = pivot["mfdc"] - pivot["or"]

# ============================================================
# FINAL TIDY TABLE (LONG FORMAT)
# ============================================================
delta_long = pivot.melt(
    id_vars=["station_code", "comid", "region", "group"],
    value_vars=["delta_kge_pre", "delta_kge_mfdc"],
    var_name="method",
    value_name="delta_kge"
)

# Clean method names
delta_long["method"] = delta_long["method"].map({
    "delta_kge_pre": "pre",
    "delta_kge_mfdc": "mfdc"
})

# ============================================================
# OUTPUT
# ============================================================
delta_long.to_csv(OUT_TABLE, index=False)

print("✔ Tradeoff table written to:")
print(OUT_TABLE)
print("✔ Records:", len(delta_long))
