import os
import pandas as pd
import numpy as np
from pathlib import Path

# ============================================================
# INPUTS
# ============================================================
METADATA_XLSX = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\world_stations.xlsx"
SHEET_NAME = "selected_stations"

OBS_TEMPLATE = (
    r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"
    r"\observed_data\{folder}\{source}\{station_code}_Q.csv"
)

OUT_TABLE = (
    r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"
    r"\seasonal_groups_by_station.csv"
)

# Quality criteria
MIN_DAYS_PER_MONTH = 30
REQUIRE_ALL_12_MONTHS = True

# ============================================================
# HELPERS
# ============================================================
def read_observed(path):
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    date_col = df.columns[0]
    val_col = df.columns[1]

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, val_col])
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
    df = df.dropna(subset=[val_col])

    df = df.set_index(date_col).sort_index()
    return df[val_col]


def compute_monthly_climatology(obs_series):
    """
    Returns:
      clim (Series indexed by month 1..12),
      counts (Series indexed by month)
    """
    df = obs_series.to_frame("Q")
    df["month"] = df.index.month

    counts = df.groupby("month")["Q"].count()
    clim = df.groupby("month")["Q"].median()

    # apply minimum data criterion
    clim = clim[counts >= MIN_DAYS_PER_MONTH]
    counts = counts.loc[clim.index]

    return clim, counts


def classify_months(clim):
    """
    clim: Series indexed by month, length must be 12
    """
    sorted_months = clim.sort_values().index.tolist()

    dry = sorted_months[:4]
    wet = sorted_months[-4:]
    shoulder = sorted_months[4:8]

    return dry, shoulder, wet


# ============================================================
# MAIN
# ============================================================
meta = pd.read_excel(METADATA_XLSX, sheet_name=SHEET_NAME)
meta.columns = [c.lower() for c in meta.columns]

results = []

for _, row in meta.iterrows():
    folder = row["folder"]
    source = row["source"]
    station_code = str(row["station_code"])
    comid = row["comid"]
    region = row["region"]

    obs_path = OBS_TEMPLATE.format(
        folder=folder,
        source=source,
        station_code=station_code
    )

    print(f"\nStation {station_code} | COMID {comid}")

    if not os.path.exists(obs_path):
        raise FileNotFoundError(f"Observed file missing: {obs_path}")

    obs = read_observed(obs_path)

    clim, counts = compute_monthly_climatology(obs)

    if REQUIRE_ALL_12_MONTHS and len(clim) < 12:
        print("  ❌ Excluded (insufficient monthly coverage)")
        continue

    dry, shoulder, wet = classify_months(clim)

    results.append({
        "station_code": station_code,
        "comid": comid,
        "region": region,
        "dry_months": ",".join(map(str, dry)),
        "shoulder_months": ",".join(map(str, shoulder)),
        "wet_months": ",".join(map(str, wet)),
        "min_days_per_month": int(counts.min()),
        "total_obs_days": int(len(obs))
    })

# ============================================================
# OUTPUT
# ============================================================
out_df = pd.DataFrame(results)
out_df.to_csv(OUT_TABLE, index=False)

print(f"\n✔ Seasonal classification written to:\n{OUT_TABLE}")
print(f"✔ Stations retained: {len(out_df)}")
