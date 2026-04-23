import os
import numpy as np
import pandas as pd
from pathlib import Path

# ============================================================
# INPUTS
# ============================================================
BASE_DIR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"

SEASONAL_TABLE = os.path.join(BASE_DIR, "seasonal_groups_by_station.csv")

OBS_TEMPLATE = (BASE_DIR + r"\observed_data\{folder}\{source}\{station_code}_Q.csv")
OR_TEMPLATE = BASE_DIR + r"\Historical_Simulation_or\{comid}_or.csv"
PRE_TEMPLATE = BASE_DIR + r"\Historical_Simulation_bc\{comid}_bc.csv"
MFDC_TEMPLATE = BASE_DIR + r"\Historical_Simulation_MFDC-QM\{station_code}-{comid}_Q.csv"

OUT_METRICS = os.path.join(BASE_DIR, "seasonal_metrics_by_station.csv")

MIN_DAYS_PER_GROUP = 90

# ============================================================
# METRICS
# ============================================================
def kge_components(sim, obs):
    """
    Returns: KGE, r, beta, gamma
    """
    sim = np.asarray(sim)
    obs = np.asarray(obs)

    r = np.corrcoef(sim, obs)[0, 1]
    beta = sim.mean() / obs.mean()
    gamma = (sim.std() / sim.mean()) / (obs.std() / obs.mean())

    kge = 1 - np.sqrt((r - 1) ** 2 + (beta - 1) ** 2 + (gamma - 1) ** 2)
    return kge, r, beta, gamma


# ============================================================
# HELPERS
# ============================================================
def read_series(path):
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    s = df.iloc[:, 1]
    idx = pd.to_datetime(df.iloc[:, 0], errors="coerce")
    s.index = idx
    s = s.dropna()
    s = s[s >= 0]
    return s.sort_index()


# ============================================================
# MAIN
# ============================================================
seasonal = pd.read_csv(SEASONAL_TABLE)

results = []

for _, row in seasonal.iterrows():

    station_code = row["station_code"]
    comid = int(row["comid"])
    region = row["region"]

    dry_months = list(map(int, row["dry_months"].split(",")))
    shoulder_months = list(map(int, row["shoulder_months"].split(",")))
    wet_months = list(map(int, row["wet_months"].split(",")))

    folder = row.get("folder", None)
    source = row.get("source", None)

    print(f"\nStation {station_code} | COMID {comid}")

    obs_path = OBS_TEMPLATE.format(
        folder=row["folder"],
        source=row["source"],
        station_code=station_code
    )

    or_path = OR_TEMPLATE.format(comid=comid)
    pre_path = PRE_TEMPLATE.format(comid=comid)
    mfdc_path = MFDC_TEMPLATE.format(
        station_code=station_code,
        comid=comid
    )

    obs = read_series(obs_path)
    or_sim = read_series(or_path)
    pre_sim = read_series(pre_path)
    mfdc_sim = read_series(mfdc_path)

    # Align all series on common dates
    df = pd.concat(
        {
            "obs": obs,
            "or": or_sim,
            "pre": pre_sim,
            "mfdc": mfdc_sim,
        },
        axis=1,
        join="inner"
    ).dropna()

    df["month"] = df.index.month

    groups = {
        "dry": dry_months,
        "shoulder": shoulder_months,
        "wet": wet_months,
    }

    for group, months in groups.items():
        sub = df[df["month"].isin(months)]

        if len(sub) < MIN_DAYS_PER_GROUP:
            print(f"  {group}: skipped (only {len(sub)} days)")
            continue

        for method in ["or", "pre", "mfdc"]:
            kge, r, beta, gamma = kge_components(
                sub[method].values,
                sub["obs"].values
            )

            results.append({
                "station_code": station_code,
                "comid": comid,
                "region": region,
                "group": group,
                "method": method,
                "n_days": len(sub),
                "kge": kge,
                "r": r,
                "beta": beta,
                "gamma": gamma,
            })

# ============================================================
# OUTPUT
# ============================================================
out_df = pd.DataFrame(results)
out_df.to_csv(OUT_METRICS, index=False)

print(f"\n✔ Seasonal metrics written to:\n{OUT_METRICS}")
print(f"✔ Total records: {len(out_df)}")
