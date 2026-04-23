import os
import pandas as pd
import numpy as np

# ============================================================
# INPUTS
# ============================================================
BASE_DIR = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"

STATIONS_XLSX = os.path.join(BASE_DIR, "world_stations.xlsx")
SHEET_NAME = "selected_stations"

OBS_TEMPLATE = (BASE_DIR + r"\observed_data\{folder}\{source}\{station_code}_Q.csv")
OR_TEMPLATE = BASE_DIR + r"\Historical_Simulation_or\{comid}_or.csv"
PRE_TEMPLATE = BASE_DIR + r"\Historical_Simulation_bc\{comid}_bc.csv"
MFDC_TEMPLATE = BASE_DIR + r"\Historical_Simulation_MFDC-QM\{station_code}-{comid}_Q.csv"

OUT_METRICS = os.path.join(BASE_DIR, "global_metrics_by_station.csv")

MIN_TOTAL_DAYS = 360  # mínimo 1 año de solape

# ============================================================
# METRICS
# ============================================================
def kge_components(sim, obs):
    sim = np.asarray(sim)
    obs = np.asarray(obs)

    r = np.corrcoef(sim, obs)[0, 1]
    beta = sim.mean() / obs.mean()
    gamma = (sim.std() / sim.mean()) / (obs.std() / obs.mean())

    kge = 1 - np.sqrt(
        (r - 1) ** 2 +
        (beta - 1) ** 2 +
        (gamma - 1) ** 2
    )
    return kge, r, beta, gamma


# ============================================================
# HELPERS
# ============================================================
def read_series(path):
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    dates = pd.to_datetime(df.iloc[:, 0], errors="coerce")
    values = pd.to_numeric(df.iloc[:, 1], errors="coerce")

    s = pd.Series(values.values, index=dates)
    s = s.dropna()
    s = s[s >= 0]
    return s.sort_index()


# ============================================================
# MAIN
# ============================================================
meta = pd.read_excel(STATIONS_XLSX, sheet_name=SHEET_NAME)
meta.columns = [c.lower() for c in meta.columns]

results = []

for _, row in meta.iterrows():

    station_code = str(row["station_code"])
    comid = int(row["comid"])
    region = row["region"]

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

    print(f"\nStation {station_code} | COMID {comid}")

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

    if len(df) < MIN_TOTAL_DAYS:
        print(f"  ❌ Skipped (only {len(df)} overlapping days)")
        continue

    for method in ["or", "pre", "mfdc"]:
        kge, r, beta, gamma = kge_components(
            df[method].values,
            df["obs"].values
        )

        results.append({
            "station_code": station_code,
            "comid": comid,
            "region": region,
            "method": method,
            "n_days": len(df),
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

print(f"\n✔ Global metrics written to:\n{OUT_METRICS}")
print(f"✔ Stations evaluated: {out_df['station_code'].nunique()}")
