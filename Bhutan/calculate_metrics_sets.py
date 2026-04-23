"""
Compute NSE, KGE12 (KGE' 2012), r (Pearson), and PBIAS for each station and each
bias-correction calibration period (2012, 2012-2013, ..., 2012-2016).

For each station and each calibration period:
1) Calibration metrics:
   Observed (subset period) vs Bias-Corrected series (same period), using only common dates.

2) Validation metrics (Bias-Corrected):
   Validation observed (2018-2022) vs Bias-Corrected series (2018-2022), using only common dates.

3) Original model metrics (same for all rows):
   Validation observed (2018-2022) vs Original simulated (2018-2022), using only common dates.

Inputs:
- Observed training (2012–2016):
  OBS_DIR\{Station}.csv with columns: 'datetime', 'discharge(m3/s)'

- Validation observed (2018–2022):
  VAL_DIR\{Station}.csv with columns: 'Datetime', 'Discharge (m3/s)'

- Original simulated historical:
  SIM_DIR\{Station}.csv with columns: 'datetime', 'streamflow_m^3/s'

- Bias-corrected historical (from Part 1):
  BC_DIR\{Station}_{Period}.csv  where Period in:
    2012, 2012-2013, 2012-2014, 2012-2015, 2012-2016

Outputs:
- Per-station summary CSV:
  OUT_DIR\Metrics_Summary_{Station}.csv

- Combined summary CSV:
  OUT_DIR\Metrics_Summary_ALL_STATIONS.csv

Notes:
- All computations use only overlapping timestamps and drop NaNs.
- KGE12 implemented as KGE' (2012): variability term uses CV ratio (gamma).
"""

import os
import math
import traceback
import pandas as pd


# =============================================================================
# USER SETTINGS (edit as needed)
# =============================================================================
OBS_DIR = r"G:\My Drive\Personal_Files\Post_Doc\Reviewing_Papers_and_Proposals\Paper_06\Review_3\Observed"
VAL_DIR = r"G:\My Drive\Personal_Files\Post_Doc\Reviewing_Papers_and_Proposals\Paper_06\Review_3\Validation_Dataset"
SIM_DIR = r"G:\My Drive\Personal_Files\Post_Doc\Reviewing_Papers_and_Proposals\Paper_06\Review_3\Simulated"
BC_DIR = r"G:\My Drive\Personal_Files\Post_Doc\Reviewing_Papers_and_Proposals\Paper_06\Review_3\Bias_Corrected"
OUT_DIR = r"G:\My Drive\Personal_Files\Post_Doc\Reviewing_Papers_and_Proposals\Paper_06\Review_3\Metrics_Summary"
os.makedirs(OUT_DIR, exist_ok=True)

STATIONS = ["Chimakoti", "Haa", "Kurizampa", "Kurjey", "Lungtenphu"]

PERIODS = [
    ("2012", 2012, 2012),
    ("2012-2013", 2012, 2013),
    ("2012-2014", 2012, 2014),
    ("2012-2015", 2012, 2015),
    ("2012-2016", 2012, 2016),
]

VAL_START = "2018-01-01"
VAL_END = "2022-12-31"


# =============================================================================
# READERS
# =============================================================================
def read_observed_training(path: str) -> pd.Series:
    """
    Observed training CSV format:
      columns: 'datetime', 'discharge(m3/s)'
      datetime format: %Y-%m-%d
    Returns: Series indexed by datetime.
    """
    df = pd.read_csv(path)
    required = {"datetime", "discharge(m3/s)"}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing columns in observed training file: {path} | found: {list(df.columns)}")

    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["datetime"]).set_index("datetime").sort_index()

    s = pd.to_numeric(df["discharge(m3/s)"], errors="coerce")
    s = s.dropna()
    return s


def read_observed_validation(path: str) -> pd.Series:
    """
    Validation observed CSV format:
      columns: 'Datetime', 'Discharge (m3/s)'
      datetime format: %Y-%m-%d
    Returns: Series indexed by datetime.
    """
    df = pd.read_csv(path)
    required = {"Datetime", "Discharge (m3/s)"}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing columns in validation file: {path} | found: {list(df.columns)}")

    df["Datetime"] = pd.to_datetime(df["Datetime"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["Datetime"]).set_index("Datetime").sort_index()

    s = pd.to_numeric(df["Discharge (m3/s)"], errors="coerce")
    s = s.dropna()
    return s


def read_simulated_original(path: str) -> pd.Series:
    """
    Original simulated CSV format:
      columns: 'datetime', 'streamflow_m^3/s'
      datetime format: %Y-%m-%d
    Returns: Series indexed by datetime.
    """
    df = pd.read_csv(path)
    required = {"datetime", "streamflow_m^3/s"}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing columns in simulated file: {path} | found: {list(df.columns)}")

    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["datetime"]).set_index("datetime").sort_index()

    s = pd.to_numeric(df["streamflow_m^3/s"], errors="coerce")
    s = s.dropna()
    return s


def read_bias_corrected(path: str) -> pd.Series:
    """
    Bias-corrected CSV from Part 1:
      - Two columns
      - First column is datetime but has NO header (usually becomes 'Unnamed: 0' in pandas)
      - Second column is: 'Corrected Simulated Streamflow'

    Returns: Series indexed by datetime with corrected streamflow values.
    """
    df = pd.read_csv(path)

    # --- Identify datetime column ---
    # If the date column has no name, pandas usually calls it 'Unnamed: 0'.
    # Otherwise, fall back to common datetime header names, and finally to first column.
    if "Unnamed: 0" in df.columns:
        dt_col = "Unnamed: 0"
    else:
        dt_col = None
        for c in ["datetime", "Datetime", "date", "Date"]:
            if c in df.columns:
                dt_col = c
                break
        if dt_col is None:
            dt_col = df.columns[0]  # fallback: first column

    # Parse datetime
    df[dt_col] = pd.to_datetime(df[dt_col], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=[dt_col]).set_index(dt_col).sort_index()

    # --- Identify value column ---
    val_col = None
    if "Corrected Simulated Streamflow" in df.columns:
        val_col = "Corrected Simulated Streamflow"
    else:
        # fallback: choose first numeric-like column
        for c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if not numeric_cols:
            raise ValueError(
                f"No numeric column found in bias-corrected file: {path} | columns: {list(df.columns)}"
            )
        val_col = numeric_cols[0]

    s = pd.to_numeric(df[val_col], errors="coerce").dropna()
    return s


# =============================================================================
# TIME WINDOW HELPERS
# =============================================================================
def subset_years(s: pd.Series, y0: int, y1: int) -> pd.Series:
    """Subset a datetime-indexed Series to [y0-01-01, y1-12-31]."""
    start = pd.Timestamp(y0, 1, 1)
    end = pd.Timestamp(y1, 12, 31)
    return s.loc[(s.index >= start) & (s.index <= end)].copy()


def subset_dates(s: pd.Series, start: str, end: str) -> pd.Series:
    """Subset a datetime-indexed Series to [start, end] (inclusive)."""
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    return s.loc[(s.index >= start_ts) & (s.index <= end_ts)].copy()


def align_series(obs: pd.Series, sim: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Inner-join by datetime and drop NaNs."""
    df = pd.concat([obs.rename("obs"), sim.rename("sim")], axis=1, join="inner").dropna()
    return df["obs"], df["sim"]


# =============================================================================
# METRICS
# =============================================================================
def nse(obs: pd.Series, sim: pd.Series) -> float:
    """Nash-Sutcliffe Efficiency."""
    obs, sim = align_series(obs, sim)
    if len(obs) < 2:
        return float("nan")
    denom = ((obs - obs.mean()) ** 2).sum()
    if denom == 0:
        return float("nan")
    return 1.0 - (((sim - obs) ** 2).sum() / denom)


def pearson_r(obs: pd.Series, sim: pd.Series) -> float:
    """Pearson correlation coefficient."""
    obs, sim = align_series(obs, sim)
    if len(obs) < 2:
        return float("nan")
    return obs.corr(sim)


def pbias(obs: pd.Series, sim: pd.Series) -> float:
    """Percent bias: 100 * sum(sim - obs) / sum(obs)."""
    obs, sim = align_series(obs, sim)
    if len(obs) < 1:
        return float("nan")
    denom = obs.sum()
    if denom == 0:
        return float("nan")
    return 100.0 * (sim.sum() - obs.sum()) / denom


def kge12(obs: pd.Series, sim: pd.Series) -> float:
    """
    Kling-Gupta Efficiency (2012) a.k.a. KGE' (KGE prime):
      KGE' = 1 - sqrt( (r-1)^2 + (beta-1)^2 + (gamma-1)^2 )

    where:
      r     = Pearson correlation
      beta  = mean(sim) / mean(obs)
      gamma = CV(sim) / CV(obs)
      CV(x) = std(x) / mean(x)
    """
    obs, sim = align_series(obs, sim)
    if len(obs) < 2:
        return float("nan")

    r = obs.corr(sim)

    mu_o = obs.mean()
    mu_s = sim.mean()
    if mu_o == 0 or mu_s == 0:
        return float("nan")

    beta = mu_s / mu_o

    # Sample std (ddof=1)
    sig_o = obs.std(ddof=1)
    sig_s = sim.std(ddof=1)

    cv_o = sig_o / mu_o
    cv_s = sig_s / mu_s

    if cv_o == 0:
        return float("nan")

    gamma = cv_s / cv_o

    for x in (r, beta, gamma):
        if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
            return float("nan")

    return 1.0 - math.sqrt((r - 1.0) ** 2 + (beta - 1.0) ** 2 + (gamma - 1.0) ** 2)


def compute_all_metrics(obs: pd.Series, sim: pd.Series) -> dict:
    """Compute all metrics using only common dates."""
    obs_a, sim_a = align_series(obs, sim)
    return {
        "NSE": nse(obs_a, sim_a),
        "KGE12": kge12(obs_a, sim_a),
        "r": pearson_r(obs_a, sim_a),
        "PBIAS": pbias(obs_a, sim_a),
        "n_common": int(len(obs_a)),
    }


# =============================================================================
# MAIN
# =============================================================================
def main():
    print("Starting metrics computation...")
    print(f"Observed training folder:  {OBS_DIR}")
    print(f"Validation folder:         {VAL_DIR}")
    print(f"Original simulated folder: {SIM_DIR}")
    print(f"Bias-corrected folder:     {BC_DIR}")
    print(f"Output folder:             {OUT_DIR}")
    print("")

    # Validate folders
    for p in [OBS_DIR, VAL_DIR, SIM_DIR, BC_DIR]:
        if not os.path.isdir(p):
            raise FileNotFoundError(f"Folder not found: {p}")

    combined_rows = []

    total_jobs = len(STATIONS) * len(PERIODS)
    job_counter = 0

    for station in STATIONS:
        print("=" * 90)
        print(f"Station: {station}")

        obs_path = os.path.join(OBS_DIR, f"{station}.csv")
        val_path = os.path.join(VAL_DIR, f"{station}.csv")
        sim_path = os.path.join(SIM_DIR, f"{station}.csv")

        if not os.path.isfile(obs_path):
            print(f"[WARNING] Observed training file not found: {obs_path}. Skipping station.")
            continue
        if not os.path.isfile(val_path):
            print(f"[WARNING] Validation file not found: {val_path}. Skipping station.")
            continue
        if not os.path.isfile(sim_path):
            print(f"[WARNING] Original simulated file not found: {sim_path}. Skipping station.")
            continue

        try:
            obs_full = read_observed_training(obs_path)
            val_full = read_observed_validation(val_path)
            sim_orig_full = read_simulated_original(sim_path)

            # Restrict validation to 2018–2022
            val = subset_dates(val_full, VAL_START, VAL_END)
            sim_orig_val = subset_dates(sim_orig_full, VAL_START, VAL_END)

            print(f"Loaded observed training rows: {len(obs_full):,}")
            print(f"Loaded validation rows (2018–2022 used): {len(val):,}")
            print(f"Loaded original simulated rows: {len(sim_orig_full):,}")

        except Exception as e:
            print(f"[ERROR] Failed reading inputs for {station}: {e}")
            print(traceback.format_exc())
            continue

        # Original model metrics vs validation (same for all calibration periods)
        orig_val_metrics = compute_all_metrics(val, sim_orig_val)
        print(
            "Original model metrics (validation 2018–2022): "
            f"NSE={orig_val_metrics['NSE']:.3f}, "
            f"KGE12={orig_val_metrics['KGE12']:.3f}, "
            f"r={orig_val_metrics['r']:.3f}, "
            f"PBIAS={orig_val_metrics['PBIAS']:.2f}, "
            f"n={orig_val_metrics['n_common']}"
        )

        station_rows = []

        for period_label, y0, y1 in PERIODS:
            job_counter += 1
            print("-" * 90)
            print(f"[{job_counter}/{total_jobs}] Period: {period_label}")

            bc_path = os.path.join(BC_DIR, f"{station}_{period_label}.csv")
            if not os.path.isfile(bc_path):
                print(f"[WARNING] Bias-corrected file not found: {bc_path}. Skipping this period.")
                continue

            try:
                bc_full = read_bias_corrected(bc_path)

                # Calibration window (same as observed subset used for bias correction)
                obs_cal = subset_years(obs_full, y0, y1)
                bc_cal = subset_years(bc_full, y0, y1)

                # Validation window (2018–2022)
                bc_val = subset_dates(bc_full, VAL_START, VAL_END)

                cal_metrics = compute_all_metrics(obs_cal, bc_cal)
                bc_val_metrics = compute_all_metrics(val, bc_val)

                row = {
                    "Station": station,
                    "Observed_Data_Used_For_Bias_Correction": period_label,

                    # Bias-corrected metrics on calibration period
                    "BC_Cal_NSE": cal_metrics["NSE"],
                    "BC_Cal_KGE12": cal_metrics["KGE12"],
                    "BC_Cal_r": cal_metrics["r"],
                    "BC_Cal_PBIAS": cal_metrics["PBIAS"],
                    "BC_Cal_n_common": cal_metrics["n_common"],

                    # Bias-corrected metrics on validation period
                    "BC_Val_NSE": bc_val_metrics["NSE"],
                    "BC_Val_KGE12": bc_val_metrics["KGE12"],
                    "BC_Val_r": bc_val_metrics["r"],
                    "BC_Val_PBIAS": bc_val_metrics["PBIAS"],
                    "BC_Val_n_common": bc_val_metrics["n_common"],

                    # Original metrics on validation period (repeat each row)
                    "Orig_Val_NSE": orig_val_metrics["NSE"],
                    "Orig_Val_KGE12": orig_val_metrics["KGE12"],
                    "Orig_Val_r": orig_val_metrics["r"],
                    "Orig_Val_PBIAS": orig_val_metrics["PBIAS"],
                    "Orig_Val_n_common": orig_val_metrics["n_common"],
                }

                station_rows.append(row)
                combined_rows.append(row)

                print(
                    f"[OK] {period_label} | "
                    f"BC-Cal: NSE={row['BC_Cal_NSE']:.3f}, KGE12={row['BC_Cal_KGE12']:.3f}, "
                    f"r={row['BC_Cal_r']:.3f}, PBIAS={row['BC_Cal_PBIAS']:.2f}, n={row['BC_Cal_n_common']} | "
                    f"BC-Val: NSE={row['BC_Val_NSE']:.3f}, KGE12={row['BC_Val_KGE12']:.3f}, "
                    f"r={row['BC_Val_r']:.3f}, PBIAS={row['BC_Val_PBIAS']:.2f}, n={row['BC_Val_n_common']}"
                )

            except Exception as e:
                print(f"[ERROR] Failed metrics for station={station}, period={period_label}: {e}")
                print(traceback.format_exc())
                continue

        # Save per-station summary
        if station_rows:
            station_df = pd.DataFrame(station_rows)
            out_station = os.path.join(OUT_DIR, f"Metrics_Summary_{station}.csv")
            station_df.to_csv(out_station, index=False)
            print(f"Saved station summary CSV: {out_station}")
        else:
            print("[WARNING] No rows produced for this station (check bias-corrected files).")

    # Save combined summary
    if combined_rows:
        combined_df = pd.DataFrame(combined_rows)
        out_all = os.path.join(OUT_DIR, "Metrics_Summary_ALL_STATIONS.csv")
        combined_df.to_csv(out_all, index=False)
        print(f"\nSaved combined summary CSV: {out_all}")

    print("\nAll done.")


if __name__ == "__main__":
    main()
