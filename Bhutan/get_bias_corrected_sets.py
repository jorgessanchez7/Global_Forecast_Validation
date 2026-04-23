"""
Bias-correct GEOGloWS historical simulated daily streamflow using observed daily discharge
for different observed record lengths (2012, 2012-2013, ..., 2012-2016).

Outputs:
    G:\My Drive\Personal_Files\Post_Doc\Reviewing_Papers_and_Proposals\Paper_06\Review_3\Bias_Corrected
    Files saved as: {Station}_{Period}.csv

Requirements:
    pip install geoglows pandas
"""

import os
import sys
import geoglows
import traceback
import pandas as pd


# -------------------------
# User inputs / paths
# -------------------------
OBS_DIR = r"G:\My Drive\Personal_Files\Post_Doc\Reviewing_Papers_and_Proposals\Paper_06\Review_3\Observed"
SIM_DIR = r"G:\My Drive\Personal_Files\Post_Doc\Reviewing_Papers_and_Proposals\Paper_06\Review_3\Simulated"
OUT_DIR = r"G:\My Drive\Personal_Files\Post_Doc\Reviewing_Papers_and_Proposals\Paper_06\Review_3\Bias_Corrected"
STATIONS = ["Chimakoti", "Haa", "Kurizampa", "Kurjey", "Lungtenphu"]

# Observed periods to test (start year fixed at 2012)
PERIODS = [
    ("2012", 2012, 2012),
    ("2012-2013", 2012, 2013),
    ("2012-2014", 2012, 2014),
    ("2012-2015", 2012, 2015),
    ("2012-2016", 2012, 2016),
]


# -------------------------
# Helpers
# -------------------------
def read_observed_csv(path: str) -> pd.DataFrame:
    """
    Read observed discharge CSV and return a DataFrame indexed by datetime
    with a single column representing discharge.
    Expected columns: 'datetime', 'discharge(m3/s)'
    """
    df = pd.read_csv(path)

    if "datetime" not in df.columns or "discharge(m3/s)" not in df.columns:
        raise ValueError(
            f"Observed file does not contain required columns: {path}\n"
            f"Found columns: {list(df.columns)}"
        )

    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["datetime"]).copy()
    df = df.set_index("datetime").sort_index()

    # Keep only the target column, rename to something clean (optional)
    df = df[["discharge(m3/s)"]].copy()

    return df


def read_simulated_csv(path: str) -> pd.DataFrame:
    """
    Read simulated streamflow CSV and return a DataFrame indexed by datetime
    with the required column.
    Expected columns: 'datetime', 'streamflow_m^3/s'
    """
    df = pd.read_csv(path)

    if "datetime" not in df.columns or "streamflow_m^3/s" not in df.columns:
        raise ValueError(
            f"Simulated file does not contain required columns: {path}\n"
            f"Found columns: {list(df.columns)}"
        )

    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["datetime"]).copy()
    df = df.set_index("datetime").sort_index()

    df = df[["streamflow_m^3/s"]].copy()

    return df


def subset_by_years(df: pd.DataFrame, start_year: int, end_year: int) -> pd.DataFrame:
    """Subset a datetime-indexed DataFrame to [start_year-01-01, end_year-12-31]."""
    start = pd.Timestamp(start_year, 1, 1)
    end = pd.Timestamp(end_year, 12, 31)
    return df.loc[(df.index >= start) & (df.index <= end)].copy()


def has_all_months(df: pd.DataFrame) -> bool:
    """
    Check if a datetime-indexed DataFrame has at least one record for each month (1..12),
    across the whole subset (not per-year).
    """
    if df.empty:
        return False
    months_present = set(df.index.month.unique())
    return all(m in months_present for m in range(1, 13))


# -------------------------
# Main
# -------------------------
def main():
    print("Starting bias-correction workflow...")
    print(f"Observed folder:  {OBS_DIR}")
    print(f"Simulated folder: {SIM_DIR}")
    print(f"Output folder:    {OUT_DIR}")
    print("")

    # Basic path checks
    if not os.path.isdir(OBS_DIR):
        raise FileNotFoundError(f"Observed folder not found: {OBS_DIR}")
    if not os.path.isdir(SIM_DIR):
        raise FileNotFoundError(
            f"Simulated folder not found: {SIM_DIR}\n"
            f"Please set SIM_DIR to the correct path."
        )
    os.makedirs(OUT_DIR, exist_ok=True)

    total_jobs = len(STATIONS) * len(PERIODS)
    job_counter = 0

    for station in STATIONS:
        obs_path = os.path.join(OBS_DIR, f"{station}.csv")
        sim_path = os.path.join(SIM_DIR, f"{station}.csv")

        print("=" * 80)
        print(f"Station: {station}")
        print(f"Observed file:  {obs_path}")
        print(f"Simulated file: {sim_path}")

        if not os.path.isfile(obs_path):
            print(f"[WARNING] Observed file not found. Skipping station: {station}")
            continue
        if not os.path.isfile(sim_path):
            print(f"[WARNING] Simulated file not found. Skipping station: {station}")
            continue

        try:
            obs_df_full = read_observed_csv(obs_path)
            sim_df = read_simulated_csv(sim_path)
            print(f"Loaded observed rows:  {len(obs_df_full):,}")
            print(f"Loaded simulated rows: {len(sim_df):,}")
        except Exception as e:
            print(f"[ERROR] Failed to read input files for station {station}: {e}")
            print(traceback.format_exc())
            continue

        for period_label, y0, y1 in PERIODS:
            job_counter += 1
            print("-" * 80)
            print(f"[{job_counter}/{total_jobs}] Processing period: {period_label}")

            try:
                obs_subset = subset_by_years(obs_df_full, y0, y1)
                print(f"Observed subset rows: {len(obs_subset):,}")

                # Check monthly coverage requirement (at least 1 observation per month)
                if not has_all_months(obs_subset):
                    months_present = sorted(set(obs_subset.index.month.unique())) if not obs_subset.empty else []
                    print(
                        "[SKIP] Monthly coverage requirement not met for this subset.\n"
                        f"       Months present: {months_present}\n"
                        "       Requirement: at least one value for each month 1..12."
                    )
                    continue

                print("Running GEOGloWS historical bias correction...")
                corrected_df = geoglows.bias.correct_historical(sim_df, obs_subset)

                # Save output
                out_name = f"{station}_{period_label}.csv"
                out_path = os.path.join(OUT_DIR, out_name)
                corrected_df.to_csv(out_path)

                print(f"[OK] Saved bias-corrected series to: {out_path}")

            except Exception as e:
                print(f"[ERROR] Failed for station={station}, period={period_label}: {e}")
                print(traceback.format_exc())
                continue

    print("\nAll done.")


if __name__ == "__main__":
    main()
