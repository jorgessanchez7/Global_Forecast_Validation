"""
Ensure station time series files exist (Observed, GEOGLOWS v1 OR, GEOGLOWS v1 BC, MFDC-QM).
- Observed: REQUIRED. If missing -> raise error.
- OR: if missing -> extract from regional NetCDF and write CSV.
- BC: if missing -> extract from regional NetCDF and write CSV.
- MFDC-QM: if missing -> compute using geoglows.bias.correct_historical(simulated_or, observed) and write CSV.

Assumptions:
- Metadata file: world_stations.xlsx with columns:
  folder, source, station_code, comid, region
  (case-insensitive; a mapping is applied)
- Observed CSV path pattern: {folder}/{source}/{station_code}_Q.csv
- GEOGLOWS outputs are stored under fixed roots (edit below if needed).
- NetCDF paths:
  ...\netcdf_files\{region}-geoglows\Qout_geoglows_v1_or.nc
  ...\netcdf_files\{region}-geoglows\Qout_geoglows_v1_bc.nc
- Each CSV has 2 columns: date (%Y-%m-%d) + value.
"""

import os
import pandas as pd
import numpy as np
import netCDF4 as nc
import datetime as dt
from pathlib import Path

import warnings
warnings.filterwarnings("ignore")

# Optional (only needed if you want to generate MFDC-QM when missing)
import geoglows


# ============================================================
# USER INPUTS
# ============================================================
METADATA_XLSX = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\world_stations.xlsx"

# Root where observed folders live.
# If "folder" values in the Excel are absolute paths, set OBS_ROOT = "".
#OBS_ROOT = r"G:\My Drive\Personal_Files\Post_Doc\Global_Hydroserver\Observed_Data"
OBS_ROOT = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\observed_data"

GEOGLOWS_ROOT = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1"
OUT_OR_DIR = os.path.join(GEOGLOWS_ROOT, r"Historical_Simulation_or")
OUT_BC_DIR = os.path.join(GEOGLOWS_ROOT, r"Historical_Simulation_bc")
OUT_MFDC_DIR = os.path.join(GEOGLOWS_ROOT, r"Historical_Simulation_MFDC-QM")
NETCDF_DIR = os.path.join(GEOGLOWS_ROOT, r"netcdf_files")

DATE_INI = dt.datetime(1979, 1, 1)
DATE_END = dt.datetime(2024, 12, 31)

# Minimum data needed to attempt MFDC-QM
MIN_OVERLAP_DAYS = 30

# If True, will create missing MFDC-QM files (requires geoglows installed)
CREATE_MFDCQM = True

# ============================================================
# HELPERS
# ============================================================
def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize metadata column names to a standard set."""
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    # Common aliases seen in your other files
    colmap = {
        "folder": "folder",
        "source": "source",
        "data_source": "source",
        "station_code": "station_code",
        "samplingfeaturecode": "station_code",
        "code": "station_code",
        "comid": "comid",
        "samplingfeaturetype": "comid",
        "region": "region",
        "geoglows_v1_region": "region",
    }

    out = {}
    for c in df.columns:
        out[c] = colmap.get(c, c)
    df.rename(columns=out, inplace=True)

    required = ["folder", "source", "station_code", "comid", "region"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in metadata Excel: {missing}. "
            f"Available columns: {list(df.columns)}"
        )
    return df


def read_timeseries_csv(path: str) -> pd.Series:
    """Read 2-column CSV (date, value) into a Series with DatetimeIndex."""
    df = pd.read_csv(path)
    if df.shape[1] < 2:
        raise ValueError(f"CSV has fewer than 2 columns: {path}")

    date_col = df.columns[0]
    val_col = df.columns[1]

    s = df[[date_col, val_col]].copy()
    s[date_col] = pd.to_datetime(s[date_col], format="%Y-%m-%d", errors="coerce")
    s = s.dropna(subset=[date_col])
    s = s.set_index(date_col)[val_col]
    s = pd.to_numeric(s, errors="coerce").dropna()
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()
    return s


def write_timeseries_csv(series: pd.Series, out_path: str, value_col: str = "Streamflow (m3/s)") -> None:
    """Write a Series with DatetimeIndex to CSV with the requested formatting."""
    out = pd.DataFrame({value_col: series.values}, index=series.index)
    out.index.name = "datetime"
    out.index = pd.to_datetime(out.index)
    # normalize to YYYY-MM-DD
    out.index = out.index.to_series().dt.strftime("%Y-%m-%d")
    out.index = pd.to_datetime(out.index)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path)


def get_netcdf_path(region: str, kind: str) -> str:
    """
    kind: 'or' or 'bc'
    """
    region = str(region).strip()
    if kind == "or":
        fname = "Qout_geoglows_v1_or.nc"
    elif kind == "bc":
        fname = "Qout_geoglows_v1_bc.nc"
    else:
        raise ValueError("kind must be 'or' or 'bc'")
    return os.path.join(NETCDF_DIR, f"{region}-geoglows", fname)


def extract_from_netcdf(comid: int, region: str, kind: str) -> pd.Series:
    """
    Extract daily Qout for one COMID from the correct regional netcdf.
    Returns a Series indexed by date (daily) from DATE_INI to DATE_END.
    """
    nc_path = get_netcdf_path(region, kind)
    if not os.path.exists(nc_path):
        raise FileNotFoundError(f"NetCDF not found for region='{region}', kind='{kind}': {nc_path}")

    fechas = pd.date_range(DATE_INI, DATE_END, freq="D")

    ds = nc.Dataset(nc_path)
    try:
        rivid = list(ds["rivid"][:])
        try:
            idx = rivid.index(int(comid))
        except ValueError:
            raise KeyError(f"COMID {comid} not found in rivid for {nc_path}")

        values = ds["Qout"][:, idx]
        values = np.array(values, dtype=float)

    finally:
        ds.close()

    # Defensive alignment with fechas length
    if len(values) != len(fechas):
        raise ValueError(
            f"Length mismatch for comid={comid} in {nc_path}: "
            f"Qout len={len(values)} vs dates len={len(fechas)}"
        )

    s = pd.Series(values, index=fechas, name="Streamflow (m3/s)")
    s[s < 0] = 0.0
    return s


def build_observed_path(folder: str, source: str, station_code: str) -> str:
    # If folder in excel is absolute, OBS_ROOT can be empty
    base = Path(OBS_ROOT) if OBS_ROOT else Path(".")
    # Allow folder entries like "Africa" (relative) or "E:\something" (absolute)
    folder_path = Path(folder)
    if folder_path.is_absolute():
        p = folder_path / str(source) / f"{station_code}_Q.csv"
    else:
        p = base / str(folder) / str(source) / f"{station_code}_Q.csv"
    return str(p)


# ============================================================
# MAIN
# ============================================================
def main():
    meta = pd.read_excel(METADATA_XLSX, sheet_name="selected_stations")
    meta = _standardize_columns(meta)

    # Ensure output dirs exist
    Path(OUT_OR_DIR).mkdir(parents=True, exist_ok=True)
    Path(OUT_BC_DIR).mkdir(parents=True, exist_ok=True)
    Path(OUT_MFDC_DIR).mkdir(parents=True, exist_ok=True)

    log_rows = []

    for _, row in meta.iterrows():
        folder = row["folder"]
        source = row["source"]
        station_code = str(row["station_code"])
        comid = int(row["comid"])
        region = str(row["region"]).strip()

        obs_path = build_observed_path(folder, source, station_code)
        or_path = os.path.join(OUT_OR_DIR, f"{comid}_or.csv")
        bc_path = os.path.join(OUT_BC_DIR, f"{comid}_bc.csv")
        mfdc_path = os.path.join(OUT_MFDC_DIR, f"{station_code}-{comid}_Q.csv")

        print(f"\nStation {station_code} | COMID {comid} | region={region}")
        print(f"  Observed: {obs_path}")

        # ----------------------------
        # 1) Observed must exist
        # ----------------------------
        if not os.path.exists(obs_path):
            raise FileNotFoundError(
                f"Observed file is required but missing:\n{obs_path}\n"
                f"Station={station_code}, COMID={comid}, folder={folder}, source={source}, region={region}"
            )

        observed = read_timeseries_csv(obs_path)

        # ----------------------------
        # 2) Ensure OR exists
        # ----------------------------
        created_or = False
        if not os.path.exists(or_path):
            print("  OR missing -> extracting from NetCDF...")
            s_or = extract_from_netcdf(comid=comid, region=region, kind="or")
            write_timeseries_csv(s_or, or_path, value_col="Streamflow (m3/s)")
            created_or = True
        else:
            print("  OR exists.")

        simulated_or = read_timeseries_csv(or_path)

        # ----------------------------
        # 3) Ensure BC exists
        # ----------------------------
        created_bc = False
        if not os.path.exists(bc_path):
            print("  BC missing -> extracting from NetCDF...")
            s_bc = extract_from_netcdf(comid=comid, region=region, kind="bc")
            write_timeseries_csv(s_bc, bc_path, value_col="Streamflow (m3/s)")
            created_bc = True
        else:
            print("  BC exists.")

        # ----------------------------
        # 4) Ensure MFDC-QM exists (optional)
        # ----------------------------
        created_mfdc = False
        if CREATE_MFDCQM:
            if not os.path.exists(mfdc_path):
                print("  MFDC-QM missing -> computing with geoglows...")
                # Align on overlap (geoglows.correct_historical expects DataFrames,
                # but it is robust if you pass two-column frames with datetime index.)
                obs_df = observed.to_frame(name="Observed (m3/s)")
                sim_df = simulated_or.to_frame(name="Simulated (m3/s)")

                overlap = obs_df.join(sim_df, how="inner")
                if overlap.shape[0] < MIN_OVERLAP_DAYS:
                    print(f"  MFDC-QM skipped (insufficient overlap days: {overlap.shape[0]}).")
                else:
                    corrected_df = geoglows.bias.correct_historical(sim_df, obs_df)
                    # geoglows returns a DataFrame; standardize and write
                    # keep first column
                    corrected_series = corrected_df.iloc[:, 0]
                    corrected_series = corrected_series.astype(float)
                    corrected_series[corrected_series < 0] = 0.0
                    write_timeseries_csv(corrected_series, mfdc_path, value_col="Streamflow (m3/s)")
                    created_mfdc = True
            else:
                print("  MFDC-QM exists.")

        log_rows.append({
            "station_code": station_code,
            "comid": comid,
            "region": region,
            "observed_exists": True,
            "or_exists": os.path.exists(or_path),
            "bc_exists": os.path.exists(bc_path),
            "mfdc_exists": os.path.exists(mfdc_path),
            "created_or": created_or,
            "created_bc": created_bc,
            "created_mfdc": created_mfdc,
        })

    log_df = pd.DataFrame(log_rows)
    log_path = os.path.join(GEOGLOWS_ROOT, "ensure_files_log.csv")
    log_df.to_csv(log_path, index=False)
    print(f"\nDone. Log written to: {log_path}")


if __name__ == "__main__":
    main()
