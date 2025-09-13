import os
import time
import numpy as np
import pandas as pd
import xarray as xr

import warnings
warnings.filterwarnings("ignore")

# =========================
# CONFIGURACIÓN
# =========================
BASE_DIR_DATA = r"D:\GloFAS_Forecast"  # raíz donde están los .nc por fecha
STATIONS_CSV = r"G:\My Drive\GEOGLOWS\Forecast_Comparison\Stations_Comparison_v2.csv"
OUTPUT_BASE  = r"G:\My Drive\GEOGLOWS\Forecast_Comparison\Forecast_Values"

FECHAS = ['20250805', '20250806', '20250807', '20250808', '20250809', '20250810', '20250811', '20250812', '20250813',
          '20250814', '20250815', '20250816', '20250817', '20250818', '20250819', '20250820', '20250821', '20250822',
          '20250823', '20250824', '20250825']

DATE_AS_DAY = True       # True: índice YYYY-MM-DD; False: conserva hora
ENGINE      = "netcdf4"  # suele ir bien con GloFAS

# =========================
# GRUPOS (por proximidad) Y TAMAÑO DE LOTE
# Cada entrada es: nombre_grupo: {"coords": [(lat, lon), ...], "batch_size": int}
# =========================
GROUPS = {
    # Multi-punto cercanos (minilotes)
    "colombia_andes": {
        "coords": [
            (5.975, -75.825), (4.775, -75.925), (4.425, -75.875),
            (5.225, -75.825), (4.775, -75.625), (4.125, -76.225), (4.975, -75.875),
            (4.475, -74.625), (4.225, -74.625), (7.275, -75.425), (5.775, -73.025),
            (5.675, -73.225), (5.775, -72.875), (5.525, -72.775)
        ],
        "batch_size": 4
    },
    "ecuador_costa": {
        "coords": [(-0.625, -80.325), (0.875, -79.625), (-2.175, -79.875)],
        "batch_size": 2
    },
    "orinoco_67": {
        "coords": [(0.025, -67.275)],
        "batch_size": 2
    },
    "us_wyoming": {
        "coords": [(41.175, -106.525), (41.875, -107.025), (42.575, -105.025)],
        "batch_size": 2
    },
    "balkans": {
        "coords": [(44.725, 19.775), (47.175, 20.275)],
        "batch_size": 2
    },
    "caucasus": {
        "coords": [(45.375, 40.275), (43.475, 46.375)],
        "batch_size": 2
    },
    "nigeria_chad": {
        "coords": [(10.175, 15.475), (8.675, 10.275)],
        "batch_size": 2
    },
    "brazil_ms": {
        "coords": [(-19.025, -57.475), (-15.475, -57.375)],
        "batch_size": 2
    },

    # Aislados (muy dispersos): procesa de a 1
    "south_america_isolated": {
        "coords": [(-6.125, -52.525), (-14.175, -43.675)],
        "batch_size": 1
    },
    "north_america_isolated": {
        "coords": [(41.775, -90.275), (55.275, -129.075), (18.175, -95.375)],
        "batch_size": 1
    },
    "eurasia_isolated": {
        "coords": [(53.175, 30.275), (56.475, 41.975), (44.675, 76.475)],
        "batch_size": 1
    },
    "asia_se_isolated": {
        "coords": [(26.775, 93.475), (25.575, 81.625), (28.575, 70.025), (30.375, 111.675)],
        "batch_size": 1
    },
    "africa_me_isolated": {
        "coords": [(8.175, 34.875)],
        "batch_size": 1
    },
    "oceania_seasia_isolated": {
        "coords": [(-34.125, 141.325), (-1.575, 110.425), (-4.175, 143.775)],
        "batch_size": 1
    },
}

# =========================
# UTILIDADES
# =========================
def should_write(path: str) -> bool:
    return not os.path.exists(path)

def build_mfdataset_for_date(fecha: str, base_dir: str) -> xr.Dataset:
    folder = os.path.join(base_dir, fecha)
    if not os.path.isdir(folder):
        raise FileNotFoundError(f"No existe carpeta de la fecha: {folder}")

    files = [os.path.join(folder, f"dis_{i:02d}_{fecha}00.nc") for i in range(51)]
    missing = [f for f in files if not os.path.isfile(f)]
    if missing:
        raise FileNotFoundError("Faltan ensembles para "
                                f"{fecha}:\n" + "\n".join(missing))

    ds = xr.open_mfdataset(
        files,
        combine="nested",
        concat_dim="ensemble",
        engine=ENGINE,
        decode_times=True,
        parallel=True,
        chunks={"ensemble": 1, "time": -1, "lat": 200, "lon": 200},
    )

    if "dis" not in ds.variables:
        raise KeyError("Variable 'dis' no encontrada en el dataset.")
    if not {"lon", "lat"}.issubset(ds.coords):
        raise KeyError("Coordenadas 'lon' y/o 'lat' no encontradas en el dataset.")
    if "time" not in ds.coords:
        raise KeyError("Coordenada 'time' no encontrada en el dataset.")
    if "ensemble" not in ds.dims or ds.dims["ensemble"] != 51:
        raise ValueError(f"Se esperaban 51 ensembles y se encontraron {ds.dims.get('ensemble', 0)}.")

    ds = ds.transpose(..., "time", "ensemble", "lat", "lon")
    return ds

def _nearest_indices(vals: np.ndarray, targets: np.ndarray):
    """Para cada target devuelve (idx, nearest_vals) sobre el array 1D 'vals' (asc o desc)."""
    vals = np.asarray(vals)
    targets = np.asarray(targets, dtype=float)
    asc = bool(vals[0] <= vals[-1])
    arr = vals if asc else vals[::-1]
    pos = np.searchsorted(arr, targets)
    pos = np.clip(pos, 1, len(arr) - 1)
    left = arr[pos - 1]
    right = arr[pos]
    pick_left = (targets - left) <= (right - targets)
    idx_in_arr = pos - pick_left.astype(int)
    idx = idx_in_arr if asc else (len(arr) - 1) - idx_in_arr
    nearest = vals[idx]
    return idx, nearest

def map_points_to_grid_indices(ds: xr.Dataset, lats: np.ndarray, lons: np.ndarray, tol: float = 1e-10):
    """Mapea coordenadas a índices de grilla con verificación estricta (|diff| <= tol)."""
    lat_idx, lat_near = _nearest_indices(ds["lat"].values, lats)
    lon_idx, lon_near = _nearest_indices(ds["lon"].values, lons)
    lat_diff = np.abs(lat_near - lats)
    lon_diff = np.abs(lon_near - lons)
    bad = (lat_diff > tol) | (lon_diff > tol)
    if np.any(bad):
        bad_ix = np.where(bad)[0]
        lines = []
        for i in bad_ix[:10]:
            lines.append(
                f"punto {i}: lat pedida={lats[i]} (grilla={lat_near[i]}, Δ={lat_diff[i]:.3g}); "
                f"lon pedida={lons[i]} (grilla={lon_near[i]}, Δ={lon_diff[i]:.3g})"
            )
        msg = (
            "Algunas coordenadas no existen exactamente en la grilla del NetCDF.\n"
            "Ejemplos (primeros 10):\n  - " + "\n  - ".join(lines) +
            "\nCorrige el CSV a las etiquetas exactas o ajusta la tolerancia del chequeo."
        )
        raise KeyError(msg)
    return lat_idx, lon_idx

def find_indices_for_coords_in_csv(lats_all, lons_all, coords, atol=1e-9):
    """
    Para una lista de (lat,lon) regresa los índices en los arrays lats_all/lons_all del CSV.
    Usa igualdad exacta con tolerancia numérica pequeña.
    """
    idxs = []
    for (lt, ln) in coords:
        mask = (np.isclose(lats_all, lt, atol=atol)) & (np.isclose(lons_all, ln, atol=atol))
        where = np.where(mask)[0]
        if where.size != 1:
            raise KeyError(f"Coordenada {(lt, ln)} no encontrada de forma única en el CSV (encontrados={where.size}).")
        idxs.append(int(where[0]))
    return idxs

def extract_batch(ds: xr.Dataset, lat_idx_batch: np.ndarray, lon_idx_batch: np.ndarray, date_as_day: bool = True) -> xr.DataArray:
    """Selecciona un LOTE de puntos por índice y carga solo ese sub-bloque. Devuelve ('time','ensemble','point')."""
    q = ds["dis"].isel(lat=("point", lat_idx_batch), lon=("point", lon_idx_batch))
    if set(("time", "ensemble", "point")) - set(q.dims):
        q = q.transpose("time", "ensemble", "point")
    if date_as_day:
        t = pd.to_datetime(q["time"].values).normalize()
        q = q.assign_coords(time=("time", t))
    q = q.load()
    return q

def save_batch_csvs(q: xr.DataArray, lats_batch: np.ndarray, lons_batch: np.ndarray, out_folder: str) -> None:
    """Escribe un CSV por punto con columnas ensemble_01..ensemble_51 para el lote."""
    os.makedirs(out_folder, exist_ok=True)
    q = q.transpose("time", "ensemble", "point")
    time_index = pd.to_datetime(q["time"].values)
    n_ens = q.sizes["ensemble"]
    cols = [f"ensemble_{i+1:02d}" for i in range(n_ens)]
    data = q.values  # ndarray (time, ensemble, point)
    for p in range(q.sizes["point"]):
        lat = float(lats_batch[p]); lon = float(lons_batch[p])
        out_path = os.path.join(out_folder, f"{lat}_{lon}.csv")
        if not should_write(out_path):
            continue
        arr = data[:, :, p]
        df = pd.DataFrame(arr, index=time_index, columns=cols).clip(lower=0)
        df.to_csv(out_path)

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # Leer estaciones (se esperan columnas 'Lat_GloFAS' y 'Lon_GloFAS')
    stations = pd.read_csv(STATIONS_CSV)
    if not {"Lat_GloFAS", "Lon_GloFAS"}.issubset(stations.columns):
        raise KeyError("El CSV debe contener columnas 'Lat_GloFAS' y 'Lon_GloFAS'.")

    lats_all = stations["Lat_GloFAS"].astype(float).to_numpy()
    lons_all = stations["Lon_GloFAS"].astype(float).to_numpy()

    # Convertir grupos de coords -> grupos de índices respecto al CSV
    GROUPS_BY_INDEX = {}
    all_group_indices = set()
    for name, meta in GROUPS.items():
        idxs = find_indices_for_coords_in_csv(lats_all, lons_all, meta["coords"])
        GROUPS_BY_INDEX[name] = {"idxs": idxs, "batch_size": meta["batch_size"]}
        all_group_indices.update(idxs)

    # Verificación: que los 51 puntos del CSV estén cubiertos por los grupos
    missing = sorted(set(range(len(lats_all))) - all_group_indices)
    if missing:
        raise RuntimeError(f"Hay índices del CSV no cubiertos por los grupos: {missing}")

    for fecha in FECHAS:
        print(f"\n=== Procesando fecha {fecha} ===")
        t0 = time.time()

        # Abrir 51 ensembles (lazy)
        ds = build_mfdataset_for_date(fecha, BASE_DIR_DATA)

        # Mapear TODOS los puntos a índices de grilla (estricto)
        lat_idx_all, lon_idx_all = map_points_to_grid_indices(ds, lats_all, lons_all, tol=1e-10)

        # Carpeta de salida YYYY-MM-DD
        out_folder = os.path.join(OUTPUT_BASE, f"{fecha[0:4]}-{fecha[4:6]}-{fecha[6:8]}")
        os.makedirs(out_folder, exist_ok=True)

        # Procesar grupo por grupo
        for gname, meta in GROUPS_BY_INDEX.items():
            idxs = meta["idxs"]
            batch_size = int(meta["batch_size"])
            print(f"  > Grupo '{gname}' ({len(idxs)} puntos) - batch_size={batch_size}")

            # Partir el grupo en minilotes
            for start in range(0, len(idxs), batch_size):
                end = min(start + batch_size, len(idxs))
                sel = idxs[start:end]

                # Indices de grilla para este minilote
                lat_idx_batch = lat_idx_all[sel]
                lon_idx_batch = lon_idx_all[sel]
                lats_batch = lats_all[sel]
                lons_batch = lons_all[sel]

                tb0 = time.time()
                q_batch = extract_batch(ds, lat_idx_batch, lon_idx_batch, date_as_day=DATE_AS_DAY)
                tb1 = time.time()

                save_batch_csvs(q_batch, lats_batch, lons_batch, out_folder)
                tb2 = time.time()

                print(f"    - Lote {start}-{end-1}: load={tb1-tb0:.1f}s, write={tb2-tb1:.1f}s")

                del q_batch  # liberar memoria del lote

        t1 = time.time()
        print(f"Fecha {fecha} completada en {t1-t0:.1f}s")

        del ds  # liberar dataset