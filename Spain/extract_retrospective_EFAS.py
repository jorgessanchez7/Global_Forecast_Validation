import os
import glob
import numpy as np
import xarray as xr
import pandas as pd
from pathlib import Path


# ====== CONFIGURACIÓN ======
# Carpeta con NetCDF (1 por año; 2025 puede venir en 2 archivos)
nc_dir = Path(r"D:\EFAS_Retrospective")
# Archivo con puntos (usa tus columnas: Lat_EFAS, Lon_EFAS)
points_path = Path(r"G:\My Drive\Personal_Files\Post_Doc\GEOGLOWS_Applications\Spain\Coordenadas_POI.csv")  # <-- cambia a tu ruta real
points_sep = ","  # usa "\t" si tu tabla está separada por tabulaciones
# Carpeta de salida
out_dir = Path(r"G:\My Drive\Personal_Files\Post_Doc\GEOGLOWS_Applications\Spain\Retrospective_EFAS")
out_dir.mkdir(parents=True, exist_ok=True)

# Variable y coordenadas esperadas en EFAS historical
VAR_NAME = "dis06"        # river_discharge_in_the_last_24_hours
LAT_NAME = "latitude"
LON_NAME = "longitude"
TIME_NAME = "time"

# ====== UTILIDADES ======
def list_nc_files(nc_folder: Path):
    """Lista y ordena NetCDF por nombre; asegura que 2025a/2025b queden contiguos."""
    files = sorted(glob.glob(str(nc_folder / "*.nc")))
    if not files:
        raise FileNotFoundError(f"No se encontraron .nc en {nc_folder}")
    return files

def safe_sel_point(ds: xr.Dataset, lat: float, lon: float):
    """Selecciona punto más cercano (lat/lon) con tolerancia básica al antimeridiano."""
    # Normaliza longitud a [-180, 180] si fuera necesario
    if ds[LON_NAME].min() < 0 and ds[LON_NAME].max() <= 180:
        # dataset está en [-180,180] -> normalizamos lon
        lon = ((lon + 180) % 360) - 180
    elif ds[LON_NAME].min() >= 0 and ds[LON_NAME].max() > 180:
        # dataset está en [0,360] -> normalizamos lon
        lon = lon % 360
    return ds.sel({LAT_NAME: lat, LON_NAME: lon}, method="nearest")

def extract_series_from_file(nc_file: str, lat: float, lon: float) -> pd.DataFrame:
    """Abre un NetCDF, extrae serie en (lat, lon) y retorna DataFrame."""
    with xr.open_dataset(nc_file) as ds:
        if VAR_NAME not in ds:
            raise KeyError(f"{VAR_NAME} no está en {nc_file}. Variables: {list(ds.data_vars)}")

        point = safe_sel_point(ds, lat, lon)
        da = point[VAR_NAME]

        # A DataFrame con índice de tiempo
        df = da.to_series().to_frame(name="Streamflow (m3/s)")
        df.index.name = "Datetime"

    # Limpieza
    df.index = pd.to_datetime(df.index)
    if df.index.tz is not None:
        df.index = df.index.tz_convert(None)

    # ✅ NO agrupar por día → conservar pasos de 6 horas
    df[df < 0] = 0
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]

    # ✅ Formatear índice con hora
    df.index = df.index.strftime("%Y-%m-%d %H:%M:%S")
    df.index = pd.to_datetime(df.index)

    return df

def concat_time_series(nc_files, lat: float, lon: float) -> pd.DataFrame:
    """Concatena la serie de múltiples NetCDF ordenados por tiempo."""
    parts = []
    for f in nc_files:
        try:
            df = extract_series_from_file(f, lat, lon)
            parts.append(df)
        except Exception as e:
            print(f"[WARN] {f} -> {e}")
    if not parts:
        raise RuntimeError("No se pudo extraer ninguna serie de los archivos NetCDF.")
    out = pd.concat(parts).sort_index()
    # dedup por fecha (si hay solapes entre archivos, p.ej. 2025a/2025b)
    out = out[~out.index.duplicated(keep="last")]
    return out

def latlon_to_filename(lat: float, lon: float) -> str:
    """Nombre de archivo lat_lon con 3 decimales (consistente con tu tabla)."""
    return f"{lat:.6f}_{lon:.6f}.csv".replace("+", "")

# ====== MAIN ======
def main():
    # Lee puntos
    pts = pd.read_csv(points_path, sep=points_sep)
    if not {"Lat_EFAS", "Lon_EFAS"}.issubset(pts.columns):
        raise ValueError("El archivo de puntos debe contener columnas 'Lat_EFAS' y 'Lon_EFAS'.")

    nc_files = list_nc_files(nc_dir)

    # Opcional: ordenar por año detectado en el nombre si está presente
    # (Si tus archivos se llaman, p.ej., EFAS_YYYY.nc). Si no, el sorted() inicial suele bastar.
    # nc_files = sorted(nc_files, key=lambda p: ''.join(filter(str.isdigit, os.path.basename(p))))

    for i, row in pts.iterrows():
        lat = float(row["Lat_EFAS"])
        lon = float(row["Lon_EFAS"])

        try:
            ts = concat_time_series(nc_files, lat, lon)
            # Guarda CSV
            fname = latlon_to_filename(lat, lon)
            out_path = out_dir / fname
            ts.to_csv(out_path, float_format="%.6f")
            print(f"[OK] Guardado: {out_path}")
        except Exception as e:
            print(f"[ERROR] Punto (lat={lat}, lon={lon}) -> {e}")

if __name__ == "__main__":
    main()
