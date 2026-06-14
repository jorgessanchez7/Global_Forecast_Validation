import os
import s3fs
import argparse
import xarray as xr
import pandas as pd
import datetime as dt

# === lista de COMIDs ===
DRAINAGE_PATH = r"C:\Users\jsanchez\Downloads\profesor_Alvaro\COMID_v2.txt"

# === Carpeta de salida solicitada (Windows) ===
OUTPUT_DIR = r"C:\Users\jsanchez\Downloads\profesor_Alvaro\Simulated_Data\GEOGLOWS_v2\Row_Data"

def write_historical_csvs(start: int, finish: int) -> None:
    # 1) Abrir dataset Zarr (retrospectivo diario) en S3
    params = {"region_name": "us-west-2"}
    url = "s3://geoglows-v2/retrospective/daily.zarr"

    s3 = s3fs.S3FileSystem(anon=True, client_kwargs=params)
    s3store = s3fs.S3Map(root=url, s3=s3, check=False)
    ds = xr.open_zarr(s3store)

    # 2) Intersecar COMIDs disponibles en Zarr con tu lista (para filtrar tu universo de interés)
    ds_comids = set(ds.river_id.values)
    ec_comids = pd.read_csv(DRAINAGE_PATH)["comid"].tolist()
    comids = list(ds_comids.intersection(ec_comids))[start:finish]

    # 3) Asegurar carpeta de salida
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 4) Procesar por lotes para no cargar demasiada memoria
    chunk_size = 200  # puedes ajustar si ves mucha/muy poca memoria
    date_min = pd.Timestamp("1940-01-01")

    for start_idx in range(0, len(comids), chunk_size):
        slice_ids = comids[start_idx:start_idx + chunk_size]

        # Extraer Q para este lote de COMIDs
        df = (
            ds["Q"]
            .sel(river_id=slice_ids)
            .to_dataframe()          # -> columns: ['Q'] ; index: ['time','river_id']
            .reset_index()           # -> columns: ['time','river_id','Q']
        )

        # Renombrar a nombres finales
        df = df.rename(columns={
            "time": "Datetime",
            "river_id": "comid",
            "Q": "Retrospective Discharge (m3/s)"
        })

        # Filtrar fechas desde 1980-01-01 (por coherencia con tus particiones previas)
        df = df[df["Datetime"] >= date_min]

        # Formato de fecha %Y-%m-%d (sin hora)
        # Nota: esto la convierte a string ya formateada, como pediste.
        df["Datetime"] = df["Datetime"].dt.strftime("%Y-%m-%d")

        # 5) Escribir un CSV por comid (reach_id)
        for comid, g in df.groupby("comid", sort=False):
            out_path = os.path.join(OUTPUT_DIR, f"{int(comid)}.csv")
            # Solo las dos columnas pedidas
            g[["Datetime", "Retrospective Discharge (m3/s)"]].to_csv(
                out_path, index=False
            )

        print(f"Guardados CSVs para COMIDs {slice_ids[0]}–{slice_ids[-1]} ({len(slice_ids)} archivos)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", "-s", type=int, default=0)
    parser.add_argument("--finish", "-f", type=int, default=1000)
    args = parser.parse_args()
    print(f"Run script for element {args.start} to {args.finish}")

    write_historical_csvs(args.start, args.finish)