import os
import glob
import math
import cdsapi
import shutil
import zipfile
import xarray as xr
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

def should_download(file_path):
    #return (not os.path.exists(file_path)) or (os.path.getsize(file_path) <= 5120)  # 5KB = 5120 bytes
    return (not os.path.exists(file_path))

fechas = ['20260110', '20260111', '20260112', '20260113', '20260114', '20260115', '20260116', '20260117', '20260118', '20260119',
          '20260120', '20260121', '20260122', '20260123', '20260124', '20260125', '20260126', '20260127', '20260128', '20260129',
          '20260130', '20260131', '20260201', '20260202', '20260203', '20260204', '20260205', '20260206', '20260207', '20260208',
          '20260209', '20260210', '20260211', '20260212', '20260213', '20260214', '20260215', '20260216', '20260217', '20260218',
          '20260219', '20260220', '20260221', '20260222', '20260223', '20260224', '20260225', '20260226', '20260227', '20260228',
          '20260301', '20260302', '20260303', '20260304', '20260305', '20260306', '20260307', '20260308', '20260309']

base_folder = "D:\\GEOGLOWS\\Cordoba_Floods"

stations_pd = pd.read_csv("C:\\Users\\jsanchez\\Downloads\\Estaciones_Rio_Sinu.csv")

latitudes = stations_pd['Lat_GloFAS'].to_list()
longitudes = stations_pd['Lon_GloFAS'].to_list()

for fecha in fechas:

    output_folder = f"D:\\GloFAS_Forecast_2\\{fecha}"
    original_dir = os.getcwd()

    folder_path = os.path.join(base_folder, f"{fecha[0:4]}-{fecha[4:6]}-{fecha[6:8]}")
    os.makedirs(folder_path, exist_ok=True)

    dataset = "cems-glofas-forecast"
    request = {
        "system_version": ["operational"],
        "hydrological_model": ["lisflood"],
        "product_type": ["control_forecast", "ensemble_perturbed_forecasts"],
        "variable": "river_discharge_in_the_last_24_hours",
        "year": [fecha[:4]],
        "month": [fecha[4:6]],
        "day": [fecha[6:]],
        "leadtime_hour": [str(lt) for lt in range(24, 721, 24)],
        "data_format": "netcdf",
        "download_format": "zip",
        "area": [10, -77, 8, -75]
    }

    client = cdsapi.Client()
    client.retrieve(dataset, request).download()

    zip_files = glob.glob("*.zip")
    if len(zip_files) != 1:
        raise RuntimeError("No se encontró exactamente un archivo .zip descargado.")

    zip_filename = zip_files[0]

    os.makedirs(output_folder, exist_ok=True)

    destination_zip = os.path.join(output_folder, zip_filename)
    shutil.move(zip_filename, destination_zip)

    with zipfile.ZipFile(destination_zip, 'r') as zip_ref:
        zip_ref.extractall(output_folder)

    os.remove(destination_zip)

    files = [
        "{0}\\data_0.nc".format(output_folder),
        "{0}\\data_1.nc".format(output_folder)
    ]

    for latitude, longitude in zip(latitudes, longitudes):

        print(latitude, '-', longitude, '-', fecha[0:4], '-', fecha[4:6], '-', fecha[6:8])

        file_path = os.path.join(folder_path, f"{latitude}_{longitude}.csv")

        forecast_df = pd.DataFrame()
        ensemble_counter = 1

        for file in files:
            ds = xr.open_dataset(file)

            forecast_datetimes = pd.to_datetime(ds['valid_time'].values)

            # Seleccionar descarga más cercana
            discharge = ds['dis24'].sel(latitude=latitude, longitude=longitude, method='nearest')

            # Si forecast_df aún no tiene índice, establecerlo ahora
            if forecast_df.empty:
                forecast_df = pd.DataFrame(index=forecast_datetimes)

            if 'number' in discharge.dims:
                # Múltiples ensambles (data_1.nc)
                for ens in discharge['number'].values:
                    col_name = f"ensemble_{ensemble_counter:02d}"
                    forecast_df[col_name] = discharge.sel(number=ens).values
                    ensemble_counter += 1
            else:
                # Control forecast también será parte del conjunto
                col_name = f"ensemble_{ensemble_counter:02d}"
                forecast_df[col_name] = discharge.values
                ensemble_counter += 1

            ds.close()

        # Reemplazar negativos por cero
        forecast_df[forecast_df < 0] = 0

        forecast_df.to_csv(file_path)

        os.chdir(original_dir)