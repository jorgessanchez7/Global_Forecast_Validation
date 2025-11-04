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

fechas = ['20241010', '20241011', '20241012', '20241013', '20241014', '20241015', '20241016', '20241017', '20241018', '20241019',
          '20241020', '20241021', '20241022', '20241023', '20241024', '20241025', '20241026', '20241027', '20241028', '20241029',
          '20241030', '20241031', '20241101', '20241102', '20241103', '20241104', '20241105', '20241106', '20241107', '20241108',
          '20241109', '20241110']

base_folder = "G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Values_GloFAS"

stations_pd = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Coordenadas_POI.csv")

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
        "area": [42.0, -2.0, 38.0, 1.0]
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