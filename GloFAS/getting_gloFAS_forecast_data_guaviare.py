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

#fechas = ['20250610', '20250611', '20250612', '20250613', '20250614', '20250615', '20250616', '20250617', '20250618', '20250619',
#          '20250620', '20250621', '20250622', '20250623', '20250624', '20250625', '20250626', '20250627', '20250628', '20250629',
#          '20250630', '20250701', '20250702', '20250703', '20250704', '20250705', '20250706', '20250707', '20250708', '20250709',
#          '20250710', '20250711', '20250712', '20250713', '20250714', '20250715', '20250716', '20250717', '20250718', '20250719',
#          '20250720', '20250721', '20250722', '20250723', '20250724', '20250725', '20250726', '20250727', '20250728', '20250729',
#          '20250730', '20250731', '20250801', '20250802', '20250803', '20250804', '20250805', '20250806', '20250807', '20250808',
#          '20250809', '20250810', '20250811', '20250812', '20250813', '20250814', '20250815', '20250816', '20250817', '20250818',
#          '20250819', '20250820', '20250821', '20250822', '20250823', '20250824', '20250825', '20250826', '20250827', '20250828',
#          '20250829', '20250830', '20250831', '20250901', '20250902', '20250903', '20250904', '20250905', '20250906', '20250907',
#          '20250908', '20250909', '20250910', '20250911', '20250912']

fechas = ['20250610', '20250611', '20250612', '20250613', '20250614', '20250615', '20250616', '20250617', '20250618', '20250619',
          '20250620', '20250621', '20250622', '20250623', '20250624', '20250625', '20250626', '20250627', '20250628', '20250629',
          '20250630', '20250701', '20250702', '20250703', '20250704', '20250705', '20250706', '20250707', '20250708', '20250709',
          '20250710', '20250711', '20250712', '20250713', '20250714', '20250715', '20250716', '20250717', '20250718', '20250719',
          '20250720', '20250721', '20250722', '20250723', '20250724', '20250725', '20250726', '20250727', '20250728', '20250729',
          '20250730', '20250731']

base_folder = "G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values"

stations_pd = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Stations_Comparison_v1.csv")

latitudes = stations_pd['Lat_GloFAS'].to_list()
longitudes = stations_pd['Lon_GloFAS'].to_list()


for latitude, longitude in zip(latitudes, longitudes):

    for fecha in fechas:

        output_folder = f"D:\\GloFAS_Forecast_2\\{fecha}"
        original_dir = os.getcwd()

        folder_path = os.path.join(base_folder, f"{fecha[0:4]}-{fecha[4:6]}-{fecha[6:8]}")
        os.makedirs(folder_path, exist_ok=True)

        print(latitude, '-', longitude, '-', fecha[0:4], '-', fecha[4:6], '-', fecha[6:8])

        file_path = os.path.join(folder_path, f"{latitude}_{longitude}.csv")
        if should_download(file_path):

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
                "area": [math.ceil(latitude), math.floor(longitude), math.floor(latitude), math.ceil(longitude)]
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