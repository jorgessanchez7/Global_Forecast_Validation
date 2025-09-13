import os
import xarray as xr
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

def should_download(file_path):
    return (not os.path.exists(file_path))

def get_glofas_forecast(lat: float, lon: float, fecha: str, base_dir: str = "D:\\GloFAS_Forecast"):
    """
    Lee los archivos GloFAS para una latitud, longitud y fecha determinada,
    y devuelve un DataFrame con los pronósticos de todos los ensembles.

    Parámetros
    ----------
    lat : float
        Latitud del punto de interés.
    lon : float
        Longitud del punto de interés.
    fecha : str
        Fecha en formato 'YYYYMMDD' (ejemplo: '20250801').
    base_dir : str
        Carpeta base donde están almacenados los archivos GloFAS.

    Retorna
    -------
    forecast_df : pd.DataFrame
        DataFrame con todos los ensembles concatenados.
    """

    forecast_df = pd.DataFrame()

    ensembles = [f"{i:02d}" for i in range(51)]  # '00' a '50'

    # Recorrer ensembles
    for ensemble in ensembles:
        nc_file = f"{base_dir}\\{fecha}\\dis_{ensemble}_{fecha}00.nc"
        print(nc_file)
        dataset = xr.open_dataset(nc_file)

        qout_datasets = dataset.sel(lon=lon, method="nearest").sel(lat=lat, method="nearest").dis
        time_dataset = qout_datasets.time

        ens = int(ensemble) + 1
        ens = f"{ens:02d}"  # siempre 2 dígitos

        file_df = pd.DataFrame(qout_datasets, index=time_dataset, columns=[f"ensemble_{ens}"])
        file_df.index.name = "datetime"
        file_df.sort_index(inplace=True)
        file_df[file_df < 0] = 0
        file_df.index = pd.to_datetime(file_df.index).strftime("%Y-%m-%d")
        file_df.index = pd.to_datetime(file_df.index)

        forecast_df = pd.concat([forecast_df, file_df], axis=1)

    return forecast_df


#fechas = ['20250731', '20250801', '20250802', '20250803', '20250804', '20250805', '20250806', '20250807', '20250808',
#          '20250809', '20250810', '20250811', '20250812', '20250813', '20250814', '20250815', '20250816', '20250817',
#          '20250818', '20250819', '20250820', '20250821', '20250822', '20250823', '20250824', '20250825', '20250826',
#          '20250827', '20250828', '20250829', '20250830', '20250831', '20250901', '20250902', '20250903', '20250904',
#          '20250905', '20250906', '20250907', '20250908', '20250909', '20250910', '20250911', '20250912']

fechas = ['20250731', '20250801', '20250802', '20250803', '20250804', '20250805', '20250806', '20250807', '20250808',
          '20250809', '20250810', '20250811', '20250812', '20250813', '20250814', '20250815', '20250816', '20250817',
          '20250818', '20250819', '20250820', '20250821', '20250822', '20250823', '20250824', '20250825', '20250826',
          '20250827', '20250828']

base_folder = "G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values"

stations_pd = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Stations_Comparison_v1.csv")

latitudes = stations_pd['Lat_GloFAS'].to_list()
longitudes = stations_pd['Lon_GloFAS'].to_list()

for latitude, longitude in zip(latitudes, longitudes):

    for fecha in fechas:

        print(latitude, '-', longitude, '-', fecha[0:4], '-', fecha[4:6], '-', fecha[6:8])

        folder_path = os.path.join(base_folder, f"{fecha[0:4]}-{fecha[4:6]}-{fecha[6:8]}")
        os.makedirs(folder_path, exist_ok=True)

        file_path = os.path.join(folder_path, f"{latitude}_{longitude}.csv")
        if should_download(file_path):

            df_forecast = get_glofas_forecast(latitude, longitude, fecha)
            #print(df_forecast)
            df_forecast[df_forecast < 0] = 0

            df_forecast.to_csv(file_path)

