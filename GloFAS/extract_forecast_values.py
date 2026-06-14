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


fechas = ['20250731', '20250801', '20250802', '20250803', '20250804', '20250805', '20250806', '20250807', '20250808', '20250809',
          '20250810', '20250811', '20250812', '20250813', '20250814', '20250815', '20250816', '20250817', '20250818', '20250819',
          '20250820', '20250821', '20250822', '20250823', '20250824', '20250825', '20250826', '20250827', '20250828', '20250829',
          '20250830', '20250831', '20250901', '20250902', '20250903', '20250904', '20250905', '20250906', '20250907', '20250908',
          '20250909', '20250910', '20250911', '20250912', '20250913', '20250914', '20250915', '20250916', '20250917', '20250918',
          '20250919', '20250920', '20250921', '20250922', '20250923', '20250924', '20250925', '20250926', '20250927', '20250928',
          '20250929', '20250930', '20251001', '20251002', '20251003', '20251004', '20251005', '20251006', '20251007', '20251008',
          '20251009', '20251010', '20251011', '20251012', '20251013', '20251014', '20251015', '20251016', '20251017', '20251018',
          '20251019', '20251020', '20251021', '20251022', '20251023', '20251024', '20251025', '20251026', '20251027', '20251028',
          '20251029', '20251030', '20251031', '20251101', '20251102', '20251103', '20251104', '20251105', '20251106', '20251107',
          '20251108', '20251109', '20251110', '20251111', '20251112', '20251113', '20251114', '20251115', '20251116', '20251117',
          '20251118', '20251119', '20251120', '20251121', '20251122', '20251123', '20251124', '20251125', '20251126', '20251127',
          '20251128', '20251129', '20251130', '20251201', '20251202', '20251203', '20251204', '20251205', '20251206', '20251207',
          '20251208', '20251209', '20251210', '20251211', '20251212', '20251213', '20251214', '20251215', '20251216', '20251217',
          '20251218', '20251219', '20251220', '20251221', '20251222', '20251223', '20251224', '20251225', '20251226', '20251227',
          '20251228', '20251229', '20251230', '20251231', '20260101', '20260102', '20260103', '20260104', '20260105', '20260106',
          '20260107', '20260108', '20260109', '20260110', '20260111', '20260112', '20260113', '20260114', '20260115', '20260116',
          '20260117', '20260118', '20260119', '20260120', '20260121', '20260122', '20260123', '20260124', '20260125', '20260126',
          '20260127', '20260128', '20260129', '20260130', '20260131', '20260201', '20260202', '20260203', '20260204', '20260205',
          '20260206', '20260207', '20260208', '20260209', '20260210', '20260211', '20260212', '20260213', '20260214', '20260215',
          '20260216', '20260217', '20260218', '20260219', '20260220', '20260221', '20260222', '20260223', '20260224', '20260225',
          '20260226', '20260227', '20260228', '20260301', '20260302', '20260303', '20260304', '20260305', '20260306', '20260307',
          '20260308', '20260309', '20260310', '20260311', '20260312', '20260313', '20260314', '20260315', '20260316', '20260317',
          '20260318', '20260319', '20260320', '20260321', '20260322', '20260323', '20260324', '20260325', '20260326', '20260327',
          '20260328', '20260329', '20260330', '20260331', '20260401', '20260402', '20260403', '20260404', '20260405', '20260406',
          '20260407', '20260408', '20260409', '20260410', '20260411', '20260412', '20260413', '20260414', '20260415', '20260416',
          '20260417', '20260418', '20260419', '20260420', '20260421', '20260422', '20260423', '20260424', '20260425', '20260426',
          '20260427', '20260428', '20260429', '20260430', '20260501', '20260502', '20260503', '20260504', '20260505', '20260506',
          '20260507', '20260508', '20260509', '20260510', '20260511', '20260512', '20260513', '20260514', '20260515', '20260516',
          '20260517', '20260518', '20260519', '20260520', '20260521', '20260522', '20260523', '20260524', '20260525', '20260526',
          '20260527', '20260528', '20260529', '20260530', '20260531', '20260601', '20260602', '20260603', '20260604', '20260605',
          '20260606', '20260607', '20260608', '20260609', '20260610', '20260611', '20260612']

base_folder = "D:\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values"

stations_pd = pd.read_csv("D:\\GEOGLOWS\\Forecast_Comparison\\Stations_Comparison_v1.csv")

latitudes = stations_pd['Lat_GloFAS'].to_list()
longitudes = stations_pd['Lon_GloFAS'].to_list()

#for latitude, longitude in zip(latitudes, longitudes):
for fecha in fechas:

    #for fecha in fechas:
    for latitude, longitude in zip(latitudes, longitudes):

        print(latitude, '-', longitude, '-', fecha[0:4], '-', fecha[4:6], '-', fecha[6:8])

        folder_path = os.path.join(base_folder, f"{fecha[0:4]}-{fecha[4:6]}-{fecha[6:8]}")
        os.makedirs(folder_path, exist_ok=True)

        file_path = os.path.join(folder_path, f"{latitude}_{longitude}.csv")
        if should_download(file_path):

            df_forecast = get_glofas_forecast(latitude, longitude, fecha)
            #print(df_forecast)
            df_forecast[df_forecast < 0] = 0

            df_forecast.to_csv(file_path)

