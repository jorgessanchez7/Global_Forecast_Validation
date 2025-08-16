import io
import os
import requests
import geoglows
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

def should_download(file_path):
    return (not os.path.exists(file_path)) or (os.path.getsize(file_path) <= 5120)  # 5KB = 5120 bytes

def download_geoglows_forecast(comid_v1, comid_v2, fecha, base_folder):
    folder_path = os.path.join(base_folder, f"{fecha[0:4]}-{fecha[4:6]}-{fecha[6:8]}")
    os.makedirs(folder_path, exist_ok=True)

    # Archivo V1
    file_path_v1 = os.path.join(folder_path, f"{comid_v1}.csv")
    if should_download(file_path_v1):
        #print(f"⬇️ Descargando GEOGloWS v1 para COMID {comid_v1}...")
        url_1_v1 = f'https://geoglows.ecmwf.int/api/ForecastEnsembles/?reach_id={comid_v1}&date={fecha}&ensemble=1-52&return_format=csv'
        try:
            s = requests.get(url_1_v1, verify=False).content
            df1_v1 = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)
            df1_v1[df1_v1 < 0] = 0
            df1_v1.index = pd.to_datetime(df1_v1.index)
            df1_v1.index = df1_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            df1_v1.index = pd.to_datetime(df1_v1.index)
            df1_v1.rename(columns={col: col.replace("_m^3/s", "") for col in df1_v1.columns}, inplace=True)
            df1_v1.to_csv(file_path_v1)
        except Exception as e:
            #print(f"⚠️ Error descargando v1 para {comid_v1}: {e}")
            a=1
    else:
        #print(f"✔️ Archivo v1 ya existe y es válido: {file_path_v1}")
        a = 2

    # Archivo V2
    file_path_v2 = os.path.join(folder_path, f"{comid_v2}.csv")
    if should_download(file_path_v2):
        #print(f"⬇️ Descargando GEOGloWS v2 para COMID {comid_v2}...")
        url_1_v2 = f'https://geoglows.ecmwf.int/api/v2/forecastensemble/{comid_v2}?date={fecha}&format=csv'
        try:
            r = requests.get(url_1_v2, verify=False)
            if r.status_code == 200:
                s = requests.get(url_1_v2, verify=False).content
                df1_v2 = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)
                df1_v2[df1_v2 < 0] = 0
                df1_v2.index = pd.to_datetime(df1_v2.index)
                df1_v2.index = df1_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
                df1_v2.index = pd.to_datetime(df1_v2.index)
                df1_v2.to_csv(file_path_v2)
            else:
                #print(f"❌ Respuesta {r.status_code} para V2 – COMID {comid_v2}")
                try:
                    df1_v2 = geoglows.data.forecast_ensembles(comid_v2, date=fecha)
                    df1_v2[df1_v2 < 0] = 0
                    df1_v2.index = pd.to_datetime(df1_v2.index)
                    df1_v2.index = df1_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
                    df1_v2.index = pd.to_datetime(df1_v2.index)
                    df1_v2.to_csv(file_path_v2)
                except Exception as e:
                    # print(f"⚠️ Error descargando v2 para {comid_v2}: {e}")
                    b = 1
        except Exception as e:
            #print(f"⚠️ Error descargando v2 para {comid_v2}: {e}")
            b = 2
    else:
        #print(f"✔️ Archivo v2 ya existe y es válido: {file_path_v2}")
        b = 3


fechas = ['20250610', '20250611', '20250612', '20250613', '20250614', '20250615', '20250616', '20250617', '20250618', '20250619',
          '20250620', '20250621', '20250622', '20250623', '20250624', '20250625', '20250626', '20250627', '20250628', '20250629',
          '20250630', '20250701', '20250702', '20250703', '20250704', '20250705', '20250706', '20250707', '20250708', '20250709',
          '20250710', '20250711', '20250712', '20250713', '20250714', '20250715', '20250716', '20250717', '20250718', '20250719',
          '20250720', '20250721', '20250722', '20250723', '20250724', '20250725', '20250726', '20250727', '20250728', '20250729',
          '20250730', '20250731', '20250801', '20250802', '20250803', '20250804', '20250805', '20250806', '20250807', '20250808',
          '20250809', '20250810', '20250811', '20250812', '20250813', '20250814', '20250815']

base_folder = "G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values"

stations_pd = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Stations_Comparison.csv")

comids_v1 = stations_pd['COMID_v1'].to_list()
comids_v2 = stations_pd['COMID_v2'].to_list()

for fecha in fechas:

    print(fecha)

    for comid1, comid2 in zip(comids_v1, comids_v2):

        print(comid1, '-', comid2, '-', fecha[0:4], '-', fecha[4:6], '-', fecha[6:8])

        download_geoglows_forecast(comid1, comid2, fecha, base_folder)
