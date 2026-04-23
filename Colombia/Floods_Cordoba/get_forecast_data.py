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


fechas = ['20260110', '20260111', '20260112', '20260113', '20260114', '20260115', '20260116', '20260117', '20260118', '20260119',
          '20260120', '20260121', '20260122', '20260123', '20260124', '20260125', '20260126', '20260127', '20260128', '20260129',
          '20260130', '20260131', '20260201', '20260202', '20260203', '20260204', '20260205', '20260206', '20260207', '20260208',
          '20260209', '20260210', '20260211', '20260212', '20260213', '20260214', '20260215', '20260216', '20260217', '20260218',
          '20260219', '20260220', '20260221', '20260222', '20260223', '20260224', '20260225', '20260226', '20260227', '20260228',
          '20260301', '20260302', '20260303', '20260304', '20260305', '20260306', '20260307', '20260308', '20260309']

base_folder = "D:\\GEOGLOWS\\Cordoba_Floods"

stations_pd = pd.read_csv("C:\\Users\\jsanchez\\Downloads\\Estaciones_Rio_Sinu.csv")

comids_v1 = stations_pd['COMID_v1'].to_list()
comids_v2 = stations_pd['COMID_v2'].to_list()

for fecha in fechas:

    print(fecha)

    for comid1, comid2 in zip(comids_v1, comids_v2):

        print(comid1, '-', comid2, '-', fecha[0:4], '-', fecha[4:6], '-', fecha[6:8])

        download_geoglows_forecast(comid1, comid2, fecha, base_folder)
