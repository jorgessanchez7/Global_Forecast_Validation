import os
import pandas as pd
import netCDF4 as nc
import datetime as dt

import warnings
warnings.filterwarnings("ignore")

stations_pd = pd.read_csv(r"C:\Users\jsanchez\Downloads\profesor_Alvaro\CAMELS_COL_GAUGING.csv")
stations_pd = stations_pd[stations_pd['COMID_v2'] != 0]

IDs = stations_pd['IDEAM_CODE'].tolist()
COMIDs = stations_pd['COMID_v2'].tolist()

output_folder = r"C:\Users\jsanchez\Downloads\profesor_Alvaro\Simulated_Data\GEOGLOWS_v2"

for id, comid in zip(IDs, COMIDs):

    comid = int(comid)

    print(id, ' - ', comid, ' - Q')

    file_path = f"{output_folder}\\{id}.csv"

    if os.path.exists(file_path):
        print(f"File already exists: {file_path}. Skipping download.")
        continue

    unit_type = 'metric'

    date_ini = dt.datetime(1979, 1, 1)
    date_end = dt.datetime(2024, 12, 31)

    simulated_df = pd.read_csv("C:\\Users\\jsanchez\\Downloads\\profesor_Alvaro\\Simulated_Data\\GEOGLOWS_v2\\Row_Data\\{}.csv".format(comid), index_col=0)
    simulated_df[simulated_df < 0] = 0
    simulated_df.index = pd.to_datetime(simulated_df.index)
    simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
    simulated_df.index = pd.to_datetime(simulated_df.index)
    simulated_df = simulated_df.loc[simulated_df.index >= pd.to_datetime("1979-01-01")]
    simulated_df = simulated_df.loc[simulated_df.index <= pd.to_datetime("2024-12-31")]

    simulated_df.to_csv('C:\\Users\\jsanchez\\Downloads\\profesor_Alvaro\\Simulated_Data\\GEOGLOWS_v2\\{}.csv'.format(id))
