import os
import pandas as pd
import netCDF4 as nc
import datetime as dt

import warnings
warnings.filterwarnings("ignore")

stations_pd = pd.read_csv(r"C:\Users\jsanchez\Downloads\profesor_Alvaro\CAMELS_COL_GAUGING.csv")
stations_pd = stations_pd[stations_pd['COMID_v1'] != 0]

IDs = stations_pd['IDEAM_CODE'].tolist()
COMIDs = stations_pd['COMID_v1'].tolist()

output_folder = r"C:\Users\jsanchez\Downloads\profesor_Alvaro\Simulated_Data\GEOGLOWS_v1"

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

    fechas = pd.date_range(date_ini, date_end, freq='D')

    qout_nc = nc.Dataset('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\netcdf_files\\south_america-geoglows\\Qout_geoglows_v1_or.nc')

    try:

        values = qout_nc['Qout'][:, list(qout_nc['rivid'][:]).index(comid)]

        pairs = [list(a) for a in zip(fechas, values)]
        pairs = sorted(pairs, key=lambda x: x[0])

        simulated_df = pd.DataFrame(pairs, columns=['datetime', 'Streamflow (m3/s)'])
        simulated_df.set_index('datetime', inplace=True)

        qout_nc.close()
    except Exception as e:
        qout_nc.close()
        raise e

    # Removing Negative Values
    simulated_df[simulated_df < 0] = 0
    simulated_df.index = pd.to_datetime(simulated_df.index)
    simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
    simulated_df.index = pd.to_datetime(simulated_df.index)

    simulated_df.to_csv('C:\\Users\\jsanchez\\Downloads\\profesor_Alvaro\\Simulated_Data\\GEOGLOWS_v1\\{}.csv'.format(id))
