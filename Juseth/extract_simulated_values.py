import pandas as pd
import netCDF4 as nc
import datetime as dt

import warnings
warnings.filterwarnings("ignore")

stations_pd = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\World_Stations.csv')
stations_pd = stations_pd[stations_pd['samplingFeatureType'] != 0]
stations_pd = stations_pd[stations_pd['Q'] == 'YES']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'south_america']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'north_america']
stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'europe']

Folders = stations_pd['Folder'].tolist()
Sources = stations_pd['Data_Source'].tolist()
IDs = stations_pd['samplingFeatureCode'].tolist()
COMIDs = stations_pd['samplingFeatureType'].tolist()
Names = stations_pd['name'].tolist()

for id, name, comid, folder, source in zip(IDs, Names, COMIDs, Folders, Sources):

    print(id, ' - ', name, ' - ', comid, ' - Q')

    unit_type = 'metric'

    date_ini = dt.datetime(1979, 1, 1)
    date_end = dt.datetime(2024, 12, 31)

    fechas = pd.date_range(date_ini, date_end, freq='D')

    #qout_nc = nc.Dataset('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\netcdf_files\\south_america-geoglows\\Qout_geoglows_v1_or.nc')
    #qout_nc = nc.Dataset('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\netcdf_files\\north_america-geoglows\\Qout_geoglows_v1_or.nc')
    qout_nc = nc.Dataset('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\netcdf_files\\europe-geoglows\\Qout_geoglows_v1_or.nc')

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

    simulated_df.to_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Historical_Simulation_or\\{}_or.csv'.format(comid))
