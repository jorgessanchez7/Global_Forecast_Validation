import numpy as np
import pandas as pd

import h5py
import xarray as xr

ds1 = xr.open_dataset("/Users/grad/Downloads/era_5_1979_01_01.nc", 'r')
ds2 = xr.open_dataset("/Users/grad/Downloads/era5_Ro1_19790101.nc", engine="netcdf4")




stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Stations.csv')
stations_pd = stations_pd[stations_pd['samplingFeatureType'] != 0]

UIDS = stations_pd['uid'].tolist()
CODEs = stations_pd['samplingFeatureCode'].tolist()
COMIDs = stations_pd['samplingFeatureType'].tolist()
Names = stations_pd['name'].tolist()
Folders = stations_pd['Folder'].tolist()
DataSources = stations_pd['Data_Source'].tolist()
Countries = stations_pd['country_name'].tolist()

for uid, code, comid, name, folder, data_source, country in zip(UIDS, CODEs, COMIDs, Names, Folders, DataSources, Countries):

	print(uid, ' - ', code, ' - ', comid, ' - ', name, ' - ', country)

	file_path = '/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Simulated_Data/GEOGLOWS_v1/{0}.csv'.format(comid)
	df1 = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Simulated_Data/GEOGLOWS_v1/{0}.csv'.format(comid), index_col=0)

	#file_path = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Hydroserver/Simulated_Data/GEOGLOWS_v1/{0}.csv'.format(comid)
	#df1 = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Hydroserver/Simulated_Data/GEOGLOWS_v1/{0}.csv'.format(comid), index_col=0)
	
	df1.index = pd.to_datetime(df1.index)
	df1.index = df1.index.to_series().dt.strftime("%Y-%m-%d")
	df1.index = pd.to_datetime(df1.index)

	#print(df1)