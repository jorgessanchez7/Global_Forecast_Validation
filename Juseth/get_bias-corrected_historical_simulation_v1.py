import math
import geoglows
import numpy as np
import pandas as pd
from scipy import interpolate

import warnings
warnings.filterwarnings('ignore')


stations_pd = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\World_Stations.csv')
stations_pd = stations_pd[stations_pd['samplingFeatureType'] != 0]
stations_pd = stations_pd[stations_pd['Q'] == 'YES']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'south_america']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'north_america']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'europe']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'central_america']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'africa']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'australia']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'islands']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'japan']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'south_asia']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'central_asia']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'east_asia']
#stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'middle_east']
stations_pd = stations_pd[stations_pd['GEOGloWS_v1_region'] == 'west_asia']

Folders = stations_pd['Folder'].tolist()
Sources = stations_pd['Data_Source'].tolist()
IDs = stations_pd['samplingFeatureCode'].tolist()
COMIDs = stations_pd['samplingFeatureType'].tolist()
Names = stations_pd['name'].tolist()

for id, name, comid, folder, source in zip(IDs, Names, COMIDs, Folders, Sources):

	print(id, ' - ', name, ' - ', comid, ' - Q')

	#Observed Data
	df = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_Q.csv'.format(folder, source, id), na_values=-9999, index_col=0)
	df.index = pd.to_datetime(df.index)
	observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)
	observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
	observed_df.index = pd.to_datetime(observed_df.index)

	#Simulated Data
	simulated_df = pd.read_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Historical_Simulation_or\\{}_or.csv'.format(comid), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	#Getting the Bias Corrected Simulation
	try:
		corrected_df = geoglows.bias.correct_historical(simulated_df, observed_df)
		#corrected_df = correct_bias(simulated_df, observed_df)
		corrected_df.to_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Historical_Simulation_MFDC-QM\\{0}-{1}_Q.csv'.format(id, comid))
	except Exception as e:
		print(e)