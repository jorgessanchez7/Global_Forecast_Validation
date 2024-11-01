import io
import os
import requests
import pandas as pd
import datetime as dt

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv('/Users/grad/Github/Global_Forecast_Validation/Global/World_Stations_WL_Sat.csv')

IDs = stations_pd['Station'].tolist()
COMIDs = stations_pd['COMID'].tolist()

for id, comid in zip(IDs, COMIDs):

	print(id, ' - ', comid)

	# Define the path for the output file
	output_path = '/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Simulated_Data/GEOGLOWS_v1/{}.csv'.format(comid)

	# Check if the file already exists
	if os.path.exists(output_path):
		print(f"File for COMID {comid} already exists. Skipping download.")
		continue  # Skip to the next station if the file exists

	''''Using REST API'''
	era_res = requests.get('https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id={0}&return_format=csv'.format(comid), verify=False).content
	simulated_df = pd.read_csv(io.StringIO(era_res.decode('utf-8')), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df = simulated_df.loc[simulated_df.index <= pd.to_datetime("2022-05-31")]

	simulated_df.to_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Simulated_Data/GEOGLOWS_v1/{}.csv'.format(comid))