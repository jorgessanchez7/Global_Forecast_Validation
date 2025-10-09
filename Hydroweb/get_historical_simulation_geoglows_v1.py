import io
import os
import requests
import pandas as pd
import datetime as dt

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Reviewing_Papers_and_Proposals\\Paper_06\\SABER_Bhutan\\Selected_Stations_Bhutan.csv')

stations_pd = stations_pd[stations_pd['samplingFeatureType'] != 0]

IDs = stations_pd['samplingFeatureCode'].tolist()
COMIDs = stations_pd['samplingFeatureType'].tolist()
Names = stations_pd['name'].tolist()

output_folder = "E:\\GEOGloWS\\01_Simulated_Values\\v1"

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	file_path = f"{output_folder}/{comid}.csv"

	# Check if the file exists before downloading
	if os.path.exists(file_path):
		print(f"File already exists: {file_path}. Skipping download.")
		continue  # Skip to the next station

	''''Using REST API'''
	era_res = requests.get('https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id={0}&return_format=csv'.format(comid), verify=False).content
	simulated_df = pd.read_csv(io.StringIO(era_res.decode('utf-8')), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df = simulated_df.loc[simulated_df.index <= pd.to_datetime("2022-05-31")]
	#simulated_df = simulated_df.loc[simulated_df.index >= pd.to_datetime("1980-01-01")]

	simulated_df.to_csv('E:\\GEOGloWS\\01_Simulated_Values\\v1\\{}.csv'.format(comid))