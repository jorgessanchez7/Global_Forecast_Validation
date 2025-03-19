import io
import os
import requests
import pandas as pd
import datetime as dt

import warnings
warnings.filterwarnings('ignore')

#stations_pd = pd.read_csv('/Users/grad/Github/Global_Forecast_Validation/Global/World_Stations.csv')
stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Stations_missing.csv')
#stations_pd = stations_pd[stations_pd['Data_Source'] == 'Togo']

stations_pd = stations_pd[stations_pd['samplingFeatureType'] != 0]

IDs = stations_pd['samplingFeatureCode'].tolist()
COMIDs = stations_pd['samplingFeatureType'].tolist()
Names = stations_pd['name'].tolist()

output_folder = "/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Forecast_Data/GEOGLOWS_v1"

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	file_path_1 = f"{output_folder}/2025-02-03/{comid}.csv"
	#file_path_2 = f"{output_folder}/2025-02-03/{comid}_HR.csv"

	# Check if the file exists before downloading
	#if os.path.exists(file_path_1):
	#	#print(f"File already exists: {file_path}. Skipping download.")
	#	continue  # Skip to the next station

	''''Using REST API'''
	ensembles = requests.get('https://geoglows.ecmwf.int/api/ForecastEnsembles/?reach_id={0}&date=20250204&ensemble=1-51&return_format=csv'.format(comid), verify=False).content
	#ensembles = requests.get('https://geoglows.ecmwf.int/api/ForecastEnsembles/?reach_id={0}&ensemble=1-51&return_format=csv'.format(comid), verify=False).content
	ensembles_df = pd.read_csv(io.StringIO(ensembles.decode('utf-8')), index_col=0)
	ensembles_df[ensembles_df < 0] = 0
	ensembles_df.index = pd.to_datetime(ensembles_df.index)
	ensembles_df.index = ensembles_df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
	ensembles_df.index = pd.to_datetime(ensembles_df.index)
	ensembles_df.dropna(inplace=True)
	ensembles_df.rename(columns={"ensemble_01_m^3/s": "ensemble_01", "ensemble_02_m^3/s": "ensemble_02", "ensemble_03_m^3/s": "ensemble_03", "ensemble_04_m^3/s": "ensemble_04",
								 "ensemble_05_m^3/s": "ensemble_05", "ensemble_06_m^3/s": "ensemble_06", "ensemble_07_m^3/s": "ensemble_07", "ensemble_08_m^3/s": "ensemble_08",
								 "ensemble_09_m^3/s": "ensemble_09", "ensemble_10_m^3/s": "ensemble_10", "ensemble_11_m^3/s": "ensemble_11", "ensemble_12_m^3/s": "ensemble_12",
								 "ensemble_13_m^3/s": "ensemble_13", "ensemble_14_m^3/s": "ensemble_14", "ensemble_15_m^3/s": "ensemble_15", "ensemble_16_m^3/s": "ensemble_16",
								 "ensemble_17_m^3/s": "ensemble_17",  "ensemble_18_m^3/s": "ensemble_18", "ensemble_19_m^3/s": "ensemble_19", "ensemble_20_m^3/s": "ensemble_20",
								 "ensemble_21_m^3/s": "ensemble_21", "ensemble_22_m^3/s": "ensemble_22", "ensemble_23_m^3/s": "ensemble_23", "ensemble_24_m^3/s": "ensemble_24",
								 "ensemble_25_m^3/s": "ensemble_25", "ensemble_26_m^3/s": "ensemble_26", "ensemble_27_m^3/s": "ensemble_27", "ensemble_28_m^3/s": "ensemble_28",
								 "ensemble_29_m^3/s": "ensemble_29", "ensemble_30_m^3/s": "ensemble_30", "ensemble_31_m^3/s": "ensemble_31", "ensemble_32_m^3/s": "ensemble_32",
								 "ensemble_33_m^3/s": "ensemble_33", "ensemble_34_m^3/s": "ensemble_34", "ensemble_35_m^3/s": "ensemble_35", "ensemble_36_m^3/s": "ensemble_36",
								 "ensemble_37_m^3/s": "ensemble_37", "ensemble_38_m^3/s": "ensemble_38", "ensemble_39_m^3/s": "ensemble_39", "ensemble_40_m^3/s": "ensemble_40",
								 "ensemble_41_m^3/s": "ensemble_41", "ensemble_42_m^3/s": "ensemble_42", "ensemble_43_m^3/s": "ensemble_43", "ensemble_44_m^3/s": "ensemble_44",
								 "ensemble_45_m^3/s": "ensemble_45", "ensemble_46_m^3/s": "ensemble_46", "ensemble_47_m^3/s": "ensemble_47", "ensemble_48_m^3/s": "ensemble_48",
								 "ensemble_49_m^3/s": "ensemble_49", "ensemble_50_m^3/s": "ensemble_50", "ensemble_51_m^3/s": "ensemble_51"}, inplace=True)

	high_res = requests.get('https://geoglows.ecmwf.int/api/ForecastEnsembles/?reach_id={0}&date=20250204&ensemble=52&return_format=csv'.format(comid), verify=False).content
	#high_res = requests.get('https://geoglows.ecmwf.int/api/ForecastEnsembles/?reach_id={0}&ensemble=52&return_format=csv'.format(comid), verify=False).content
	high_res_df = pd.read_csv(io.StringIO(high_res.decode('utf-8')), index_col=0)
	high_res_df.dropna(inplace=True)
	high_res_df[high_res_df < 0] = 0
	high_res_df.index = pd.to_datetime(high_res_df.index)
	high_res_df.index = high_res_df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
	high_res_df.index = pd.to_datetime(high_res_df.index)
	high_res_df.rename(columns={"ensemble_52_m^3/s": "ensemble_52"}, inplace=True)
	high_res_df.dropna(inplace=True)

	ini_date = pd.to_datetime(ensembles_df.index[0]).strftime("%Y-%m-%d")

	if not os.path.isdir("/{0}/{1}".format(output_folder, ini_date)):
		os.makedirs("/{0}/{1}".format(output_folder, ini_date))

	ensembles_df.to_csv('/{0}/{1}/{2}.csv'.format(output_folder, ini_date, comid))
	high_res_df.to_csv('/{0}/{1}/{2}_HR.csv'.format(output_folder, ini_date, comid))