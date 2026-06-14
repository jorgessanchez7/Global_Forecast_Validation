import os
import geoglows
import pandas as pd

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv(r"C:\Users\jsanchez\Downloads\profesor_Alvaro\CAMELS_COL_GAUGING.csv")
stations_pd = stations_pd[stations_pd['COMID_v2'] != 0]

IDs = stations_pd['IDEAM_CODE'].tolist()
COMIDs = stations_pd['COMID_v2'].tolist()

output_folder = "C:\\Users\\jsanchez\\Downloads\\profesor_Alvaro\\Simulated_Data\\GEOGLOWS_v2"

for id, comid in zip(IDs, COMIDs):

	print(id, ' - ', comid, ' - Q')

	file_path = f"{output_folder}\\{id}_bc.csv"

	if os.path.exists(file_path):
		print(f"File already exists: {file_path}. Skipping download.")
		continue

	#Observed Data
	df = pd.read_excel('C:\\Users\\jsanchez\\Downloads\\profesor_Alvaro\\0_COMPLETAS_SERIES_CAMELS\\{}.xlsx'.format(id), index_col=0)
	df.drop(columns=["Nivel_consistencia"], inplace=True)
	df.index = pd.to_datetime(df.index)
	observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)
	observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
	observed_df.index = pd.to_datetime(observed_df.index)

	#Simulated Data
	simulated_df = pd.read_csv('C:\\Users\\jsanchez\\Downloads\\profesor_Alvaro\\Simulated_Data\\GEOGLOWS_v2\\{}.csv'.format(id), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	#Correct Simulation
	corrected_df = geoglows.bias.correct_historical(simulated_df, observed_df)
	corrected_df.to_csv('C:\\Users\\jsanchez\\Downloads\\profesor_Alvaro\\Simulated_Data\\GEOGLOWS_v2\\{}_bc.csv'.format(id))