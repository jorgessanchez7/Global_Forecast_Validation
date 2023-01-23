import os
import requests
import statistics
from os import path
import pandas as pd
import datetime as dt
from io import StringIO
import hydrostats as hs
import hydrostats.data as hd
import hydrostats.visual as hv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from csv import writer as csv_writer

import warnings
warnings.filterwarnings("ignore")

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Middle_East/Israel/Selected_Stations_Israel_Q_v0.csv')

IDs = stations_pd['statid'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Name'].tolist()

obsFiles = []
simFiles = []
#COD = []

for id, name, comid in zip(IDs, Names, COMIDs):
	obsFiles.append('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Middle_East/Israel/data/historical/Observed_Data/{}.csv'.format(id))
	simFiles.append('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Middle_East/Israel/data/historical/Corrected_Data_ag/{0}-{1}.csv'.format(id, comid))


#User Input
country = 'Israel'
output_dir = '/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Middle_East/Israel/data/historical/validationResults_Corrected_ag/'

'''Initializing Variables to Append to'''
#Creating blank dataframe for Tables
all_station_table = pd.DataFrame()
station_array = []
comid_array = []
all_lag_table = pd.DataFrame()
#Creating an empty list for volumes
volume_list = []
#Creating a table template for the lag time table
#lag_table = 'Station, COMID, Metric, Max, Max Lag Number, Min, Min LagNumber\n'

#Making directories for all the Desired Plots
table_out_dir = path.join(output_dir, 'Tables')
if not path.isdir(table_out_dir):
	os.makedirs(table_out_dir)


for id, comid, name, obsFile, simFile in zip(IDs, COMIDs, Names, obsFiles, simFiles):
	print(id, comid, name)

	obs_df = pd.read_csv(obsFile, index_col=0)
	obs_df[obs_df < 0] = 0
	obs_df.index = pd.to_datetime(obs_df.index)
	observed_df = obs_df.groupby(obs_df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)

	dates_obs = observed_df.index.tolist()

	sim_df = pd.read_csv(simFile, index_col=0)
	sim_df.index = pd.to_datetime(sim_df.index)
	sim_df = sim_df.groupby(sim_df.index.strftime("%Y-%m-%d")).mean()
	sim_df.index = pd.to_datetime(sim_df.index)

	simulated_df = sim_df['corrected streamflow_m^3/s'].to_frame()

	dates_sim = simulated_df.index.tolist()

	# Merging the Data
	merged_df = hd.merge_data(obs_df=observed_df, sim_df=simulated_df)

	'''Tables and Plots'''
	# Appending the table to the final table
	table = hs.make_table(merged_df, metrics=['ME', 'MAE', 'MAPE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'R (Pearson)', 'R (Spearman)', 'r2'], location=id, remove_neg=False, remove_zero=False)
	all_station_table = all_station_table.append(table)

	#Making plots for all the stations

	sim_array = merged_df.iloc[:, 0].values
	obs_array = merged_df.iloc[:, 1].values

	sim_mean = statistics.mean(sim_array)
	obs_mean = statistics.mean(obs_array)

	sim_std = statistics.stdev(sim_array)
	obs_std = statistics.stdev(obs_array)

	'''Calculating the Volume of the Streams'''
	sim_volume_dt = sim_array * 0.0864
	obs_volume_dt = obs_array * 0.0864

	sim_volume_cum = []
	obs_volume_cum = []
	sum_sim = 0
	sum_obs = 0

	for i in sim_volume_dt:
		sum_sim = sum_sim + i
		sim_volume_cum.append(sum_sim)

	for j in obs_volume_dt:
		sum_obs = sum_obs + j
		obs_volume_cum.append(sum_obs)

	volume_percent_diff = (max(sim_volume_cum)-max(obs_volume_cum))/max(sim_volume_cum)

	table['Bias'] = sim_mean / obs_mean
	table['Variability'] = ((sim_std / sim_mean) / (obs_std / obs_mean))
	table['Observed Volume'] = max(obs_volume_cum)
	table['Simulated Volume'] = max(sim_volume_cum)
	table['Volume Percent Difference'] = volume_percent_diff
	table['COMID'] = comid

	all_station_table = all_station_table.append(table)

	volume_list.append([id, max(obs_volume_cum), max(sim_volume_cum), volume_percent_diff])

	'''Time Lag Analysis'''
	time_lag_metrics = ['ME', 'MAE', 'MAPE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'SA', 'R (Pearson)', 'R (Spearman)', 'r2']

#Writing the lag table to excel

#table_IO = StringIO(all_lag_table)
#table_IO.seek(0)
#time_lag_df = pd.read_csv(table_IO, sep=",")

#Writing the Volume Dataframe to a csv
volume_df = pd.DataFrame(volume_list, columns=['Station', 'Observed Volume', 'Simulated Volume', 'Percent Difference'])
volume_df.to_excel(path.join(table_out_dir, 'Volume_Table.xlsx'))

#Stations for the Country to an Excel Spreadsheet
all_station_table.to_excel(path.join(table_out_dir, 'Table_of_all_stations.xlsx'))