import os
import requests
import statistics
from os import path
import pandas as pd
import datetime as dt
import hydrostats as hs
from io import StringIO
import hydrostats.data as hd
import hydrostats.visual as hv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from csv import writer as csv_writer

import warnings
warnings.filterwarnings("ignore")

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/Hydroweb_v2.csv')

COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()

obsFiles = []
simFiles = []
#COD = []

for name, comid in zip(Names, COMIDs):

	#print(name, ' - ', comid)

	'''max'''
	#obsFiles.append('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/max/{0}_max.csv'.format(name))
	#simFiles.append('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Corrected_Data/max/{0}-{1}_max.csv'.format(name, comid))

	'''mean'''
	#obsFiles.append('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/mean/{0}_mean.csv'.format(name))
	#simFiles.append('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Corrected_Data/mean/{0}-{1}_mean.csv'.format(name, comid))

	'''min'''
	obsFiles.append('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/min/{0}_min.csv'.format(name))
	simFiles.append('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Corrected_Data/min/{0}-{1}_min.csv'.format(name, comid))


#User Input
country = 'World'
output_dir = '/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/validationResults/'

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


for comid, name, obsFile, simFile in zip(COMIDs, Names, obsFiles, simFiles):

	print(name, ' - ', comid)

	obs_df = pd.read_csv(obsFile, index_col=0)
	obs_df.index = pd.to_datetime(obs_df.index)
	observed_df = obs_df.groupby(obs_df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)

	dates_obs = observed_df.index.tolist()

	sim_df = pd.read_csv(simFile, index_col=0)
	sim_df.index = pd.to_datetime(sim_df.index)
	simulated_df = sim_df.groupby(sim_df.index.strftime("%Y-%m-%d")).mean()
	simulated_df.index = pd.to_datetime(simulated_df.index)

	dates_sim = simulated_df.index.tolist()

	#Merging the Data
	merged_df = hd.merge_data(obs_df=observed_df, sim_df=simulated_df)
	#print(merged_df)

	'''Tables and Plots'''
	# Appending the table to the final table
	table = hs.make_table(merged_df, metrics=['ME', 'MAE', 'MAPE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'R (Pearson)', 'R (Spearman)', 'r2'], location=name, remove_neg=False, remove_zero=False)
	all_station_table = all_station_table.append(table)

	#Making plots for all the stations

	sim_array = merged_df.iloc[:, 0].values
	obs_array = merged_df.iloc[:, 1].values

	sim_mean = statistics.mean(sim_array)
	obs_mean = statistics.mean(obs_array)

	sim_std = statistics.stdev(sim_array)
	obs_std = statistics.stdev(obs_array)

	table['Bias'] = sim_mean / obs_mean
	table['Variability'] = ((sim_std / sim_mean) / (obs_std / obs_mean))
	table['COMID'] = comid

	all_station_table = all_station_table.append(table)

#Stations for the Country to an Excel Spreadsheet
#all_station_table.to_excel(path.join(table_out_dir, 'Table_of_all_stations_max.xlsx'))
#all_station_table.to_excel(path.join(table_out_dir, 'Table_of_all_stations_mean.xlsx'))
all_station_table.to_excel(path.join(table_out_dir, 'Table_of_all_stations_min.xlsx'))