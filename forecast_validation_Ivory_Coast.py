import pandas as pd
import os
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import hydrostats.data as hd
import hydrostats as hs
import hydrostats.visual as hv
import hydrostats.metrics as hm
import hydrostats.ens_metrics as em
from io import StringIO
import numpy as np

df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Stations_Ivory_Coast.csv')

IDs = df['Code'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Station'].tolist()
Rivers = df['Stream'].tolist()

initializationFiles = []
forecastFiles_1Day = []
forecastFiles_2Day = []
forecastFiles_3Day = []
forecastFiles_4Day = []
forecastFiles_5Day = []
forecastFiles_6Day = []
forecastFiles_7Day = []
forecastFiles_8Day = []
forecastFiles_9Day = []
forecastFiles_10Day = []
forecastFiles_11Day = []
forecastFiles_12Day = []
forecastFiles_13Day = []
forecastFiles_14Day = []
forecastFiles_15Day = []

for id, comid, name in zip(IDs, COMIDs, Names):

	initializationFiles.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/Initialization_Values.csv'.format(id,name))
	forecastFiles_1Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/1_Day_Forecasts.csv'.format(id,name))
	forecastFiles_2Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/2_Day_Forecasts.csv'.format(id,name))
	forecastFiles_3Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/3_Day_Forecasts.csv'.format(id,name))
	forecastFiles_4Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/4_Day_Forecasts.csv'.format(id,name))
	forecastFiles_5Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/5_Day_Forecasts.csv'.format(id,name))
	forecastFiles_6Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/6_Day_Forecasts.csv'.format(id,name))
	forecastFiles_7Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/7_Day_Forecasts.csv'.format(id,name))
	forecastFiles_8Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/8_Day_Forecasts.csv'.format(id,name))
	forecastFiles_9Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/9_Day_Forecasts.csv'.format(id,name))
	forecastFiles_10Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/10_Day_Forecasts.csv'.format(id,name))
	forecastFiles_11Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/11_Day_Forecasts.csv'.format(id,name))
	forecastFiles_12Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/12_Day_Forecasts.csv'.format(id,name))
	forecastFiles_13Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/13_Day_Forecasts.csv'.format(id,name))
	forecastFiles_14Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/14_Day_Forecasts.csv'.format(id,name))
	forecastFiles_15Day.append('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/Simulated_Data/{0}-{1}/15_Day_Forecasts.csv'.format(id,name))


# Making directories for all the Stations

# User Input
country = 'Ivory_Coast'
typeOfData = 'Observed_Values'
output_dir = '/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Ivory_Coast/Forecasts/validationResults/'

# Making directories for all the all the Desired Plots
plot_obs_hyd_dir = os.path.join(output_dir, 'Observed')
if not os.path.isdir(plot_obs_hyd_dir):
	os.makedirs(plot_obs_hyd_dir)

initialization_hyd_dir = os.path.join(output_dir, 'Initialization')
if not os.path.isdir(initialization_hyd_dir):
	os.makedirs(initialization_hyd_dir)

forecastDaysAhead = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15']

# #Initializing Variables to Append to
all_station_table = pd.DataFrame()
all_lag_table = pd.DataFrame()
station_array = []
comid_array = []
volume_list = []

for id, comid, name, rio, initializationFile, forecastFile_1Day, forecastFile_2Day, forecastFile_3Day, forecastFile_4Day, forecastFile_5Day, forecastFile_6Day, forecastFile_7Day, forecastFile_8Day, forecastFile_9Day, forecastFile_10Day, forecastFile_11Day, forecastFile_12Day, forecastFile_13Day, forecastFile_14Day, forecastFile_15Day in zip(IDs, COMIDs, Names, Rivers, initializationFiles, forecastFiles_1Day, forecastFiles_2Day, forecastFiles_3Day, forecastFiles_4Day, forecastFiles_5Day, forecastFiles_6Day, forecastFiles_7Day, forecastFiles_8Day, forecastFiles_9Day, forecastFiles_10Day, forecastFiles_11Day, forecastFiles_12Day, forecastFiles_13Day, forecastFiles_14Day, forecastFiles_15Day):
	print(id, comid, name, rio, 'Initialization')

	'''Initialization Values'''
	initialization_df = pd.read_csv(initializationFile, index_col=0)
	dates_initialization = initialization_df.index.tolist()
	dates = []
	for date in dates_initialization:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_initialization = dates

	plt.figure(1)
	plt.figure(figsize=(15, 9))
	plt.plot(dates_initialization, initialization_df.iloc[:, 0].values, 'k', color='red', label='Initialization')
	plt.title('Initial Values Hydrograph for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	plt.xlim(dates_initialization[0], dates_initialization[len(dates_initialization) - 1])
	t = pd.date_range(dates_initialization[0], dates_initialization[len(dates_initialization) - 1],
	                  periods=10).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(
		initialization_hyd_dir + '/Initial Values Hydrograph for ' + str(id) + ' - ' + name + '. COMID - ' + str(
			comid) + '.png')

	plot_out_dir = os.path.join(initialization_hyd_dir, 'Hydrographs')
	if not os.path.isdir(plot_out_dir):
		os.makedirs(plot_out_dir)

for daysAhead in forecastDaysAhead:

	for id, comid, name, rio, initializationFile, forecastFile_1Day, forecastFile_2Day, forecastFile_3Day, forecastFile_4Day, forecastFile_5Day, forecastFile_6Day, forecastFile_7Day, forecastFile_8Day, forecastFile_9Day, forecastFile_10Day, forecastFile_11Day, forecastFile_12Day, forecastFile_13Day, forecastFile_14Day, forecastFile_15Day in zip(IDs, COMIDs, Names, Rivers, initializationFiles, forecastFiles_1Day, forecastFiles_2Day, forecastFiles_3Day, forecastFiles_4Day, forecastFiles_5Day, forecastFiles_6Day, forecastFiles_7Day, forecastFiles_8Day, forecastFiles_9Day, forecastFiles_10Day, forecastFiles_11Day, forecastFiles_12Day, forecastFiles_13Day, forecastFiles_14Day, forecastFiles_15Day):

		print(id, comid, name, rio, '{}_Day_Forecast_HR'.format(daysAhead))

		dir_Forecasts = os.path.join(output_dir, '{}_Day_Forecast'.format(daysAhead))
		if not os.path.isdir(dir_Forecasts):
			os.makedirs(dir_Forecasts)

		high_res_folder = os.path.join(dir_Forecasts, 'High_Resolution')
		if not os.path.isdir(high_res_folder):
			os.makedirs(high_res_folder)

		plot_out_dir = os.path.join(high_res_folder, 'Hydrographs')
		if not os.path.isdir(plot_out_dir):
			os.makedirs(plot_out_dir)


	for id, comid, name, rio, initializationFile, forecastFile_1Day, forecastFile_2Day, forecastFile_3Day, forecastFile_4Day, forecastFile_5Day, forecastFile_6Day, forecastFile_7Day, forecastFile_8Day, forecastFile_9Day, forecastFile_10Day, forecastFile_11Day, forecastFile_12Day, forecastFile_13Day, forecastFile_14Day, forecastFile_15Day in zip(IDs, COMIDs, Names, Rivers, initializationFiles, forecastFiles_1Day, forecastFiles_2Day, forecastFiles_3Day, forecastFiles_4Day, forecastFiles_5Day, forecastFiles_6Day, forecastFiles_7Day, forecastFiles_8Day, forecastFiles_9Day, forecastFiles_10Day, forecastFiles_11Day, forecastFiles_12Day, forecastFiles_13Day, forecastFiles_14Day, forecastFiles_15Day):

		print(id, comid, name, rio, '{}_Day_Forecast'.format(daysAhead))

		'''Initialization Values'''
		initialization_df = pd.read_csv(initializationFile, index_col=0)
		initialization_df.index = pd.to_datetime(initialization_df.index)
		dates_initialization = initialization_df.index.tolist()
		dates = []
		for date in dates_initialization:
			dates.append(date.to_pydatetime())
		dates_initialization = dates

		dir_Forecasts = os.path.join(output_dir, '{}_Day_Forecast'.format(daysAhead))
		if not os.path.isdir(dir_Forecasts):
			os.makedirs(dir_Forecasts)

		emsemble_res_folder = os.path.join(dir_Forecasts, 'Ensemble')
		if not os.path.isdir(emsemble_res_folder):
			os.makedirs(emsemble_res_folder)

		emsemble_hid_folder = os.path.join(emsemble_res_folder, 'Hydrographs')
		if not os.path.isdir(emsemble_hid_folder):
			os.makedirs(emsemble_hid_folder)

		mean_simulation_hyd_dir = os.path.join(dir_Forecasts, 'Mean Simulation')
		if not os.path.isdir(mean_simulation_hyd_dir):
			os.makedirs(mean_simulation_hyd_dir)

		'''Ensemble Forecast Values'''
		forecast_df = pd.read_csv(globals()['forecastFile_{}Day'.format(daysAhead)], index_col=0)
		forecast_df.index = pd.to_datetime(forecast_df.index)
		dates_forecast = forecast_df.index.tolist()
		dates = []
		for date in dates_forecast:
			dates.append(date.to_pydatetime())
		dates_forecast = dates

		avg_series = forecast_df.mean(axis=1)
		max_series = forecast_df.max(axis=1)
		min_series = forecast_df.min(axis=1)
		std_series = forecast_df.std(axis=1)
		std_dev_upper_series = avg_series + std_series
		std_dev_lower_series = avg_series - std_series

		plt.figure(2)
		plt.figure(figsize=(15, 9))
		plt.plot(dates_forecast, avg_series, 'k', color='blue', label='Mean')
		plt.fill_between(dates_forecast, min_series, max_series, alpha=0.5, edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
		plt.title('{0} Day Forecast for {1} - {2}'.format(daysAhead, id, name) + '\n River: ' + rio + '. COMID: ' + str(comid))
		plt.xlabel('Date')
		plt.ylabel('Streamflow (m$^3$/s)')
		plt.legend()
		plt.grid()
		xmin = max(dates_initialization[0], dates_forecast[0])
		xmax = min(dates_initialization[len(dates_initialization) - 1], dates_forecast[len(dates_forecast) - 1])
		plt.xlim(xmin, xmax)
		t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
		plt.xticks(t)
		plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
		plt.tight_layout()
		plt.savefig(emsemble_res_folder + '/{0} Day Forecast for {1} - {2}.png'.format(daysAhead, id, name))

		plt.figure(3)
		plt.figure(figsize=(15, 9))
		plt.plot(dates_initialization, initialization_df.iloc[:, 0].values, 'k', color='red', label='Initialization')
		plt.plot(dates_forecast, avg_series, 'k', color='blue', label='Mean')
		plt.fill_between(dates_forecast, min_series, max_series, alpha=0.5, edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
		plt.title('{0} Day Forecast for {1} - {2}'.format(daysAhead, id, name) + '\n River: ' + rio + '. COMID: ' + str(comid))
		plt.xlabel('Date')
		plt.ylabel('Streamflow (m$^3$/s)')
		plt.legend()
		plt.grid()
		xmin = max(dates_initialization[0], dates_forecast[0])
		xmax = min(dates_initialization[len(dates_initialization) - 1], dates_forecast[len(dates_forecast) - 1])
		plt.xlim(xmin, xmax)
		t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
		plt.xticks(t)
		plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
		plt.tight_layout()
		plt.savefig(emsemble_hid_folder + '/{0} Day Forecast for {1} - {2}.png'.format(daysAhead, id, name))
		plt.close('all')

		# Comparing the Mean Simulation
		plt.figure(4)
		plt.figure(figsize=(15, 9))
		plt.plot(dates_forecast, avg_series, 'k', color='blue', label='Mean Simulation - {} Day Forecast'.format(daysAhead))
		plt.title('Mean Ensamble Forecast for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid))
		plt.xlabel('Date')
		plt.ylabel('Streamflow (m$^3$/s)')
		plt.legend()
		plt.grid()
		plt.xlim(dates_forecast[0], dates_forecast[len(dates_forecast) - 1])
		t = pd.date_range(dates_forecast[0], dates_forecast[len(dates_forecast) - 1], periods=10).to_pydatetime()
		plt.xticks(t)
		plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
		plt.tight_layout()
		plt.savefig(mean_simulation_hyd_dir + '/Observed Hydrograph for ' + str(id) + ' - ' + name + '. COMID - ' + str(comid) + '.png')

		plt.close('all')