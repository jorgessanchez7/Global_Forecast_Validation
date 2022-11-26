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

df = pd.read_csv('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Stations_Selected_Colombia_RT.csv')

IDs = df['Codigo'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()


'''Get Observed Data'''

# observed_dir = '/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Real_Time_Observed'
# daily_observed_dir = '/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Daily_Real_Time_Observed'
#
# dischargeData = pd.read_csv('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Real_Time_IDEAM/CAUDAL-DHIME.csv', index_col=9)
# dischargeData.index = pd.to_datetime(dischargeData.index)
# fechas = dischargeData.index.tolist()
# estaciones = dischargeData['CodigoEstacion'].tolist()
# valores = dischargeData['Valor'].tolist()
#
# for id in IDs:
#
# 	discharge = []
# 	dates = []
#
# 	for i in range (0, len(estaciones)):
# 		if (id == estaciones[i]):
# 			discharge.append(valores[i])
# 			dates.append(fechas[i])
#
# 	pairs = [list(a) for a in zip(dates, discharge)]
# 	pd.DataFrame(pairs, columns= ['Datetime', 'oberved streamflow (m3/s)']).to_csv(observed_dir + "/{}_real_time_observed.csv".format(id), encoding='utf-8', header=True, index=0)
# 	#print("{}_real_time_observed.csv".format(id))
#
# 	data = pd.read_csv("/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Real_Time_Observed/{0}_real_time_observed.csv".format(id), index_col=0)
#
# 	data.index = pd.to_datetime(data.index)
#
# 	daily_df = data.groupby(data.index.strftime("%Y/%m/%d")).mean()
# 	daily_df.index = pd.to_datetime(daily_df.index)
#
# 	daily_df.to_csv("/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Daily_Real_Time_Observed/{0}_real_time_observed.csv".format(id), index_label="Datetime")
#
# 	print(daily_df)


obsFiles = []
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
forecastFiles_1Day_HR = []
forecastFiles_2Day_HR = []
forecastFiles_3Day_HR = []
forecastFiles_4Day_HR = []
forecastFiles_5Day_HR = []
forecastFiles_6Day_HR = []
forecastFiles_7Day_HR = []
forecastFiles_8Day_HR = []
forecastFiles_9Day_HR = []
forecastFiles_10Day_HR = []

for id in IDs:
	obsFiles.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Daily_Real_Time_Observed/' + str(
			id) + '_real_time_observed.csv')
	initializationFiles.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + 'Initialization_Values.csv')
	forecastFiles_1Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '1_Day_Forecasts.csv')
	forecastFiles_2Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '2_Day_Forecasts.csv')
	forecastFiles_3Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '3_Day_Forecasts.csv')
	forecastFiles_4Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '4_Day_Forecasts.csv')
	forecastFiles_5Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '5_Day_Forecasts.csv')
	forecastFiles_6Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '6_Day_Forecasts.csv')
	forecastFiles_7Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '7_Day_Forecasts.csv')
	forecastFiles_8Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '8_Day_Forecasts.csv')
	forecastFiles_9Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '9_Day_Forecasts.csv')
	forecastFiles_10Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '10_Day_Forecasts.csv')
	forecastFiles_11Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '11_Day_Forecasts.csv')
	forecastFiles_12Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '12_Day_Forecasts.csv')
	forecastFiles_13Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '13_Day_Forecasts.csv')
	forecastFiles_14Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '14_Day_Forecasts.csv')
	forecastFiles_15Day.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '15_Day_Forecasts.csv')
	forecastFiles_1Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '1_Day_Forecasts_High_Res.csv')
	forecastFiles_2Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '2_Day_Forecasts_High_Res.csv')
	forecastFiles_3Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '3_Day_Forecasts_High_Res.csv')
	forecastFiles_4Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '4_Day_Forecasts_High_Res.csv')
	forecastFiles_5Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '5_Day_Forecasts_High_Res.csv')
	forecastFiles_6Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '6_Day_Forecasts_High_Res.csv')
	forecastFiles_7Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '7_Day_Forecasts_High_Res.csv')
	forecastFiles_8Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '8_Day_Forecasts_High_Res.csv')
	forecastFiles_9Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '9_Day_Forecasts_High_Res.csv')
	forecastFiles_10Day_HR.append(
		'/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Forecasts/{}/'.format(
			id) + '10_Day_Forecasts_High_Res.csv')

# Making directories for all the Stations

# User Input
country = 'Colombia'
typeOfData = 'Observed_Values'
output_dir = '/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/{0}/Data/validationResults/Forecast/{1}/'.format(
	country, typeOfData)

# Making directories for all the all the Desired Plots
plot_obs_hyd_dir = os.path.join(output_dir, 'Observed')
if not os.path.isdir(plot_obs_hyd_dir):
	os.makedirs(plot_obs_hyd_dir)

initialization_hyd_dir = os.path.join(output_dir, 'Initialization')
if not os.path.isdir(initialization_hyd_dir):
	os.makedirs(initialization_hyd_dir)

forecastDaysAhead = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15']
forecastDaysAhead_HR = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']

# #Initializing Variables to Append to
all_station_table = pd.DataFrame()
all_lag_table = pd.DataFrame()
station_array = []
comid_array = []
volume_list = []

for id, comid, name, rio, obsFile, initializationFile, forecastFile_1Day, forecastFile_2Day, forecastFile_3Day, forecastFile_4Day, forecastFile_5Day, forecastFile_6Day, forecastFile_7Day, forecastFile_8Day, forecastFile_9Day, forecastFile_10Day, forecastFile_11Day, forecastFile_12Day, forecastFile_13Day, forecastFile_14Day, forecastFile_15Day, forecastFile_1Day_HR, forecastFile_2Day_HR, forecastFile_3Day_HR, forecastFile_4Day_HR, forecastFile_5Day_HR, forecastFile_6Day_HR, forecastFile_7Day_HR, forecastFile_8Day_HR, forecastFile_9Day_HR, forecastFile_10Day_HR in zip(IDs, COMIDs, Names, Rivers, obsFiles, initializationFiles, forecastFiles_1Day, forecastFiles_2Day, forecastFiles_3Day, forecastFiles_4Day, forecastFiles_5Day, forecastFiles_6Day, forecastFiles_7Day, forecastFiles_8Day, forecastFiles_9Day, forecastFiles_10Day, forecastFiles_11Day, forecastFiles_12Day, forecastFiles_13Day, forecastFiles_14Day, forecastFiles_15Day, forecastFiles_1Day_HR, forecastFiles_2Day_HR, forecastFiles_3Day_HR, forecastFiles_4Day_HR, forecastFiles_5Day_HR, forecastFiles_6Day_HR, forecastFiles_7Day_HR, forecastFiles_8Day_HR, forecastFiles_9Day_HR, forecastFiles_10Day_HR):
	print(id, comid, name, rio, 'Initialization')

	'''Observed Values'''
	obs_df = pd.read_csv(obsFile, index_col=0)
	dates_obs = obs_df.index.tolist()
	dates = []
	for date in dates_obs:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_obs = dates

	plt.figure(1)
	plt.figure(figsize=(15, 9))
	plt.plot(dates_obs, obs_df.iloc[:, 0].values, 'k', color='green', label='Observed Streamflow')
	plt.title('Observed Hydrograph for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	plt.xlim(dates_obs[0], dates_obs[len(dates_obs) - 1])
	t = pd.date_range(dates_obs[0], dates_obs[len(dates_obs) - 1], periods=10).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(
		plot_obs_hyd_dir + '/Observed Hydrograph for ' + str(id) + ' - ' + name + '. COMID - ' + str(comid) + '.png')

	'''Initialization Values'''
	initialization_df = pd.read_csv(initializationFile, index_col=0)
	dates_initialization = initialization_df.index.tolist()
	dates = []
	for date in dates_initialization:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_initialization = dates

	plt.figure(2)
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

	table_out_dir = os.path.join(initialization_hyd_dir, 'Tables')
	if not os.path.isdir(table_out_dir):
		os.makedirs(table_out_dir)

	plot_out_dir = os.path.join(initialization_hyd_dir, 'Hydrographs')
	if not os.path.isdir(plot_out_dir):
		os.makedirs(plot_out_dir)

	scatter_out_dir = os.path.join(initialization_hyd_dir, 'Scatter_Plots')
	if not os.path.isdir(scatter_out_dir):
		os.makedirs(scatter_out_dir)

	scatter_ls_out_dir = os.path.join(initialization_hyd_dir, 'Scatter_Plots-Log_Scale')
	if not os.path.isdir(scatter_ls_out_dir):
		os.makedirs(scatter_ls_out_dir)

	hist_out_dir = os.path.join(initialization_hyd_dir, 'Histograms')
	if not os.path.isdir(hist_out_dir):
		os.makedirs(hist_out_dir)

	qqplot_out_dir = os.path.join(initialization_hyd_dir, 'QQ_Plot')
	if not os.path.isdir(qqplot_out_dir):
		os.makedirs(qqplot_out_dir)
	'''
	daily_average_out_dir = os.path.join(initialization_hyd_dir, 'Daily_Averages')
	if not os.path.isdir(daily_average_out_dir):
		os.makedirs(daily_average_out_dir)

	monthly_average_out_dir = os.path.join(initialization_hyd_dir, 'Monthly_Averages')
	if not os.path.isdir(monthly_average_out_dir):
		os.makedirs(monthly_average_out_dir)
	'''
	volume_analysis_out_dir = os.path.join(initialization_hyd_dir, 'Volume_Analysis')
	if not os.path.isdir(volume_analysis_out_dir):
		os.makedirs(volume_analysis_out_dir)

	lag_out_dir = os.path.join(initialization_hyd_dir, 'Lag_Analysis')
	if not os.path.isdir(lag_out_dir):
		os.makedirs(lag_out_dir)

	# Merging the Data
	merged_df = hd.merge_data(initializationFile, obsFile)

	'''Tables and Plots'''
	# Appending the table to the final table
	table = hs.make_table(merged_df,
	                      metrics=['ME', 'MAE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)',
	                               'R (Pearson)',
	                               'R (Spearman)', 'r2'], location=id, remove_neg=False, remove_zero=False)
	all_station_table = all_station_table.append(table)

	# Making plots for all the stations

	sim_array = merged_df.iloc[:, 0].values
	obs_array = merged_df.iloc[:, 1].values

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

	volume_percent_diff = (max(obs_volume_cum) - max(sim_volume_cum)) / max(obs_volume_cum)
	volume_list.append([id, max(obs_volume_cum), max(sim_volume_cum), volume_percent_diff])

	plt.figure(3)
	plt.figure(figsize=(15, 9))
	plt.plot(merged_df.index, sim_volume_cum, 'k', color='red', label='Initialization Volume')
	plt.plot(merged_df.index, obs_volume_cum, 'k', color='green', label='Observed Volume')
	plt.title('Volume Analysis for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid))
	plt.xlabel('Date')
	plt.ylabel('Volume (Mm$^3$)')
	plt.legend()
	plt.grid()
	plt.savefig(
		volume_analysis_out_dir + '/Volume Analysis for ' + str(id) + ' - ' + name + '. COMID - ' + str(comid) + '.png')

	hv.plot(merged_df, legend=('Initialization', 'Observed'), grid=True,
	        title='Hydrograph for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['r-', 'g-'], fig_size=(15, 9))
	plt.savefig(os.path.join(plot_out_dir, '{0}_{1}_hydrographs.png'.format(str(id), name)))

	'''
	daily_avg = hd.daily_average(merged_df)
	daily_std_error = hd.daily_std_error(merged_data=merged_df)
	hv.plot(merged_data_df=daily_avg, legend=('Initialization', 'Observed'), grid=True, x_season=True,
	        title='Daily Average Streamflow (Standard Error) for ' + str(
		        id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['r-', 'g-'], fig_size=(15, 9), ebars=daily_std_error,
	        ecolor=('g', 'r'), tight_xlim=False)
	plt.savefig(os.path.join(daily_average_out_dir, '{0}_{1}_daily_average.png'.format(str(id), name)))

	hv.plot(merged_data_df=daily_avg, legend=('Initialization', 'Observed'), grid=True, x_season=True,
	        title='Daily Average Streamflow (Standard Error) for ' + str(
		        id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['r-', 'g-'], fig_size=(15, 9))
	plt.savefig(os.path.join(daily_average_out_dir, '{0}_{1}_daily_average_1.png'.format(str(id), name)))

	monthly_avg = hd.monthly_average(merged_df)
	monthly_std_error = hd.monthly_std_error(merged_data=merged_df)
	hv.plot(merged_data_df=monthly_avg, legend=('Initialization', 'Observed'), grid=True, x_season=True,
	        title='Monthly Average Streamflow (Standard Error) for ' + str(
		        id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['r-', 'g-'], fig_size=(15, 9),
	        ebars=monthly_std_error, ecolor=('g', 'r'), tight_xlim=False)
	plt.savefig(os. path.join(monthly_average_out_dir, '{0}_{1}_monthly_average.png'.format(str(id), name)))
	'''

	hv.scatter(merged_data_df=merged_df, grid=True,
	           title='Scatter Plot for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	           labels=('Initialization', 'Observed'), line45=True, best_fit=True, figsize=(15, 9))
	plt.savefig(os.path.join(scatter_out_dir, '{0}_{1}_scatter_plot.png'.format(str(id), name)))

	hv.scatter(sim_array=sim_array, obs_array=obs_array, grid=True,
	           title='Scatter Plot (Log Scale) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(
		           comid),
	           labels=('Initialization', 'Observed'), line45=True, best_fit=True, log_scale=True, figsize=(15, 9))
	plt.savefig(os.path.join(scatter_ls_out_dir, '{0}_{1}_scatter_plot-log_scale.png'.format(str(id), name)))

	hv.hist(merged_data_df=merged_df, num_bins=100, legend=('Initialization', 'Observed'), grid=True,
	        title='Histogram of Streamflows for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(
		        comid),
	        labels=('Bins', 'Frequency'), figsize=(15, 9))
	plt.savefig(os.path.join(hist_out_dir, '{0}_{1}_histograms.png'.format(str(id), name)))

	hv.qqplot(merged_data_df=merged_df, title='Quantile-Quantile Plot of Data for ' + str(
		id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), xlabel='Initialization', ylabel='Observed',
	          legend=True, figsize=(15, 9))
	plt.savefig(os.path.join(qqplot_out_dir, '{0}_{1}_qq-plot.png'.format(str(id), name)))

	'''Time Lag Analysis'''
	time_lag_metrics = ['ME', 'MAE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'SA', 'R (Pearson)',
	                    'R (Spearman)', 'r2']

	station_out_dir = os.path.join(lag_out_dir, str(id))
	if not os.path.isdir(station_out_dir):
		os.makedirs(station_out_dir)

	for metric in time_lag_metrics:
		_, time_table = hs.time_lag(merged_dataframe=merged_df, metrics=[metric], interp_freq='1D', interp_type='pchip',
		                            shift_range=(-10, 10), remove_neg=False, remove_zero=False,
		                            plot_title=metric + ' at Different Lags for ' + str(
			                            id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), plot=True,
		                            ylabel=metric + ' Values', xlabel='Number of Lagas', figsize=(15, 9),
		                            save_fig=os.path.join(station_out_dir,
		                                               '{0}_timelag_plot_for{1}_{2}.png'.format(metric, str(id), name)))
		plt.grid()
		all_lag_table = all_lag_table.append(time_table)

	for i in range(0, len (time_lag_metrics)):
		station_array.append(id)
		comid_array.append(comid)

	plt.close('all')

# # Writing the lag table to excel
all_lag_table = all_lag_table.assign(Station=station_array)
all_lag_table = all_lag_table.assign(COMID=comid_array)
all_lag_table.to_excel(os.path.join(lag_out_dir, 'Summary_of_all_Stations.xlsx'))

# Writing the Volume Dataframe to a csv
volume_df = pd.DataFrame(volume_list, columns=['Station', 'Observed Volume', 'Simulated Volume', 'Percent Difference'])
volume_df.to_excel(os.path.join(table_out_dir, 'Volume_Table.xlsx'))

# Stations for the Country to an Excel Spreadsheet
all_station_table.to_excel(os.path.join(table_out_dir, 'Table_of_all_stations.xlsx'))

for daysAhead in forecastDaysAhead_HR:

	# Initializing Variables to Append to
	all_station_table = pd.DataFrame()
	all_lag_table = pd.DataFrame()
	station_array = []
	comid_array = []
	volume_list = []

	for id, comid, name, rio, obsFile, initializationFile, forecastFile_1Day, forecastFile_2Day, forecastFile_3Day, forecastFile_4Day, forecastFile_5Day, forecastFile_6Day, forecastFile_7Day, forecastFile_8Day, forecastFile_9Day, forecastFile_10Day, forecastFile_11Day, forecastFile_12Day, forecastFile_13Day, forecastFile_14Day, forecastFile_15Day, forecastFile_1Day_HR, forecastFile_2Day_HR, forecastFile_3Day_HR, forecastFile_4Day_HR, forecastFile_5Day_HR, forecastFile_6Day_HR, forecastFile_7Day_HR, forecastFile_8Day_HR, forecastFile_9Day_HR, forecastFile_10Day_HR in zip(IDs, COMIDs, Names, Rivers, obsFiles, initializationFiles, forecastFiles_1Day, forecastFiles_2Day, forecastFiles_3Day, forecastFiles_4Day, forecastFiles_5Day, forecastFiles_6Day, forecastFiles_7Day, forecastFiles_8Day, forecastFiles_9Day, forecastFiles_10Day, forecastFiles_11Day, forecastFiles_12Day, forecastFiles_13Day, forecastFiles_14Day, forecastFiles_15Day, forecastFiles_1Day_HR, forecastFiles_2Day_HR, forecastFiles_3Day_HR, forecastFiles_4Day_HR, forecastFiles_5Day_HR, forecastFiles_6Day_HR, forecastFiles_7Day_HR, forecastFiles_8Day_HR, forecastFiles_9Day_HR, forecastFiles_10Day_HR):

		print(id, comid, name, rio, '{}_Day_Forecast_HR'.format(daysAhead))

		'''Observed Values'''
		obs_df = pd.read_csv(obsFile, index_col=0)
		dates_obs = obs_df.index.tolist()
		dates = []
		for date in dates_obs:
			dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
		dates_obs = dates

		dir_Forecasts = os.path.join(output_dir, '{}_Day_Forecast'.format(daysAhead))
		if not os.path.isdir(dir_Forecasts):
			os.makedirs(dir_Forecasts)

		high_res_folder = os.path.join(dir_Forecasts, 'High_Resolution')
		if not os.path.isdir(high_res_folder):
			os.makedirs(high_res_folder)

		'''Forecast Values (High Res)'''
		forecast_hr_df = pd.read_csv(globals()['forecastFile_{}Day_HR'.format(daysAhead)], index_col=0)
		dates_hr_forecast = forecast_hr_df.index.tolist()
		dates = []
		for date in dates_hr_forecast:
			dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
		dates_hr_forecast = dates

		plt.figure(1)
		plt.figure(figsize=(15, 9))
		plt.plot(dates_hr_forecast, forecast_hr_df.iloc[:, 0].values, 'k', color='black', label='{} Day Forecast (High Resolution)'.format(daysAhead))
		plt.title('{} Day Forecast Hydrograph for '.format(daysAhead) + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid) + '(High Resolution)')
		plt.xlabel('Date')
		plt.ylabel('Streamflow (m$^3$/s)')
		plt.legend()
		plt.grid()
		plt.xlim(dates_hr_forecast[0], dates_hr_forecast[len(dates_hr_forecast) - 1])
		t = pd.date_range(dates_hr_forecast[0], dates_hr_forecast[len(dates_hr_forecast) - 1], periods=10).to_pydatetime()
		plt.xticks(t)
		plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
		plt.tight_layout()
		plt.savefig(high_res_folder + '/{} Day Forecast Hydrograph for '.format(daysAhead) + str(id) + ' - ' + name + '. COMID - ' + str(comid) + ' - High Resolution.png')

		table_out_dir = os.path.join(high_res_folder, 'Tables')
		if not os.path.isdir(table_out_dir):
			os.makedirs(table_out_dir)

		plot_out_dir = os.path.join(high_res_folder, 'Hydrographs')
		if not os.path.isdir(plot_out_dir):
			os.makedirs(plot_out_dir)

		scatter_out_dir = os.path.join(high_res_folder, 'Scatter_Plots')
		if not os.path.isdir(scatter_out_dir):
			os.makedirs(scatter_out_dir)

		scatter_ls_out_dir = os.path.join(high_res_folder, 'Scatter_Plots-Log_Scale')
		if not os.path.isdir(scatter_ls_out_dir):
			os.makedirs(scatter_ls_out_dir)

		hist_out_dir = os.path.join(high_res_folder, 'Histograms')
		if not os.path.isdir(hist_out_dir):
			os.makedirs(hist_out_dir)

		qqplot_out_dir = os.path.join(high_res_folder, 'QQ_Plot')
		if not os.path.isdir(qqplot_out_dir):
			os.makedirs(qqplot_out_dir)
		'''
		daily_average_out_dir = os.path.join(high_res_folder, 'Daily_Averages')
		if not os.path.isdir(daily_average_out_dir):
			os.makedirs(daily_average_out_dir)

		monthly_average_out_dir = os.path.join(high_res_folder, 'Monthly_Averages')
		if not os.path.isdir(monthly_average_out_dir):
			os.makedirs(monthly_average_out_dir)
		'''
		volume_analysis_out_dir = os.path.join(high_res_folder, 'Volume_Analysis')
		if not os.path.isdir(volume_analysis_out_dir):
			os.makedirs(volume_analysis_out_dir)

		lag_out_dir = os.path.join(high_res_folder, 'Lag_Analysis')
		if not os.path.isdir(lag_out_dir):
			os.makedirs(lag_out_dir)

		# Merging the Data
		merged_df = hd.merge_data(globals()['forecastFile_{}Day_HR'.format(daysAhead)], obsFile)

		'''Tables and Plots'''
		# Appending the table to the final table
		table = hs.make_table(merged_df,
		                      metrics=['ME', 'MAE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)',
		                               'R (Pearson)', 'R (Spearman)', 'r2'], location=id, remove_neg=False,
		                      remove_zero=False)
		all_station_table = all_station_table.append(table)

		# Making plots for all the stations
		sim_array = merged_df.iloc[:, 0].values
		obs_array = merged_df.iloc[:, 1].values

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

		volume_percent_diff = (max(sim_volume_cum) - max(obs_volume_cum)) / max(sim_volume_cum)
		volume_list.append([id, max(obs_volume_cum), max(sim_volume_cum), volume_percent_diff])

		plt.figure(3)
		plt.figure(figsize=(15, 9))
		plt.plot(merged_df.index, sim_volume_cum, 'k', color='black', label='{} Day Forecast (High Resolution)'.format(daysAhead))
		plt.plot(merged_df.index, obs_volume_cum, 'k', color='green', label='Observed Volume')
		plt.title('Volume Analysis for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid))
		plt.xlabel('Date')
		plt.ylabel('Volume (Mm$^3$)')
		plt.legend()
		plt.grid()
		plt.savefig(volume_analysis_out_dir + '/Volume Analysis for ' + str(id) + ' - ' + name + '. COMID - ' + str(comid) + '.png')

		hv.plot(merged_df, legend=('{} Day Forecast (High Resolution)'.format(daysAhead), 'Observed'), grid=True, title='Hydrograph for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['k-', 'g-'], fig_size=(15, 9))
		plt.savefig(os.path.join(plot_out_dir, '{0}_{1}_hydrographs.png'.format(str(id), name)))
		'''
		daily_avg = hd.daily_average(merged_df)
		daily_std_error = hd.daily_std_error(merged_data=merged_df)
		hv.plot(merged_data_df=daily_avg, legend=('{} Day Forecast (High Resolution)'.format(daysAhead), 'Observed'), grid=True, x_season=True, title='Daily Average Streamflow (Standard Error) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['g-', 'k-'], fig_size=(15, 9), ebars=daily_std_error, ecolor=('g', 'k'), tight_xlim=False)
		plt.savefig(os.path.join(daily_average_out_dir, '{0}_{1}_daily_average.png'.format(str(id), name)))

		hv.plot(merged_data_df=daily_avg, legend=('{} Day Forecast (High Resolution)'.format(daysAhead), 'Observed'), grid=True, x_season=True, title='Daily Average Streamflow (Standard Error) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['g-', 'k-'], fig_size=(15, 9))
		plt.savefig(os.path.join(daily_average_out_dir, '{0}_{1}_daily_average_1.png'.format(str(id), name)))

		monthly_avg = hd.monthly_average(merged_df)
		monthly_std_error = hd.monthly_std_error(merged_data=merged_df)
		hv.plot(merged_data_df=monthly_avg, legend=('{} Day Forecast (High Resolution)'.format(daysAhead), 'Observed'), grid=True, x_season=True, title='Monthly Average Streamflow (Standard Error) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['g-', 'k-'], fig_size=(15, 9), ebars=monthly_std_error, ecolor=('g', 'k'), tight_xlim=False)
		plt.savefig(os.path.join(monthly_average_out_dir, '{0}_{1}_monthly_average.png'.format(str(id), name)))
		'''
		hv.scatter(merged_data_df=merged_df, grid=True, title='Scatter Plot for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=('{} Day Forecast (High Resolution)'.format(daysAhead), 'Observed'), line45=True, best_fit=True, figsize=(15, 9))
		plt.savefig(os.path.join(scatter_out_dir, '{0}_{1}_scatter_plot.png'.format(str(id), name)))

		hv.scatter(sim_array=sim_array, obs_array=obs_array, grid=True, title='Scatter Plot (Log Scale) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=('{} Day Forecast (High Resolution)'.format(daysAhead), 'Observed'), line45=True, best_fit=True, log_scale=True, figsize=(15, 9))
		plt.savefig(os.path.join(scatter_ls_out_dir, '{0}_{1}_scatter_plot-log_scale.png'.format(str(id), name)))

		hv.hist(merged_data_df=merged_df, num_bins=100, legend=('{} Day Forecast (High Resolution)'.format(daysAhead), 'Observed'), grid=True, title='Histogram of Streamflows for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=('Bins', 'Frequency'), figsize=(15, 9))
		plt.savefig(os.path.join(hist_out_dir, '{0}_{1}_histograms.png'.format(str(id), name)))

		hv.qqplot(merged_data_df=merged_df, title='Quantile-Quantile Plot of Data for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), xlabel='{} Day Forecast (High Resolution)'.format(daysAhead), ylabel='Observed', legend=True, figsize=(15, 9))
		plt.savefig(os.path.join(qqplot_out_dir, '{0}_{1}_qq-plot.png'.format(str(id), name)))

		'''Time Lag Analysis'''
		time_lag_metrics = ['ME', 'MAE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'SA', 'R (Pearson)', 'R (Spearman)', 'r2']

		station_out_dir = os.path.join(lag_out_dir, str(id))
		if not os.path.isdir(station_out_dir):
			os.makedirs(station_out_dir)

		for metric in time_lag_metrics:

			_, time_table = hs.time_lag(merged_dataframe=merged_df, metrics=[metric], interp_freq='1D', interp_type='pchip', shift_range=(-10, 10), remove_neg=False, remove_zero=False, plot_title=metric + ' at Different Lags for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), plot=True, ylabel=metric + ' Values', xlabel='Number of Lags', figsize=(15, 9), save_fig=os.path.join(station_out_dir, '{0}_timelag_plot_for{1}_{2}.png'.format(metric, str(id), name)))
			plt.grid()
			all_lag_table = all_lag_table.append(time_table)

		for i in range(0, len(time_lag_metrics)):
			station_array.append(id)
			comid_array.append(comid)

		plt.close('all')

	# Writing the lag table to excel
	all_lag_table = all_lag_table.assign(Station=station_array)
	all_lag_table = all_lag_table.assign(COMID=comid_array)
	all_lag_table.to_excel(os.path.join(lag_out_dir, 'Summary_of_all_Stations.xlsx'))

	# Writing the Volume Dataframe to a csv
	volume_df = pd.DataFrame(volume_list, columns=['Station', 'Observed Volume', 'Simulated Volume', 'Percent Difference'])
	volume_df.to_excel(os.path.join(table_out_dir, 'Volume_Table.xlsx'))

	# Stations for the Country to an Excel Spreadsheet
	all_station_table.to_excel(os.path.join(table_out_dir, 'Table_of_all_stations.xlsx'))


# #Initializing Variables to Append to
all_station_table = pd.DataFrame()
all_lag_table = pd.DataFrame()
station_array = []
comid_array = []
volume_list = []

me_table = []
me_normalised_table = []
mae_table = []
mae_normalised_table = []
rmse_table = []
rmse_normalised_table = []
crps_table = []
crps_normalised_table = []
skill_score_table_me = []
skill_score_table_mae = []
skill_score_table_rmse = []
skill_score_table_crps = []

for daysAhead in forecastDaysAhead:

	stations = []
	me = []
	me_normalised = []
	skillScore_me = []
	mae = []
	mae_normalised = []
	skillScore_mae = []
	rmse = []
	rmse_normalised = []
	skillScore_rmse = []
	crps_mean = []
	crps_normalised = []
	skillScore_crps = []

	for id, comid, name, rio, obsFile, initializationFile, forecastFile_1Day, forecastFile_2Day, forecastFile_3Day, forecastFile_4Day, forecastFile_5Day, forecastFile_6Day, forecastFile_7Day, forecastFile_8Day, forecastFile_9Day, forecastFile_10Day, forecastFile_11Day, forecastFile_12Day, forecastFile_13Day, forecastFile_14Day, forecastFile_15Day, forecastFile_1Day_HR, forecastFile_2Day_HR, forecastFile_3Day_HR, forecastFile_4Day_HR, forecastFile_5Day_HR, forecastFile_6Day_HR, forecastFile_7Day_HR, forecastFile_8Day_HR, forecastFile_9Day_HR, forecastFile_10Day_HR in zip(IDs, COMIDs, Names, Rivers, obsFiles, initializationFiles, forecastFiles_1Day, forecastFiles_2Day, forecastFiles_3Day, forecastFiles_4Day, forecastFiles_5Day, forecastFiles_6Day, forecastFiles_7Day, forecastFiles_8Day, forecastFiles_9Day, forecastFiles_10Day, forecastFiles_11Day, forecastFiles_12Day, forecastFiles_13Day, forecastFiles_14Day, forecastFiles_15Day, forecastFiles_1Day_HR, forecastFiles_2Day_HR, forecastFiles_3Day_HR, forecastFiles_4Day_HR, forecastFiles_5Day_HR, forecastFiles_6Day_HR, forecastFiles_7Day_HR, forecastFiles_8Day_HR, forecastFiles_9Day_HR, forecastFiles_10Day_HR):

		print(id, comid, name, rio, '{}_Day_Forecast'.format(daysAhead))

		stations.append(id)

		'''Observed Values'''
		obs_df = pd.read_csv(obsFile, index_col=0)
		obs_df.index = pd.to_datetime(obs_df.index)
		dates_obs = obs_df.index.tolist()
		dates = []
		for date in dates_obs:
			dates.append(date.to_pydatetime())
		dates_obs = dates

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

		if int(daysAhead) < 11:
			'''Forecast Values (High Res)'''
			forecast_hr_df = pd.read_csv(globals()['forecastFile_{}Day_HR'.format(daysAhead)], index_col=0)
			forecast_hr_df.index = pd.to_datetime(forecast_hr_df.index)
			dates_hr_forecast = forecast_hr_df.index.tolist()
			dates = []
			for date in dates_hr_forecast:
				dates.append(date.to_pydatetime())
			dates_hr_forecast = dates

		# Persistence Observed
		persistence_obs_df = obs_df.copy()
		persistence_obs_df.index += pd.DateOffset(days=int(daysAhead))
		persistence_obs_df.columns = ["Persistence_{}_day".format(daysAhead)]
		dates_persistence_obs = persistence_obs_df.index.tolist()
		dates = []
		for date in dates_persistence_obs:
			dates.append(date.to_pydatetime())
		dates_persistence_obs = dates

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

		plt.figure(1)
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

		plt.figure(2)
		plt.figure(figsize=(15, 9))
		plt.plot(dates_obs, obs_df.iloc[:, 0].values, 'g-', label='Observed Values')
		plt.plot(dates_initialization, initialization_df.iloc[:, 0].values, 'k', color='red', label='Initialization')
		plt.plot(dates_persistence_obs, persistence_obs_df.iloc[:, 0].values, ':', color='m', label='Benchmark')
		if int(daysAhead) < 11:
			plt.plot(dates_hr_forecast, forecast_hr_df.iloc[:, 0].values, 'k', color='black', label='HRES')
		plt.plot(dates_forecast, avg_series, 'k', color='blue', label='Mean')
		plt.fill_between(dates_forecast, min_series, max_series, alpha=0.5, edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
		plt.title('{0} Day Forecast for {1} - {2}'.format(daysAhead, id, name) + '\n River: ' + rio + '. COMID: ' + str(comid))
		plt.xlabel('Date')
		plt.ylabel('Streamflow (m$^3$/s)')
		plt.legend()
		plt.grid()
		xmin = max(dates_initialization[0], dates_forecast[0])
		xmin = max(xmin, dates_obs[0])
		xmax = min(dates_initialization[len(dates_initialization) - 1], dates_forecast[len(dates_forecast) - 1])
		xmax = min(xmax, dates_obs[len(dates_obs) - 1])
		plt.xlim(xmin, xmax)
		t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
		plt.xticks(t)
		plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
		plt.tight_layout()
		plt.savefig(emsemble_hid_folder + '/{0} Day Forecast for {1} - {2}.png'.format(daysAhead, id, name))
		plt.close('all')

		#Calculating the Metrics
		merged_df = pd.DataFrame.join(obs_df, [persistence_obs_df, forecast_df]).dropna()

		obs = merged_df.iloc[:, 0].values
		mean_obs = np.mean(obs)
		bench = merged_df.iloc[:, 1].values
		forecasts = merged_df.iloc[:, 2:].values
		datetime = pd.to_datetime(merged_df.index)

		mean_error = em.ens_me(obs, forecasts)
		mean_error_bench = np.mean(obs - bench)
		mean_error_s = em.skill_score(mean_error, mean_error_bench, 0)

		me.append(mean_error)
		me_normalised.append(mean_error / mean_obs)
		skillScore_me.append(mean_error_s["skillScore"])

		mean_absolute_error = em.ens_mae(obs, forecasts)
		mean_absolute_error_bench = np.mean(np.abs(obs - bench))
		mean_absolute_error_s = em.skill_score(mean_absolute_error, mean_absolute_error_bench, 0)

		mae.append(mean_absolute_error)
		mae_normalised.append(mean_absolute_error / mean_obs)
		skillScore_mae.append(mean_absolute_error_s["skillScore"])

		root_mean_squared_error = em.ens_rmse(obs, forecasts)
		root_mean_squared_error_bench = hm.rmse(bench, obs)
		root_mean_squared_error_s = em.skill_score(root_mean_squared_error, root_mean_squared_error_bench, 0)

		rmse.append(root_mean_squared_error)
		rmse_normalised.append(root_mean_squared_error / mean_obs)
		skillScore_rmse.append(root_mean_squared_error_s["skillScore"])

		crps = em.ens_crps(obs, forecasts)
		crps_bench = np.mean(np.abs(obs - bench))
		crpss = em.skill_score(crps["crpsMean"], crps_bench, 0)

		crps_mean.append(crps['crpsMean'])
		crps_normalised.append(crps['crpsMean']/mean_obs)
		skillScore_crps.append(crpss["skillScore"])

		# Comparing the Mean Simulation
		plt.figure(1)
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

		table_out_dir = os.path.join(mean_simulation_hyd_dir, 'Tables')
		if not os.path.isdir(table_out_dir):
			os.makedirs(table_out_dir)

		plot_out_dir = os.path.join(mean_simulation_hyd_dir, 'Hydrographs')
		if not os.path.isdir(plot_out_dir):
			os.makedirs(plot_out_dir)

		scatter_out_dir = os.path.join(mean_simulation_hyd_dir, 'Scatter_Plots')
		if not os.path.isdir(scatter_out_dir):
			os.makedirs(scatter_out_dir)

		scatter_ls_out_dir = os.path.join(mean_simulation_hyd_dir, 'Scatter_Plots-Log_Scale')
		if not os.path.isdir(scatter_ls_out_dir):
			os.makedirs(scatter_ls_out_dir)

		hist_out_dir = os.path.join(mean_simulation_hyd_dir, 'Histograms')
		if not os.path.isdir(hist_out_dir):
			os.makedirs(hist_out_dir)

		qqplot_out_dir = os.path.join(mean_simulation_hyd_dir, 'QQ_Plot')
		if not os.path.isdir(qqplot_out_dir):
			os.makedirs(qqplot_out_dir)

		'''
		daily_average_out_dir = os.path.join(mean_simulation_hyd_dir, 'Daily_Averages')
		if not os.path.isdir(daily_average_out_dir):
			os.makedirs(daily_average_out_dir)
		
		monthly_average_out_dir = os.path.join(high_res_folder, 'Monthly_Averages')
		if not os.path.isdir(monthly_average_out_dir):
			os.makedirs(monthly_average_out_dir)
		'''

		volume_analysis_out_dir = os.path.join(mean_simulation_hyd_dir, 'Volume_Analysis')
		if not os.path.isdir(volume_analysis_out_dir):
			os.makedirs(volume_analysis_out_dir)

		lag_out_dir = os.path.join(mean_simulation_hyd_dir, 'Lag_Analysis')
		if not os.path.isdir(lag_out_dir):
			os.makedirs(lag_out_dir)

		# Merging the Data
		avg_series=pd.DataFrame({'Datetime':avg_series.index,'simulated streamflow (m3/s)':avg_series.values}).set_index('Datetime',drop=True)

		merged_df = hd.merge_data(sim_df=avg_series, obs_df=obs_df)

		print(merged_df)

		'''Tables and Plots'''
		# Appending the table to the final table
		table = hs.make_table(merged_df, metrics=['ME', 'MAE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'R (Pearson)', 'R (Spearman)', 'r2'], location=id, remove_neg=False, remove_zero=False)
		all_station_table = all_station_table.append(table)

		# Making plots for all the stations
		sim_array = merged_df.iloc[:, 0].values
		obs_array = merged_df.iloc[:, 1].values

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

		volume_percent_diff = (max(sim_volume_cum) - max(obs_volume_cum)) / max(sim_volume_cum)
		volume_list.append([id, max(obs_volume_cum), max(sim_volume_cum), volume_percent_diff])

		plt.figure(2)
		plt.figure(figsize=(15, 9))
		plt.plot(merged_df.index, sim_volume_cum, 'k', color='blue', label='Mean Simulation - {} Day Forecast'.format(daysAhead))
		plt.plot(merged_df.index, obs_volume_cum, 'k', color='green', label='Observed Volume')
		plt.title('Volume Analysis for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid))
		plt.xlabel('Date')
		plt.ylabel('Volume (Mm$^3$)')
		plt.legend()
		plt.grid()
		plt.savefig(volume_analysis_out_dir + '/Volume Analysis for ' + str(id) + ' - ' + name + '. COMID - ' + str(comid) + '.png')

		hv.plot(merged_df, legend=('Mean Simulation - {} Day Forecast'.format(daysAhead), 'Observed'), grid=True, title='Hydrograph for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'g-'], fig_size=(15, 9))
		plt.savefig(os.path.join(plot_out_dir, '{0}_{1}_hydrographs.png'.format(str(id), name)))

		'''
		daily_avg = hd.daily_average(merged_df)
		daily_std_error = hd.daily_std_error(merged_data=merged_df)
		hv.plot(merged_data_df=daily_avg, legend=('Mean Simulation - {} Day Forecast'.format(daysAhead), 'Observed'), grid=True, x_season=True, title='Daily Average Streamflow (Standard Error) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'g-'], fig_size=(15, 9), ebars=daily_std_error, ecolor=('g', 'k'), tight_xlim=False)
		plt.savefig(os.path.join(daily_average_out_dir, '{0}_{1}_daily_average.png'.format(str(id), name)))
		
		hv.plot(merged_data_df=daily_avg, legend=('Mean Simulation - {} Day Forecast'.format(daysAhead), 'Observed'), grid=True, x_season=True, title='Daily Average Streamflow (Standard Error) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'g-'], fig_size=(15, 9))
		plt.savefig(os.path.join(daily_average_out_dir, '{0}_{1}_daily_average_1.png'.format(str(id), name)))
		
		monthly_avg = hd.monthly_average(merged_df)
		monthly_std_error = hd.monthly_std_error(merged_data=merged_df)
		hv.plot(merged_data_df=monthly_avg, legend=('Mean Simulation - {} Day Forecast'.format(daysAhead), 'Observed'), grid=True, x_season=True, title='Monthly Average Streamflow (Standard Error) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'g-'], fig_size=(15, 9), ebars=monthly_std_error, ecolor=('g', 'k'), tight_xlim=False)
		plt.savefig(os.path.join(monthly_average_out_dir, '{0}_{1}_monthly_average.png'.format(str(id), name)))
		'''

		hv.scatter(merged_data_df=merged_df, grid=True, title='Scatter Plot for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=('Mean Simulation - {} Day Forecast'.format(daysAhead), 'Observed'), line45=True, best_fit=True, figsize=(15, 9))
		plt.savefig(os.path.join(scatter_out_dir, '{0}_{1}_scatter_plot.png'.format(str(id), name)))

		hv.scatter(sim_array=sim_array, obs_array=obs_array, grid=True, title='Scatter Plot (Log Scale) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=('Mean Simulation - {} Day Forecast'.format(daysAhead), 'Observed'), line45=True, best_fit=True, log_scale=True, figsize=(15, 9))
		plt.savefig(os.path.join(scatter_ls_out_dir, '{0}_{1}_scatter_plot-log_scale.png'.format(str(id), name)))

		hv.hist(merged_data_df=merged_df, num_bins=100, legend=('Mean Simulation - {} Day Forecast'.format(daysAhead), 'Observed'), grid=True, title='Histogram of Streamflows for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), labels=('Bins', 'Frequency'), figsize=(15, 9))
		plt.savefig(os.path.join(hist_out_dir, '{0}_{1}_histograms.png'.format(str(id), name)))

		hv.qqplot(merged_data_df=merged_df, title='Quantile-Quantile Plot of Data for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), xlabel='Mean Simulation - {} Day Forecast'.format(daysAhead), ylabel='Observed', legend=True, figsize=(15, 9))
		plt.savefig(os.path.join(qqplot_out_dir, '{0}_{1}_qq-plot.png'.format(str(id), name)))

		'''Time Lag Analysis'''
		time_lag_metrics = ['ME', 'MAE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'SA', 'R (Pearson)', 'R (Spearman)', 'r2']

		station_out_dir = os.path.join(lag_out_dir, str(id))
		if not os.path.isdir(station_out_dir):
			os.makedirs(station_out_dir)

		for metric in time_lag_metrics:

			_, time_table = hs.time_lag(merged_dataframe=merged_df, metrics=[metric], interp_freq='1D', interp_type='pchip', shift_range=(-10, 10), remove_neg=False, remove_zero=False, plot_title=metric + ' at Different Lags for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), plot=True, ylabel=metric + ' Values', xlabel='Number of Lags', figsize=(15, 9), save_fig=os.path.join(station_out_dir, '{0}_timelag_plot_for{1}_{2}.png'.format(metric, str(id), name)))
			plt.grid()
			all_lag_table = all_lag_table.append(time_table)

		for i in range(0, len(time_lag_metrics)):
			station_array.append(id)
			comid_array.append(comid)

		plt.close('all')

	# Writing the lag table to excel
	all_lag_table = all_lag_table.assign(Station=station_array)
	all_lag_table = all_lag_table.assign(COMID=comid_array)
	all_lag_table.to_excel(os.path.join(lag_out_dir, 'Summary_of_all_Stations.xlsx'))

	# Writing the Volume Dataframe to a csv
	volume_df = pd.DataFrame(volume_list, columns=['Station', 'Observed Volume', 'Simulated Volume', 'Percent Difference'])
	volume_df.to_excel(os.path.join(table_out_dir, 'Volume_Table.xlsx'))

	# Stations for the Country to an Excel Spreadsheet
	all_station_table.to_excel(os.path.join(table_out_dir, 'Table_of_all_stations.xlsx'))

	if int(daysAhead) == 1:
		me_table.append(stations)
		me_table.append(me)
		mae_table.append(stations)
		mae_table.append(mae)
		rmse_table.append(stations)
		rmse_table.append(rmse)
		crps_table.append(stations)
		crps_table.append(crps_mean)
		me_normalised_table.append(stations)
		me_normalised_table.append(me_normalised)
		mae_normalised_table.append(stations)
		mae_normalised_table.append(mae_normalised)
		rmse_normalised_table.append(stations)
		rmse_normalised_table.append(rmse_normalised)
		crps_normalised_table.append(stations)
		crps_normalised_table.append(crps_normalised)
		skill_score_table_me.append(stations)
		skill_score_table_me.append(skillScore_me)
		skill_score_table_mae.append(stations)
		skill_score_table_mae.append(skillScore_mae)
		skill_score_table_rmse.append(stations)
		skill_score_table_rmse.append(skillScore_rmse)
		skill_score_table_crps.append(stations)
		skill_score_table_crps.append(skillScore_crps)
	else:
		me_table.append(me)
		mae_table.append(mae)
		rmse_table.append(rmse)
		crps_table.append(crps_mean)
		me_normalised_table.append(me_normalised)
		mae_normalised_table.append(mae_normalised)
		rmse_normalised_table.append(rmse_normalised)
		crps_normalised_table.append(crps_normalised)
		skill_score_table_me.append(skillScore_me)
		skill_score_table_mae.append(skillScore_mae)
		skill_score_table_rmse.append(skillScore_rmse)
		skill_score_table_crps.append(skillScore_crps)

me_table = pd.DataFrame({'Stations':me_table[0], 'Mean Error 1 day forecast':me_table[1], 'Mean Error 2 day forecast':me_table[2], 'Mean Error 3 day forecast':me_table[3], 'Mean Error 4 day forecast':me_table[4], 'Mean Error 5 day forecast':me_table[5], 'Mean Error 6 day forecast':me_table[6], 'Mean Error 7 day forecast':me_table[7], 'Mean Error 8 day forecast':me_table[8], 'Mean Error 9 day forecast':me_table[9], 'Mean Error 10 day forecast':me_table[10], 'Mean Error 11 day forecast':me_table[11], 'Mean Error 12 day forecast':me_table[12], 'Mean Error 13 day forecast':me_table[13], 'Mean Error 14 day forecast':me_table[14], 'Mean Error 15 day forecast':me_table[15]})
me_normalised_table = pd.DataFrame({'Stations':me_normalised_table[0], 'Mean Error (Normalised) 1 day forecast':me_normalised_table[1], 'Mean Error (Normalised) 2 day forecast':me_normalised_table[2], 'Mean Error (Normalised) 3 day forecast':me_normalised_table[3], 'Mean Error (Normalised) 4 day forecast':me_normalised_table[4], 'Mean Error (Normalised) 5 day forecast':me_normalised_table[5], 'Mean Error (Normalised) 6 day forecast':me_normalised_table[6], 'Mean Error (Normalised) 7 day forecast':me_normalised_table[7], 'Mean Error (Normalised) 8 day forecast':me_normalised_table[8], 'Mean Error (Normalised) 9 day forecast':me_normalised_table[9], 'Mean Error (Normalised) 10 day forecast':me_normalised_table[10], 'Mean Error (Normalised) 11 day forecast':me_normalised_table[11], 'Mean Error (Normalised) 12 day forecast':me_normalised_table[12], 'Mean Error (Normalised) 13 day forecast':me_normalised_table[13], 'Mean Error (Normalised) 14 day forecast':me_normalised_table[14], 'Mean Error (Normalised) 15 day forecast':me_normalised_table[15]})
skill_score_table_me = pd.DataFrame({'Stations':skill_score_table_me[0], 'Skill Score (ME) 1 day forecast':skill_score_table_me[1], 'Skill Score (ME) 2 day forecast':skill_score_table_me[2], 'Skill Score (ME) 3 day forecast':skill_score_table_me[3], 'Skill Score (ME) 4 day forecast':skill_score_table_me[4], 'Skill Score (ME) 5 day forecast':skill_score_table_me[5], 'Skill Score (ME) 6 day forecast':skill_score_table_me[6], 'Skill Score (ME) 7 day forecast':skill_score_table_me[7], 'Skill Score (ME) 8 day forecast':skill_score_table_me[8], 'Skill Score (ME) 9 day forecast':skill_score_table_me[9], 'Skill Score (ME) 10 day forecast':skill_score_table_me[10], 'Skill Score (ME) 11 day forecast':skill_score_table_me[11], 'Skill Score (ME) 12 day forecast':skill_score_table_me[12], 'Skill Score (ME) 13 day forecast':skill_score_table_me[13], 'Skill Score (ME) 14 day forecast':skill_score_table_me[14], 'Skill Score (ME) 15 day forecast':skill_score_table_me[15]})
mae_table = pd.DataFrame({'Stations':mae_table[0], 'Mean Absolute Error 1 day forecast':mae_table[1], 'Mean Absolute Error 2 day forecast':mae_table[2], 'Mean Absolute Error 3 day forecast':mae_table[3], 'Mean Absolute Error 4 day forecast':mae_table[4], 'Mean Absolute Error 5 day forecast':mae_table[5], 'Mean Absolute Error 6 day forecast':mae_table[6], 'Mean Absolute Error 7 day forecast':mae_table[7], 'Mean Absolute Error 8 day forecast':mae_table[8], 'Mean Absolute Error 9 day forecast':mae_table[9], 'Mean Absolute Error 10 day forecast':mae_table[10], 'Mean Absolute Error 11 day forecast':mae_table[11], 'Mean Absolute Error 12 day forecast':mae_table[12], 'Mean Absolute Error 13 day forecast':mae_table[13], 'Mean Absolute Error 14 day forecast':mae_table[14], 'Mean Absolute Error 15 day forecast':mae_table[15]})
mae_normalised_table = pd.DataFrame({'Stations':mae_normalised_table[0], 'Mean Absolute Error (Normalised) 1 day forecast':mae_normalised_table[1], 'Mean Absolute Error (Normalised) 2 day forecast':mae_normalised_table[2], 'Mean Absolute Error (Normalised) 3 day forecast':mae_normalised_table[3], 'Mean Absolute Error (Normalised) 4 day forecast':mae_normalised_table[4], 'Mean Absolute Error (Normalised) 5 day forecast':mae_normalised_table[5], 'Mean Absolute Error (Normalised) 6 day forecast':mae_normalised_table[6], 'Mean Absolute Error (Normalised) 7 day forecast':mae_normalised_table[7], 'Mean Absolute Error (Normalised) 8 day forecast':mae_normalised_table[8], 'Mean Absolute Error (Normalised) 9 day forecast':mae_normalised_table[9], 'Mean Absolute Error (Normalised) 10 day forecast':mae_normalised_table[10], 'Mean Absolute Error (Normalised) 11 day forecast':mae_normalised_table[11], 'Mean Absolute Error (Normalised) 12 day forecast':mae_normalised_table[12], 'Mean Absolute Error (Normalised) 13 day forecast':mae_normalised_table[13], 'Mean Absolute Error (Normalised) 14 day forecast':mae_normalised_table[14], 'Mean Absolute Error (Normalised) 15 day forecast':mae_normalised_table[15]})
skill_score_table_mae = pd.DataFrame({'Stations':skill_score_table_mae[0], 'Skill Score (MAE) 1 day forecast':skill_score_table_mae[1], 'Skill Score (MAE) 2 day forecast':skill_score_table_mae[2], 'Skill Score (MAE) 3 day forecast':skill_score_table_mae[3], 'Skill Score (MAE) 4 day forecast':skill_score_table_mae[4], 'Skill Score (MAE) 5 day forecast':skill_score_table_mae[5], 'Skill Score (MAE) 6 day forecast':skill_score_table_mae[6], 'Skill Score (MAE) 7 day forecast':skill_score_table_mae[7], 'Skill Score (MAE) 8 day forecast':skill_score_table_mae[8], 'Skill Score (MAE) 9 day forecast':skill_score_table_mae[9], 'Skill Score (MAE) 10 day forecast':skill_score_table_mae[10], 'Skill Score (MAE) 11 day forecast':skill_score_table_mae[11], 'Skill Score (MAE) 12 day forecast':skill_score_table_mae[12], 'Skill Score (MAE) 13 day forecast':skill_score_table_mae[13], 'Skill Score (MAE) 14 day forecast':skill_score_table_mae[14], 'Skill Score (MAE) 15 day forecast':skill_score_table_mae[15]})
rmse_table = pd.DataFrame({'Stations':rmse_table[0], 'RMSE 1 day forecast':rmse_table[1], 'RMSE 2 day forecast':rmse_table[2], 'RMSE 3 day forecast':rmse_table[3], 'RMSE 4 day forecast':rmse_table[4], 'RMSE 5 day forecast':rmse_table[5], 'RMSE 6 day forecast':rmse_table[6], 'RMSE 7 day forecast':rmse_table[7], 'RMSE 8 day forecast':rmse_table[8], 'RMSE 9 day forecast':rmse_table[9], 'RMSE 10 day forecast':rmse_table[10], 'RMSE 11 day forecast':rmse_table[11], 'RMSE 12 day forecast':rmse_table[12], 'RMSE 13 day forecast':rmse_table[13], 'RMSE 14 day forecast':rmse_table[14], 'RMSE 15 day forecast':rmse_table[15]})
rmse_normalised_table = pd.DataFrame({'Stations':rmse_normalised_table[0], 'RMSE (Normalised) 1 day forecast':rmse_normalised_table[1], 'RMSE (Normalised) 2 day forecast':rmse_normalised_table[2], 'RMSE (Normalised) 3 day forecast':rmse_normalised_table[3], 'RMSE (Normalised) 4 day forecast':rmse_normalised_table[4], 'RMSE (Normalised) 5 day forecast':rmse_normalised_table[5], 'RMSE (Normalised) 6 day forecast':rmse_normalised_table[6], 'RMSE (Normalised) 7 day forecast':rmse_normalised_table[7], 'RMSE (Normalised) 8 day forecast':rmse_normalised_table[8], 'RMSE (Normalised) 9 day forecast':rmse_normalised_table[9], 'RMSE (Normalised) 10 day forecast':rmse_normalised_table[10], 'RMSE (Normalised) 11 day forecast':rmse_normalised_table[11], 'RMSE (Normalised) 12 day forecast':rmse_normalised_table[12], 'RMSE (Normalised) 13 day forecast':rmse_normalised_table[13], 'RMSE (Normalised) 14 day forecast':rmse_normalised_table[14], 'RMSE (Normalised) 15 day forecast':rmse_normalised_table[15]})
skill_score_table_rmse = pd.DataFrame({'Stations':skill_score_table_rmse[0], 'Skill Score (RMSE) 1 day forecast':skill_score_table_rmse[1], 'Skill Score (RMSE) 2 day forecast':skill_score_table_rmse[2], 'Skill Score (RMSE) 3 day forecast':skill_score_table_rmse[3], 'Skill Score (RMSE) 4 day forecast':skill_score_table_rmse[4], 'Skill Score (RMSE) 5 day forecast':skill_score_table_rmse[5], 'Skill Score (RMSE) 6 day forecast':skill_score_table_rmse[6], 'Skill Score (RMSE) 7 day forecast':skill_score_table_rmse[7], 'Skill Score (RMSE) 8 day forecast':skill_score_table_rmse[8], 'Skill Score (RMSE) 9 day forecast':skill_score_table_rmse[9], 'Skill Score (RMSE) 10 day forecast':skill_score_table_rmse[10], 'Skill Score (RMSE) 11 day forecast':skill_score_table_rmse[11], 'Skill Score (RMSE) 12 day forecast':skill_score_table_rmse[12], 'Skill Score (RMSE) 13 day forecast':skill_score_table_rmse[13], 'Skill Score (RMSE) 14 day forecast':skill_score_table_rmse[14], 'Skill Score (RMSE) 15 day forecast':skill_score_table_rmse[15]})
crps_table = pd.DataFrame({'Stations':crps_table[0], 'CRPS 1 day forecast':crps_table[1], 'CRPS 2 day forecast':crps_table[2], 'CRPS 3 day forecast':crps_table[3], 'CRPS 4 day forecast':crps_table[4], 'CRPS 5 day forecast':crps_table[5], 'CRPS 6 day forecast':crps_table[6], 'CRPS 7 day forecast':crps_table[7], 'CRPS 8 day forecast':crps_table[8], 'CRPS 9 day forecast':crps_table[9], 'CRPS 10 day forecast':crps_table[10], 'CRPS 11 day forecast':crps_table[11], 'CRPS 12 day forecast':crps_table[12], 'CRPS 13 day forecast':crps_table[13], 'CRPS 14 day forecast':crps_table[14], 'CRPS 15 day forecast':crps_table[15]})
crps_normalised_table = pd.DataFrame({'Stations':crps_normalised_table[0], 'CRPS (Normalised) 1 day forecast':crps_normalised_table[1], 'CRPS (Normalised) 2 day forecast':crps_normalised_table[2], 'CRPS (Normalised) 3 day forecast':crps_normalised_table[3], 'CRPS (Normalised) 4 day forecast':crps_normalised_table[4], 'CRPS (Normalised) 5 day forecast':crps_normalised_table[5], 'CRPS (Normalised) 6 day forecast':crps_normalised_table[6], 'CRPS (Normalised) 7 day forecast':crps_normalised_table[7], 'CRPS (Normalised) 8 day forecast':crps_normalised_table[8], 'CRPS (Normalised) 9 day forecast':crps_normalised_table[9], 'CRPS (Normalised) 10 day forecast':crps_normalised_table[10], 'CRPS (Normalised) 11 day forecast':crps_normalised_table[11], 'CRPS (Normalised) 12 day forecast':crps_normalised_table[12], 'CRPS (Normalised) 13 day forecast':crps_normalised_table[13], 'CRPS (Normalised) 14 day forecast':crps_normalised_table[14], 'CRPS (Normalised) 15 day forecast':crps_normalised_table[15]})
skill_score_table_crps = pd.DataFrame({'Stations':skill_score_table_crps[0], 'Skill Score 1 (CRPS) day forecast':skill_score_table_crps[1], 'Skill Score 1 (CRPS) 2 day forecast':skill_score_table_crps[2], 'Skill Score 1 (CRPS) 3 day forecast':skill_score_table_crps[3], 'Skill Score 1 (CRPS) 4 day forecast':skill_score_table_crps[4], 'Skill Score 1 (CRPS) 5 day forecast':skill_score_table_crps[5], 'Skill Score 1 (CRPS) 6 day forecast':skill_score_table_crps[6], 'Skill Score 1 (CRPS) 7 day forecast':skill_score_table_crps[7], 'Skill Score 1 (CRPS) 8 day forecast':skill_score_table_crps[8], 'Skill Score 1 (CRPS) 9 day forecast':skill_score_table_crps[9], 'Skill Score 1 (CRPS) 10 day forecast':skill_score_table_crps[10], 'Skill Score 1 (CRPS) 11 day forecast':skill_score_table_crps[11], 'Skill Score 1 (CRPS) 12 day forecast':skill_score_table_crps[12], 'Skill Score 1 (CRPS) 13 day forecast':skill_score_table_crps[13], 'Skill Score 1 (CRPS) 14 day forecast':skill_score_table_crps[14], 'Skill Score 1 (CRPS) 15 day forecast':skill_score_table_crps[15]})

emsemble_me_folder = os.path.join(output_dir, 'ME')
if not os.path.isdir(emsemble_me_folder):
	os.makedirs(emsemble_me_folder)

emsemble_me_normalised_folder = os.path.join(output_dir, 'ME Normalised')
if not os.path.isdir(emsemble_me_normalised_folder):
	os.makedirs(emsemble_me_normalised_folder)

emsemble_skill_folder_me = os.path.join(output_dir, 'Skill Score - ME')
if not os.path.isdir(emsemble_skill_folder_me):
	os.makedirs(emsemble_skill_folder_me)

emsemble_mae_folder = os.path.join(output_dir, 'MAE')
if not os.path.isdir(emsemble_mae_folder):
	os.makedirs(emsemble_mae_folder)

emsemble_mae_normalised_folder = os.path.join(output_dir, 'MAE Normalised')
if not os.path.isdir(emsemble_mae_normalised_folder):
	os.makedirs(emsemble_mae_normalised_folder)

emsemble_skill_folder_mae = os.path.join(output_dir, 'Skill Score - MAE')
if not os.path.isdir(emsemble_skill_folder_mae):
	os.makedirs(emsemble_skill_folder_mae)

emsemble_rmse_folder = os.path.join(output_dir, 'RMSE')
if not os.path.isdir(emsemble_rmse_folder):
	os.makedirs(emsemble_rmse_folder)

emsemble_rmse_normalised_folder = os.path.join(output_dir, 'RMSE Normalised')
if not os.path.isdir(emsemble_rmse_normalised_folder):
	os.makedirs(emsemble_rmse_normalised_folder)

emsemble_skill_folder_rmse = os.path.join(output_dir, 'Skill Score - RMSE')
if not os.path.isdir(emsemble_skill_folder_rmse):
	os.makedirs(emsemble_skill_folder_rmse)

emsemble_crps_folder = os.path.join(output_dir, 'CRPS')
if not os.path.isdir(emsemble_crps_folder):
	os.makedirs(emsemble_crps_folder)

emsemble_crps_normalised_folder = os.path.join(output_dir, 'CRPS Normalised')
if not os.path.isdir(emsemble_crps_normalised_folder):
	os.makedirs(emsemble_crps_normalised_folder)

emsemble_skill_folder_crps = os.path.join(output_dir, 'Skill Score - CRPS')
if not os.path.isdir(emsemble_skill_folder_crps):
	os.makedirs(emsemble_skill_folder_crps)

# Stations for the Country to an Excel Spreadsheet
me_table.to_excel(os.path.join(emsemble_me_folder, 'ME_of_all_stations.xlsx'))
me_normalised_table.to_excel(os.path.join(emsemble_me_normalised_folder, 'ME_normalised_of_all_stations.xlsx'))
skill_score_table_me.to_excel(os.path.join(emsemble_skill_folder_me, 'Skill_Score_-ME-_of_all_stations.xlsx'))

mae_table.to_excel(os.path.join(emsemble_mae_folder, 'MAE_of_all_stations.xlsx'))
mae_normalised_table.to_excel(os.path.join(emsemble_mae_normalised_folder, 'MAE_normalised_of_all_stations.xlsx'))
skill_score_table_mae.to_excel(os.path.join(emsemble_skill_folder_mae, 'Skill_Score_-MAE-_of_all_stations.xlsx'))

rmse_table.to_excel(os.path.join(emsemble_rmse_folder, 'RMSE_of_all_stations.xlsx'))
rmse_normalised_table.to_excel(os.path.join(emsemble_rmse_normalised_folder, 'RMSE_normalised_of_all_stations.xlsx'))
skill_score_table_rmse.to_excel(os.path.join(emsemble_skill_folder_rmse, 'Skill_Score_-RMSE-_of_all_stations.xlsx'))

crps_table.to_excel(os.path.join(emsemble_crps_folder, 'CRPS_of_all_stations.xlsx'))
crps_normalised_table.to_excel(os.path.join(emsemble_crps_normalised_folder, 'CRPS_normalised_of_all_stations.xlsx'))
skill_score_table_crps.to_excel(os.path.join(emsemble_skill_folder_crps, 'Skill_Score_-CRPS-_of_all_stations.xlsx'))

days_forecast = np.linspace(1.0, 15.0, num=15)
days_forecast = days_forecast.tolist()

for i in range (0, len(IDs)):
	print(IDs[i], COMIDs[i], Names[i], Rivers[i])

	me = me_table.iloc[i]
	me = me.tolist()
	me_normalised = me_normalised_table.iloc[i]
	me_normalised = me_normalised.tolist()
	skillScore_me = skill_score_table_me.iloc[i]
	skillScore_me = skillScore_me.tolist()

	me = me[1:len(me)]
	me_normalised = me_normalised[1:len(me_normalised)]
	skillScore_me = skillScore_me[1:len(skillScore_me)]

	mae = mae_table.iloc[i]
	mae = mae.tolist()
	mae_normalised = mae_normalised_table.iloc[i]
	mae_normalised = mae_normalised.tolist()
	skillScore_mae = skill_score_table_mae.iloc[i]
	skillScore_mae = skillScore_mae.tolist()

	mae = mae[1:len(mae)]
	mae_normalised = mae_normalised[1:len(mae_normalised)]
	skillScore_mae = skillScore_mae[1:len(skillScore_mae)]

	rmse = rmse_table.iloc[i]
	rmse = rmse.tolist()
	rmse_normalised = rmse_normalised_table.iloc[i]
	rmse_normalised = rmse_normalised.tolist()
	skillScore_rmse = skill_score_table_rmse.iloc[i]
	skillScore_rmse = skillScore_rmse.tolist()

	rmse = rmse[1:len(rmse)]
	rmse_normalised = rmse_normalised[1:len(rmse_normalised)]
	skillScore_rmse = skillScore_rmse[1:len(skillScore_rmse)]

	crps_mean = crps_table.iloc[i]
	crps_mean = crps_mean.tolist()
	crps_normalised_mean = crps_normalised_table.iloc[i]
	crps_normalised_mean = crps_normalised_mean.tolist()
	skillScore_crps = skill_score_table_crps.iloc[i]
	skillScore_crps = skillScore_crps.tolist()

	crps_mean = crps_mean[1:len(crps_mean)]
	crps_normalised_mean = crps_normalised_mean[1:len(crps_normalised_mean)]
	skillScore_crps = skillScore_crps[1:len(skillScore_crps)]

	# ME
	plt.figure(1)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, me, label='ME')
	plt.title('Mean Error for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Mean Error (m$^3$/s)')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_me_folder + '/Mean Error at {0} - {1}.png'.format(IDs[i], Names[i]))

	# ME Normalised
	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, me_normalised, label='ME Normalised')
	plt.title('Normalised ME for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Normalised ME')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_me_normalised_folder + '/Normalised ME at {0} - {1}.png'.format(IDs[i], Names[i]))

	# Skill Score ME
	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, skillScore_me, label='Skill Socore (ME)')
	plt.title('Skill Socore (ME) for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Skill Socore (ME)')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_skill_folder_me + '/Skill Socore _ME_ at {0} - {1}.png'.format(IDs[i], Names[i]))

	# MAE
	plt.figure(1)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, mae, label='MAE')
	plt.title('Mean Absolute Error for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Mean Absolute Error (m$^3$/s)')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_mae_folder + '/Mean Absolute Error at {0} - {1}.png'.format(IDs[i], Names[i]))

	# MAE Normalised
	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, mae_normalised, label='MAE Normalised')
	plt.title('Normalised MAE for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Normalised MAE')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_mae_normalised_folder + '/Normalised MAE at {0} - {1}.png'.format(IDs[i], Names[i]))

	# Skill Score MAE
	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, skillScore_mae, label='Skill Socore (MAE)')
	plt.title('Skill Socore (MAE) for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Skill Socore (MAE)')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_skill_folder_mae + '/Skill Socore _MAE_ at {0} - {1}.png'.format(IDs[i], Names[i]))

	# RMSE
	plt.figure(1)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, rmse, label='RMSE')
	plt.title('RMSE for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('RMSE (m$^3$/s)')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_rmse_folder + '/RMSE at {0} - {1}.png'.format(IDs[i], Names[i]))

	# RMSE Normalised
	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, rmse_normalised, label='RMSE Normalised')
	plt.title('Normalised RMSE for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Normalised RMSE')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_rmse_normalised_folder + '/Normalised RMSE at {0} - {1}.png'.format(IDs[i], Names[i]))

	# Skill Score RMSE
	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, skillScore_crps, label='Skill Socore (RMSE)')
	plt.title('Skill Socore (RMSE) for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Skill Socore (RMSE)')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_skill_folder_rmse + '/Skill Socore _RMSE_ at {0} - {1}.png'.format(IDs[i], Names[i]))

	# CRPS
	plt.figure(1)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, crps_mean, label='CPRS')
	plt.title('Average CPRS for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('CPRS (m$^3$/s)')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_crps_folder + '/Average CPRS at {0} - {1}.png'.format(IDs[i], Names[i]))

	# CRPS Standard
	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, crps_normalised_mean, label='CPRS Normalised')
	plt.title('Normalised CPRS for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Normalised CPRS')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_crps_normalised_folder + '/Normalised CPRS at {0} - {1}.png'.format(IDs[i], Names[i]))

	# Skill Score CRPS
	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, skillScore_crps, label='Skill Socore (CRPS)')
	plt.title('Skill Score at for {0} - {1}'.format(IDs[i], Names[i]) + '\n River: ' + str(Rivers[i]) + '. COMID: ' + str(COMIDs[i]))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Skill Score')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(emsemble_skill_folder_crps + '/Skill Score at {0} - {1}.png'.format(IDs[i], Names[i]))
	plt.close('all')