import pandas as pd
import requests
from io import StringIO
from os import path
import os
from csv import writer as csv_writer
import hydrostats.data as hd
import hydrostats.visual as hv
import hydrostats as hs
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


df = pd.read_csv('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Stations_Selected_Colombia.csv')
#df = pd.read_csv('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Stations_Selected_Colombia_Test.csv')

IDs = df['Codigo'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()


'''Get Simulated Data'''

# # Define watershed parameters 
# watershed = 'South America'
# subbasin = 'Continental'
# tethys_token = '4248936a4f5a45e2f551f91ead1085f3cbc8f465'
#
# simulated_dir = '/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_Simulated'
#
#
# for comid in COMIDs:
# 	request_params = dict(watershed_name=watershed, subbasin_name=subbasin, reach_id=comid, return_format='csv')
# 	request_headers = dict(Authorization='Token ' + tethys_token)
# 	res = requests.get('http://tethys.byu.edu/apps/streamflow-prediction-tool/api/GetHistoricData/', params=request_params, headers=request_headers)
# 	csv = res.content
# 	csv = csv.decode('utf-8')
# 	csvfile = path.join("{}_historic_simulatied.csv".format(comid))
# 	data = StringIO(csv)
# 	df_data = pd.read_csv(data, sep=',', header=None, names=['predicted streamflow (m3/s)'], index_col=0, infer_datetime_format=True, skiprows=1)
# 	df_data.to_csv(path.join(simulated_dir, csvfile), sep=',', index_label='Datetime')
# 	print("{}_historic_simulatied.csv".format(comid))



'''Get Observed Data'''

# observed_dir = '/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_Observed'
#
# dischargeData = pd.read_csv('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_IDEAM/CAUD.tr5.csv', index_col=1)
# dischargeData.index = pd.to_datetime(dischargeData.index)
# fechas = dischargeData.index.tolist()
# estaciones = dischargeData['ESTACION'].tolist()
# valores = dischargeData['VALOR'].tolist()
#
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
# 	pd.DataFrame(pairs, columns= ['Datetime', 'oberved streamflow (m3/s)']).to_csv(observed_dir + "/{}_historic_observed.csv".format(id), encoding='utf-8', header=True, index=0)
# 	print("{}_historic_observed.csv".format(id))


obsFiles = []
simFiles = []
#COD = []

for id, comid in zip(IDs, COMIDs):
	obsFiles.append('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_Observed/' + str(
		id) + '_historic_observed.csv')
	simFiles.append('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_Simulated/' + str(
		comid) + '_historic_simulatied.csv')

#User Input
country = 'Colombia'
output_dir = '/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/{0}/Data/validationResults/Historical/'.format(country)

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

plot_obs_hyd_dir = path.join(output_dir, 'Observed_Hydrographs')
if not path.isdir(plot_obs_hyd_dir):
	os.makedirs(plot_obs_hyd_dir)

plot_sim_hyd_dir = path.join(output_dir, 'Simulated_Hydrographs')
if not path.isdir(plot_sim_hyd_dir):
	os.makedirs(plot_sim_hyd_dir)

plot_out_dir = path.join(output_dir, 'Hydrographs')
if not path.isdir(plot_out_dir):
	os.makedirs(plot_out_dir)

scatter_out_dir = path.join(output_dir, 'Scatter_Plots')
if not path.isdir(scatter_out_dir):
	os.makedirs(scatter_out_dir)

scatter_ls_out_dir = path.join(output_dir, 'Scatter_Plots-Log_Scale')
if not path.isdir(scatter_ls_out_dir):
	os.makedirs(scatter_ls_out_dir)

hist_out_dir = path.join(output_dir, 'Histograms')
if not path.isdir(hist_out_dir):
	os.makedirs(hist_out_dir)

qqplot_out_dir = path.join(output_dir, 'QQ_Plot')
if not path.isdir(qqplot_out_dir):
	os.makedirs(qqplot_out_dir)

daily_average_out_dir = path.join(output_dir, 'Daily_Averages')
if not path.isdir(daily_average_out_dir):
	os.makedirs(daily_average_out_dir)

monthly_average_out_dir = path.join(output_dir, 'Monthly_Averages')
if not path.isdir(monthly_average_out_dir):
	os.makedirs(monthly_average_out_dir)

volume_analysis_out_dir = path.join(output_dir, 'Volume_Analysis')
if not path.isdir(volume_analysis_out_dir):
	os.makedirs(volume_analysis_out_dir)

lag_out_dir = path.join(output_dir, 'Lag_Analysis')
if not path.isdir(lag_out_dir):
	os.makedirs(lag_out_dir)

for id, comid, name, rio, obsFile, simFile in zip(IDs, COMIDs, Names, Rivers, obsFiles, simFiles):
	print(id, comid, name, rio)

	obs_df = pd.read_csv(obsFile, index_col=0)
	dates_obs = obs_df.index.tolist()
	dates = []
	for date in dates_obs:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_obs = dates

	plt.figure(1)
	plt.figure(figsize=(15, 9))
	plt.plot(dates_obs, obs_df.iloc[:, 0].values, 'k', color='red', label='Observed Streamflow')
	plt.title('Observed Hydrograph for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	plt.xlim(dates_obs[0], dates_obs[len(dates_obs)-1])
	t = pd.date_range(dates_obs[0], dates_obs[len(dates_obs)-1], periods=10).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(
		plot_obs_hyd_dir + '/Observed Hydrograph for ' + str(id) + ' - ' + name + '. COMID - ' + str(comid) + '.png')

	sim_df = pd.read_csv(simFile, index_col=0)
	dates_sim = sim_df.index.tolist()
	dates=[]
	for date in dates_sim:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d %H:%M:%S"))
	dates_sim = dates

	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.plot(dates_sim, sim_df.iloc[:, 0].values, 'k', color='blue', label='Simulated Streamflow')
	plt.title('Simulated Hydrograph for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID - ' + str(comid))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	plt.xlim(dates_sim[0], dates_sim[len(dates_sim)-1])
	t = pd.date_range(dates_sim[0], dates_sim[len(dates_sim)-1], periods=10).to_pydatetime()
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(
		plot_sim_hyd_dir + '/Simulated Hydrograph for ' + str(id) + ' - ' + name + '. COMID - ' + str(comid) + '.png')

	#Merging the Data
	merged_df = hd.merge_data(simFile, obsFile)

	'''Tables and Plots'''
	# Appending the table to the final table
	table = hs.make_table(merged_df,
	                      metrics=['ME', 'MAE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'R (Pearson)',
	                               'R (Spearman)', 'r2'], location=id, remove_neg=False, remove_zero=False)
	all_station_table = all_station_table.append(table)

	#Making plots for all the stations

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

	volume_percent_diff = (max(sim_volume_cum)-max(obs_volume_cum))/max(sim_volume_cum)
	volume_list.append([id, max(obs_volume_cum), max(sim_volume_cum), volume_percent_diff])


	plt.figure(3)
	plt.figure(figsize=(15, 9))
	plt.plot(merged_df.index, sim_volume_cum, 'k', color='blue', label='Simulated Volume')
	plt.plot(merged_df.index, obs_volume_cum, 'k', color='red', label='Observed Volume')
	plt.title('Volume Analysis for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid))
	plt.xlabel('Date')
	plt.ylabel('Volume (Mm^3)')
	plt.legend()
	plt.grid()
	plt.savefig(
		volume_analysis_out_dir + '/Volume Analysis for ' + str(id) + ' - ' + name + '. COMID - ' + str(comid) + '.png')

	hv.plot(merged_df, legend=('Simulated', 'Observed'), grid=True,
	        title='Hydrograph for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9))
	plt.savefig(path.join(plot_out_dir, '{0}_{1}_hydrographs.png'.format(str(id), name)))

	daily_avg = hd.daily_average(merged_df)
	daily_std_error = hd.daily_std_error(merged_data=merged_df)
	hv.plot(merged_data_df=daily_avg, legend=('Simulated', 'Observed'), grid=True, x_season=True,
	        title='Daily Average Streamflow (Standard Error) for ' + str(
		        id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9), ebars=daily_std_error,
	        ecolor=('b', 'r'), tight_xlim=False)
	plt.savefig(path.join(daily_average_out_dir, '{0}_{1}_daily_average.png'.format(str(id), name)))

	hv.plot(merged_data_df=daily_avg, legend=('Simulated', 'Observed'), grid=True, x_season=True,
	        title='Daily Average Streamflow (Standard Error) for ' + str(
		        id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9))
	plt.savefig(path.join(daily_average_out_dir, '{0}_{1}_daily_average_1.png'.format(str(id), name)))

	monthly_avg = hd.monthly_average(merged_df)
	monthly_std_error = hd.monthly_std_error(merged_data=merged_df)
	hv.plot(merged_data_df=monthly_avg, legend=('Simulated', 'Observed'), grid=True, x_season=True,
	        title='Monthly Average Streamflow (Standard Error) for ' + str(
		        id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9),
	        ebars=monthly_std_error, ecolor=('b', 'r'), tight_xlim=False)
	plt.savefig(path.join(monthly_average_out_dir, '{0}_{1}_monthly_average.png'.format(str(id), name)))

	hv.scatter(merged_data_df=merged_df, grid=True,
	           title='Scatter Plot for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	           labels=('Simulated', 'Observed'), line45=True, best_fit=True, figsize=(15, 9))
	plt.savefig(path.join(scatter_out_dir, '{0}_{1}_scatter_plot.png'.format(str(id), name)))

	hv.scatter(sim_array=sim_array, obs_array=obs_array, grid=True,
	           title='Scatter Plot (Log Scale) for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(
		           comid),
	           labels=('Simulated', 'Observed'), line45=True, best_fit=True, log_scale=True, figsize=(15, 9))
	plt.savefig(path.join(scatter_ls_out_dir, '{0}_{1}_scatter_plot-log_scale.png'.format(str(id), name)))

	hv.hist(merged_data_df=merged_df, num_bins=100, legend=('Simulated', 'Observed'), grid=True,
	        title='Histogram of Streamflows for ' + str(id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(
		        comid),
	        labels=('Bins', 'Frequency'), figsize=(15, 9))
	plt.savefig(path.join(hist_out_dir, '{0}_{1}_histograms.png'.format(str(id), name)))

	hv.qqplot(merged_data_df=merged_df,
	          title='Quantile-Quantile Plot of Data for ' + str(
		          id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	          xlabel='Simulated', ylabel='Observed', legend=True, figsize=(15, 9))
	plt.savefig(path.join(qqplot_out_dir, '{0}_{1}_qq-plot.png'.format(str(id), name)))

	'''Time Lag Analysis'''
	time_lag_metrics = ['ME', 'MAE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'SA', 'R (Pearson)',
	                    'R (Spearman)', 'r2']


	station_out_dir = path.join(lag_out_dir, str(id))
	if not path.isdir(station_out_dir):
		os.makedirs(station_out_dir)

	for metric in time_lag_metrics:
		print(metric)
		_, time_table = hs.time_lag(merged_dataframe=merged_df, metrics=[metric], interp_freq='1D', interp_type='pchip',
		                            shift_range=(-10, 10), remove_neg=False, remove_zero=False,
		                            plot_title=metric + ' at Different Lags for ' + str(
			                            id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid), plot=True,
		                            ylabel=metric + ' Values', xlabel='Number of Lagas', figsize=(15, 9),
		                            save_fig=path.join(station_out_dir,
		                                               '{0}_timelag_plot_for{1}_{2}.png'.format(metric, str(id), name)))
		plt.grid()
		all_lag_table = all_lag_table.append(time_table)

	for i in range(0, len (time_lag_metrics)):
		station_array.append(id)
		comid_array.append(comid)

	plt.close('all')

#Writing the lag table to excel

#table_IO = StringIO(all_lag_table)
#table_IO.seek(0)
#time_lag_df = pd.read_csv(table_IO, sep=",")
all_lag_table = all_lag_table.assign(Station=station_array)
all_lag_table = all_lag_table.assign(COMID=comid_array)
all_lag_table.to_excel(path.join(lag_out_dir, 'Summary_of_all_Stations.xlsx'))

#Writing the Volume Dataframe to a csv
volume_df = pd.DataFrame(volume_list, columns=['Station', 'Observed Volume', 'Simulated Volume', 'Percent Difference'])
volume_df.to_excel(path.join(table_out_dir, 'Volume_Table.xlsx'))

#Stations for the Country to an Excel Spreadsheet
all_station_table.to_excel(path.join(table_out_dir, 'Table_of_all_stations.xlsx'))