import pandas as pd
from os import path
import os
import hydrostats.data as hd
import hydrostats.visual as hv
import hydrostats as hs
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

regions = ['japan-geoglows', 'islands-geoglows', 'middle_east-geoglows', 'central_america-geoglows',
           'central_asia-geoglows', 'australia-geoglows', 'south_asia-geoglows', 'east_asia-geoglows',
           'europe-geoglows', 'north_america-geoglows', 'west_asia-geoglows', 'africa-geoglows',
           'south_america-geoglows']

for region in regions:

	df = pd.read_csv('/Volumes/BYU_HD/Streamflow_Prediction_Tool/Shapes/{0}-drainageline.csv'.format(region))

	COMIDs = df['COMID'].tolist()

	# User Input
	catchment = 'World'
	output_dir = '/Volumes/BYU_HD/Streamflow_Prediction_Tool/validationResults/{0}/'.format(region)

	'''Initializing Variables to Append to'''
	# Creating blank dataframe for Tables
	all_station_table = pd.DataFrame()
	comid_array = []
	all_lag_table = pd.DataFrame()
	# Creating an empty list for volumes
	volume_list = []
	# Creating a table template for the lag time table
	# lag_table = 'Station, COMID, Metric, Max, Max Lag Number, Min, Min LagNumber\n'

	# Making directories for all the Desired Plots
	table_out_dir = path.join(output_dir, 'Tables')
	if not path.isdir(table_out_dir):
		os.makedirs(table_out_dir)

	#plot_obs_hyd_dir = path.join(output_dir, 'ERA-5_Hydrographs')
	#if not path.isdir(plot_obs_hyd_dir):
	#	os.makedirs(plot_obs_hyd_dir)

	#plot_sim_hyd_dir = path.join(output_dir, 'ERA-Interim_Hydrographs')
	#if not path.isdir(plot_sim_hyd_dir):
	#	os.makedirs(plot_sim_hyd_dir)

	#plot_out_dir = path.join(output_dir, 'Hydrographs')
	#if not path.isdir(plot_out_dir):
	#	os.makedirs(plot_out_dir)

	#scatter_out_dir = path.join(output_dir, 'Scatter_Plots')
	#if not path.isdir(scatter_out_dir):
	#	os.makedirs(scatter_out_dir)

	#scatter_ls_out_dir = path.join(output_dir, 'Scatter_Plots-Log_Scale')
	#if not path.isdir(scatter_ls_out_dir):
	#	os.makedirs(scatter_ls_out_dir)

	#hist_out_dir = path.join(output_dir, 'Histograms')
	#if not path.isdir(hist_out_dir):
	#	os.makedirs(hist_out_dir)

	#qqplot_out_dir = path.join(output_dir, 'QQ_Plot')
	#if not path.isdir(qqplot_out_dir):
	#	os.makedirs(qqplot_out_dir)

	#daily_average_out_dir = path.join(output_dir, 'Daily_Averages')
	#if not path.isdir(daily_average_out_dir):
	#	os.makedirs(daily_average_out_dir)

	#monthly_average_out_dir = path.join(output_dir, 'Monthly_Averages')
	#if not path.isdir(monthly_average_out_dir):
	#	os.makedirs(monthly_average_out_dir)

	#volume_analysis_out_dir = path.join(output_dir, 'Volume_Analysis')
	#if not path.isdir(volume_analysis_out_dir):
	#	os.makedirs(volume_analysis_out_dir)

	#lag_out_dir = path.join(output_dir, 'Lag_Analysis')
	#if not path.isdir(lag_out_dir):
	#	os.makedirs(lag_out_dir)

	for comid in COMIDs:

		print (region, '-', comid)

		'''Get Era_5 Data'''
		era5_df = pd.read_csv('/Volumes/BYU_HD/Streamflow_Prediction_Tool/Time_Series/ERA_5/{0}/{1}.csv'.format(region, comid), index_col=0)
		era5_df.index = pd.to_datetime(era5_df.index)

		'''Get Era_Interim Data'''
		eraI_df = pd.read_csv('/Volumes/BYU_HD/Streamflow_Prediction_Tool/Time_Series/ERA_Interim/{0}/{1}.csv'.format(region, comid), index_col=0)
		eraI_df.index = pd.to_datetime(eraI_df.index)

		#'''Hydrostats Analysis'''

		#plt.figure(1)
		#plt.figure(figsize=(15, 9))
		#plt.plot(era5_df.index, era5_df.iloc[:, 0].values, 'k', color='red', label='ERA-5 Streamflow')
		#plt.title('ERA-5 Hydrograph for COMID: ' + str(comid))
		#plt.xlabel('Date')
		#plt.ylabel('Streamflow (m$^3$/s)')
		#plt.legend()
		#plt.grid()
		#plt.xlim(era5_df.index[0], era5_df.index[len(era5_df.index) - 1])
		#t = pd.date_range(era5_df.index[0], era5_df.index[len(era5_df.index) - 1], periods=10).to_pydatetime()
		#plt.xticks(t)
		#plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
		#plt.tight_layout()
		#plt.savefig(plot_obs_hyd_dir + '/ERA-5 Hydrograph for ' + str(comid) + '.png')

		#plt.figure(2)
		#plt.figure(figsize=(15, 9))
		#plt.plot(eraI_df.index, eraI_df.iloc[:, 0].values, 'k', color='blue', label='ERA-Interim Streamflow')
		#plt.title('ERA-Interim Hydrograph for COMID: ' + str(comid))
		#plt.xlabel('Date')
		#plt.ylabel('Streamflow (m$^3$/s)')
		#plt.legend()
		#plt.grid()
		#plt.xlim(eraI_df.index[0], eraI_df.index[len(eraI_df.index) - 1])
		#t = pd.date_range(eraI_df.index[0], eraI_df.index[len(eraI_df.index) - 1], periods=10).to_pydatetime()
		#plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
		#plt.tight_layout()
		#plt.savefig(plot_sim_hyd_dir + '/ERA-Interim Hydrograph for ' + str(comid) + '.png')

		# Merging the Data
		merged_df = hd.merge_data(sim_df=eraI_df, obs_df=era5_df)

		#'''Tables and Plots'''
		# Appending the table to the final table
		table = hs.make_table(merged_df,
		                      metrics=['ME', 'MAE', 'MAPE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)',
		                               'R (Pearson)', 'R (Spearman)', 'r2'],
		                      location=comid, remove_neg=False, remove_zero=False)
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
		volume_list.append([comid, max(obs_volume_cum), max(sim_volume_cum), volume_percent_diff])

		#plt.figure(3)
		#plt.figure(figsize=(15, 9))
		#plt.plot(merged_df.index, sim_volume_cum, 'k', color='blue', label='ERA-Interim Volume')
		#plt.plot(merged_df.index, obs_volume_cum, 'k', color='red', label='ERA-5 Volume')
		#plt.title('Volume Analysis for COMID: ' + str(comid))
		#plt.xlabel('Date')
		#plt.ylabel('Volume (Mm^3)')
		#plt.legend()
		#plt.grid()
		#plt.savefig(volume_analysis_out_dir + '/Volume Analysis for ' + str(comid) + '.png')

		#hv.plot(merged_df, legend=('ERA-Interim', 'ERA-5'), grid=True,
		#        title='Hydrograph for COMID: ' + str(comid),
		#        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9))
		#plt.savefig(path.join(plot_out_dir, '{0}_hydrographs.png'.format(str(comid))))

		#daily_avg = hd.daily_average(merged_df)
		#daily_std_error = hd.daily_std_error(merged_data=merged_df)
		#hv.plot(merged_data_df=daily_avg, legend=('ERA-Interim', 'ERA-5'), grid=True, x_season=True,
		#        title='Daily Average Streamflow (Standard Error) for COMID: ' + str(comid),
		#        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9),
		#        ebars=daily_std_error,
		#        ecolor=('b', 'r'), tight_xlim=False)
		#plt.savefig(path.join(daily_average_out_dir, '{0}_daily_average.png'.format(str(comid))))

		#hv.plot(merged_data_df=daily_avg, legend=('ERA-Interim', 'ERA-5'), grid=True, x_season=True,
		#        title='Daily Average Streamflow for COMID: ' + str(comid),
		#        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9))
		#plt.savefig(path.join(daily_average_out_dir, '{0}_daily_average_1.png'.format(str(comid))))

		#monthly_avg = hd.monthly_average(merged_df)
		#monthly_std_error = hd.monthly_std_error(merged_data=merged_df)
		#hv.plot(merged_data_df=monthly_avg, legend=('ERA-Interim', 'ERA-5'), grid=True, x_season=True,
		#        title='Monthly Average Streamflow (Standard Error) for COMID: ' + str(comid),
		#        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9),
		#        ebars=monthly_std_error, ecolor=('b', 'r'), tight_xlim=False)
		#plt.savefig(path.join(monthly_average_out_dir, '{0}_monthly_average.png'.format(str(comid))))

		#try:
		#	hv.scatter(merged_data_df=merged_df, grid=True,
		#	           title='Scatter Plot for COMID: ' + str(comid),
		#	           labels=('ERA-Interim', 'ERA-5'), line45=True, best_fit=True, figsize=(15, 9))
		#	plt.savefig(path.join(scatter_out_dir, '{0}_scatter_plot.png'.format(str(comid))))
		#except:
		#	hv.scatter(merged_data_df=merged_df, grid=True,
		#	           title='Scatter Plot for COMID: ' + str(comid),
		#	           labels=('ERA-Interim', 'ERA-5'), line45=True, best_fit=False, figsize=(15, 9))
		#	plt.savefig(path.join(scatter_out_dir, '{0}_scatter_plot.png'.format(str(comid))))


		#try:
		#	hv.scatter(sim_array=sim_array, obs_array=obs_array, grid=True,
		#	           title='Scatter Plot (Log Scale) for COMID: ' + str(comid),
		#	           labels=('ERA-Interim', 'ERA-5'), line45=True, best_fit=True, log_scale=True, figsize=(15, 9))
		#	plt.savefig(path.join(scatter_ls_out_dir, '{0}_scatter_plot-log_scale.png'.format(str(comid))))

		#except:
		#	hv.scatter(sim_array=sim_array, obs_array=obs_array, grid=True,
		#	           title='Scatter Plot (Log Scale) for COMID: ' + str(comid),
		#	           labels=('ERA-Interim', 'ERA-5'), line45=True, best_fit=False, log_scale=True, figsize=(15, 9))
		#	plt.savefig(path.join(scatter_ls_out_dir, '{0}_scatter_plot-log_scale.png'.format(str(comid))))

		#hv.hist(merged_data_df=merged_df, num_bins=100, legend=('ERA-Interim', 'ERA-5'), grid=True,
		#        title='Histogram of Streamflows for COMID: ' + str(comid),
		#        labels=('Bins', 'Frequency'), figsize=(15, 9))
		#plt.savefig(path.join(hist_out_dir, '{0}_histograms.png'.format(str(comid))))

		#hv.qqplot(merged_data_df=merged_df,
		#          title='Quantile-Quantile Plot of Data for COMID: ' + str(comid),
		#          xlabel='ERA-Interim', ylabel='ERA-5', legend=True, figsize=(15, 9))
		#plt.savefig(path.join(qqplot_out_dir, '{0}_qq-plot.png'.format(str(comid))))

		#'''Time Lag Analysis'''
		#time_lag_metrics = ['ME', 'MAE', 'MAPE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'SA',
		#                    'R (Pearson)', 'R (Spearman)', 'r2']

		## station_out_dir = path.join(lag_out_dir, str(id))
		#station_out_dir = path.join(lag_out_dir, str(comid))
		#if not path.isdir(station_out_dir):
		#	os.makedirs(station_out_dir)

		#for metric in time_lag_metrics:
		#	_, time_table = hs.time_lag(merged_dataframe=merged_df, metrics=[metric], interp_freq='1D',
		#	                            interp_type='pchip', shift_range=(-10, 10), remove_neg=False, remove_zero=False,
		#	                            plot_title=metric + ' at Different Lags for COMID: ' + str(comid), plot=True,
		#	                            ylabel=metric + ' Values', xlabel='Number of Lagas', figsize=(15, 9),
		#	                            save_fig=path.join(station_out_dir,
		#	                                               '{0}_timelag_plot_for{1}.png'.format(metric, str(comid))))
		#	plt.grid()
		#	all_lag_table = all_lag_table.append(time_table)

		#for i in range(0, len(time_lag_metrics)):
		#	comid_array.append(comid)

		#plt.close('all')

	# Writing the lag table to excel
	#all_lag_table = all_lag_table.assign(COMID=comid_array)
	#all_lag_table.to_excel(path.join(lag_out_dir, 'Summary_of_all_Stations.xlsx'))

	# Writing the Volume Dataframe to a csv
	volume_df = pd.DataFrame(volume_list,
	                         columns=['Location', 'ERA-5 Volume', 'ERA-Interim Volume', 'Percent Difference'])
	volume_df.to_csv(path.join(table_out_dir, 'Volume_Table.csv'))
	#volume_df.to_excel(path.join(table_out_dir, 'Volume_Table.xlsx'))

	# Stations for the Country to an Excel Spreadsheet
	all_station_table.to_csv(path.join(table_out_dir, 'Table_of_all_stations.csv'))
	#all_station_table.to_excel(path.join(table_out_dir, 'Table_of_all_stations.xlsx'))

print ('evaluated for all the regions')