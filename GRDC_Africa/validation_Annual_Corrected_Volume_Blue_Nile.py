import pandas as pd
from os import path
import os
import hydrostats.data as hd
import hydrostats.visual as hv
import hydrostats as hs
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Blue_Nile_Stations.csv')

COMIDs = df['COMID'].tolist()
Names = df['Station'].tolist()
Rivers = df['Stream'].tolist()


obsFiles = []
simFiles = []
#COD = []



for comid, name in zip(COMIDs, Names):
	obsFiles.append(
		'/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/observed_data/Annual/'
		+ str(comid) + '_' + str(name) + '.csv')
	simFiles.append(
		'/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/simulated_data/ERA_Interim/Annual_Corrected/'
		+ str(comid) + '_' + str(name) + '.csv')
	#simFiles.append(
	#	'/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/simulated_data/ERA_5/Annual_Corrected/'
	#	+ str(comid) + '_' + str(name) + '.csv')

#User Input
catchment = 'Blue_Nile'
output_dir = '/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Annual_Corrected/validationResults_ERA-Interim/'
#output_dir = '/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Annual_Corrected/validationResults_ERA-5/'

'''Initializing Variables to Append to'''
#Creating blank dataframe for Tables
all_station_table = pd.DataFrame()

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

for comid, name, rio, obsFile, simFile in zip(COMIDs, Names, Rivers, obsFiles, simFiles):
	print(comid, name, rio)

	obs_df = pd.read_csv(obsFile, index_col=0)
	dates_obs = obs_df.index.tolist()
	dates = []
	for date in dates_obs:
		dates.append(dt.datetime.strptime(str(date), "%Y"))
	dates_obs = dates

	plt.figure(1)
	plt.figure(figsize=(15, 9))
	plt.plot(dates_obs, obs_df.iloc[:, 0].values, 'k', color='red', label='Observed Volume')
	plt.title('Observed Hydrograph for ' + name + '\n River: ' + rio + '. COMID: ' + str(comid))
	plt.xlabel('Date')
	plt.ylabel('Volume (BCM)')
	plt.legend()
	plt.grid()
	plt.xlim(dates_obs[0], dates_obs[len(dates_obs)-1])
	t = pd.date_range(dates_obs[0], dates_obs[len(dates_obs)-1], periods=10).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
	plt.tight_layout()
	plt.savefig(plot_obs_hyd_dir + '/Observed Hydrograph for ' + name + '. COMID - ' + str(comid) + '.png')

	sim_df = pd.read_csv(simFile, index_col=0)
	dates_sim = sim_df.index.tolist()
	dates=[]
	for date in dates_sim:
		dates.append(dt.datetime.strptime(str(date), "%Y"))
	dates_sim = dates

	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.plot(dates_sim, sim_df.iloc[:, 0].values, 'k', color='blue', label='ERA-Interim Volume')
	plt.title('Simulated Hydrograph for ' + name + '\n River: ' + rio + '. COMID - ' + str(comid))
	plt.xlabel('Date')
	plt.ylabel('Volume (BCM)')
	plt.legend()
	plt.grid()
	plt.xlim(dates_sim[0], dates_sim[len(dates_sim)-1])
	t = pd.date_range(dates_sim[0], dates_sim[len(dates_sim)-1], periods=10).to_pydatetime()
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
	plt.tight_layout()
	plt.savefig(plot_sim_hyd_dir + '/Simulated Hydrograph for ' + name + '. COMID - ' + str(comid) + '.png')

	obsData = pd.DataFrame({'datetime': dates_obs, 'observed volume (BCM)': obs_df.iloc[:, 0].values})
	obsData.set_index(['datetime'], inplace=True)
	simData = pd.DataFrame({'datetime': dates_sim, 'simulated volume (BCM)': sim_df.iloc[:, 0].values})
	simData.set_index(['datetime'], inplace=True)

	#Merging the Data
	merged_df = hd.merge_data(sim_df=simData, obs_df=obsData, column_names=('Simulated', 'Observed'))

	'''Tables and Plots'''
	# Appending the table to the final table
	table = hs.make_table(merged_df,
	                      metrics=['ME', 'MAE', 'MAPE', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'KGE (2012)', 'R (Pearson)',
	                               'R (Spearman)', 'r2'], location=name, remove_neg=False, remove_zero=False)
	all_station_table = all_station_table.append(table)

	#Making plots for all the stations

	sim_array = merged_df.iloc[:, 0].values
	obs_array = merged_df.iloc[:, 1].values

	hv.plot(merged_df, legend=('Simulated', 'Observed'), grid=True,
	        title='Hydrograph for ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Volume (BCM)'], linestyles=['b-', 'r-'], fig_size=(15, 9))
	plt.savefig(path.join(plot_out_dir, '{0}_{1}_hydrographs.png'.format(str(comid), name)))

	hv.scatter(merged_data_df=merged_df, grid=True,
	           title='Scatter Plot for ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	           labels=('Simulated', 'Observed'), line45=True, best_fit=True, figsize=(15, 9))
	plt.savefig(path.join(scatter_out_dir, '{0}_{1}_scatter_plot.png'.format(str(comid), name)))

	hv.scatter(sim_array=sim_array, obs_array=obs_array, grid=True,
	           title='Scatter Plot (Log Scale) for ' + name + '\n River: ' + rio + '. COMID: ' + str(
		           comid),
	           labels=('Simulated', 'Observed'), line45=True, best_fit=True, log_scale=True, figsize=(15, 9))
	plt.savefig(path.join(scatter_ls_out_dir, '{0}_{1}_scatter_plot-log_scale.png'.format(str(comid), name)))

	hv.hist(merged_data_df=merged_df, num_bins=100, legend=('Simulated', 'Observed'), grid=True,
	        title='Histogram of Volume for ' + name + '\n River: ' + rio + '. COMID: ' + str(
		        comid),
	        labels=('Bins', 'Frequency'), figsize=(15, 9))
	plt.savefig(path.join(hist_out_dir, '{0}_{1}_histograms.png'.format(str(comid), name)))

	hv.qqplot(merged_data_df=merged_df,
	          title='Quantile-Quantile Plot of Data for ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	          xlabel='Simulated', ylabel='Observed', legend=True, figsize=(15, 9))
	plt.savefig(path.join(qqplot_out_dir, '{0}_{1}_qq-plot.png'.format(str(comid), name)))

	plt.close('all')

#Writing the lag table to excel

#Stations for the Country to an Excel Spreadsheet
all_station_table.to_excel(path.join(table_out_dir, 'Table_of_all_stations.xlsx'))