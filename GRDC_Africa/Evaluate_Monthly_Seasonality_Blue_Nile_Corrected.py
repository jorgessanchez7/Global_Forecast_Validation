import pandas as pd
from os import path
import hydrostats.data as hd
import hydrostats.visual as hv
import matplotlib.pyplot as plt

df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Blue_Nile_Stations_v2.csv')

COMIDs = df['COMID'].tolist()
Names = df['Station'].tolist()
Rivers = df['Stream'].tolist()

ERA5_Files = []
ERAI_Files = []

for comid, name in zip(COMIDs, Names):
	ERA5_Files.append(
		'/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/simulated_data/ERA_5/Monthly_Corrected/'
		+ str(comid) + '_' + str(name) + '.csv')
	ERAI_Files.append(
		'/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/simulated_data/ERA_Interim/Monthly_Corrected/'
		+ str(comid) + '_' + str(name) + '.csv')

for comid, name, rio, ERA5_File, ERAI_File in zip(COMIDs, Names, Rivers, ERA5_Files, ERAI_Files):
	print(comid, name, rio)

	#Merging the Data
	merged_df = hd.merge_data(ERAI_File, ERA5_File)

	monthly_avg = hd.monthly_average(merged_df)
	monthly_std_error = hd.monthly_std_error(merged_data=merged_df)

	ERA5_monthly_avg = monthly_avg[['Observed']]
	ERA_Interim_monthly_avg = monthly_avg[['Simulated']]

	ERA5_monthly_std_error = monthly_std_error[['Observed']]
	ERA_Interim_monthly_std_error = monthly_std_error[['Simulated']]

	observed_monthly = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/observed_data/Multiannual_Mean_Streamflow/{0}_{1}.csv'.format(comid, name), dtype={'Month': str})
	observed_monthly.set_index('Month', inplace=True)

	observed_monthly_avg = observed_monthly[['Mean Streamflow (m3/s)']]
	observed_monthly_std_error = observed_monthly[['Standard Error']]

	monthly_avg_obs_ERA5 = ERA5_monthly_avg.join(observed_monthly_avg)
	monthly_std_error_ERA5 = ERA5_monthly_std_error.join(observed_monthly_std_error)

	monthly_avg_obs_ERA_Interim = ERA_Interim_monthly_avg.join(observed_monthly_avg)
	monthly_std_error_ERA_Interim = ERA_Interim_monthly_std_error.join(observed_monthly_std_error)

	hv.plot(merged_data_df=monthly_avg_obs_ERA5, legend=('ERA-5', 'Observed'), grid=True, x_season=True,
	        # title='Monthly Average Streamflow (Standard Error) for ' + str(
	        #    id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        title='Monthly Average Streamflow (Standard Error) for ' + name + '\n River: '
	              + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9),
	        ebars=monthly_std_error_ERA5, ecolor=('b', 'r'), tight_xlim=False)
	# plt.savefig(path.join(monthly_average_out_dir, '{0}_{1}_monthly_average.png'.format(str(id), name)))
	plt.savefig(path.join(
		'/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/observed_data/Multiannual_Mean_Streamflow',
		'{0}_{1}_monthly_average_ERA5_Corrected.png'.format(str(comid), name)))

	hv.plot(merged_data_df=monthly_avg_obs_ERA_Interim, legend=('ERA-5', 'Observed'), grid=True, x_season=True,
	        # title='Monthly Average Streamflow (Standard Error) for ' + str(
	        #    id) + ' - ' + name + '\n River: ' + rio + '. COMID: ' + str(comid),
	        title='Monthly Average Streamflow (Standard Error) for ' + name + '\n River: '
	              + rio + '. COMID: ' + str(comid),
	        labels=['Datetime', 'Streamflow (m$^3$/s)'], linestyles=['b-', 'r-'], fig_size=(15, 9),
	        ebars=monthly_std_error_ERA_Interim, ecolor=('b', 'r'), tight_xlim=False)
	# plt.savefig(path.join(monthly_average_out_dir, '{0}_{1}_monthly_average.png'.format(str(id), name)))
	plt.savefig(path.join(
		'/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/observed_data/Multiannual_Mean_Streamflow',
		'{0}_{1}_monthly_average_ERA-Interim_Corrected.png'.format(str(comid), name)))
