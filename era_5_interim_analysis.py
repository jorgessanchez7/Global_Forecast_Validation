import pandas as pd
import hydrostats.data as hd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import scipy.stats as sp

regions = ['japan-geoglows', 'islands-geoglows', 'middle_east-geoglows', 'central_america-geoglows',
           'central_asia-geoglows', 'australia-geoglows', 'south_asia-geoglows', 'east_asia-geoglows',
           'europe-geoglows', 'north_america-geoglows', 'west_asia-geoglows', 'africa-geoglows',
           'south_america-geoglows']

for region in regions:

	era5_prec = pd.read_csv('/volumes/files/ECMWF_Precipitation/ERA_5/Daily_GeoTIFF_Clipped/{0}.csv'.format(region), index_col=0)
	era5_prec.index = pd.to_datetime(era5_prec.index)
	era5_prec.rename({'Precipitation (mm)': 'ERA-5 Precipitation (mm)'}, axis=1, inplace=True)

	erai_prec = pd.read_csv('/volumes/files/ECMWF_Precipitation/ERA_Interim/Daily_GeoTIFF_Clipped/{0}.csv'.format(region), index_col=0)
	erai_prec.index = pd.to_datetime(erai_prec.index)
	erai_prec.rename({'Precipitation (mm)': 'ERA-I Precipitation (mm)'}, axis=1, inplace=True)

	era5_run = pd.read_csv('/volumes/files/ECMWF_Runoff/ERA_5/Daily_GeoTIFF_Clipped/{0}.csv'.format(region), index_col=0)
	era5_run.index = pd.to_datetime(era5_run.index)
	era5_run.rename({'Runoff (mm)': 'ERA-5 Runoff (mm)'}, axis=1, inplace=True)

	erai_run = pd.read_csv('/volumes/files/ECMWF_Runoff/ERA_Interim/Daily_GeoTIFF_Clipped/{0}.csv'.format(region), index_col=0)
	erai_run.index = pd.to_datetime(erai_run.index)
	erai_run.rename({'Runoff (mm)': 'ERA-I Runoff (mm)'}, axis=1, inplace=True)

	merged_prec = hd.merge_data(sim_df=erai_prec, obs_df=era5_prec)

	plt.figure(1)
	plt.figure(figsize=(17.7983738762, 11))
	plt.plot(merged_prec.index, merged_prec.iloc[:, 0].values, 'k', color='red', label='ERA-5')
	plt.plot(merged_prec.index, merged_prec.iloc[:, 1].values, 'k', color='blue', label='ERA-Interim')
	plt.title('ERA-5 and ERA-Interim Precipitation at ' + region)
	plt.xlabel('Date')
	plt.ylabel('Precipitation (mm)')
	plt.legend()
	plt.grid()
	plt.xlim(merged_prec.index[0], merged_prec.index[len(merged_prec.index) - 1])
	#t = pd.date_range(merged_prec.index[0], merged_prec.index[len(merged_prec.index) - 1], periods=10).to_pydatetime()
	#plt.xticks(t)
	#plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig('/volumes/files/ECMWF_Precipitation/{0}.png'.format(region))

	merged_run = hd.merge_data(sim_df=erai_run, obs_df=era5_run)

	plt.figure(2)
	plt.figure(figsize=(14.5623058987, 9))
	plt.plot(merged_run.index, merged_run.iloc[:, 0].values, 'k', color='red', label='ERA-5')
	plt.plot(merged_run.index, merged_run.iloc[:, 1].values, 'k', color='blue', label='ERA-Interim')
	plt.title('ERA-5 and ERA-Interim Runoff at ' + region)
	plt.xlabel('Date')
	plt.ylabel('Runoff (mm)')
	plt.legend()
	plt.grid()
	plt.xlim(merged_run.index[0], merged_run.index[len(merged_run.index) - 1])
	#t = pd.date_range(merged_run.index[0], merged_run.index[len(merged_run.index) - 1], periods=10).to_pydatetime()
	#plt.xticks(t)
	#plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig('/volumes/files/ECMWF_Runoff/{0}.png'.format(region))

	merged_era5 = hd.merge_data(sim_df=era5_prec, obs_df=era5_run)
	min_value_era5 = min(min(merged_era5.iloc[:, 1].values), min(merged_era5.iloc[:, 0].values))
	max_value_era5 = max(max(merged_era5.iloc[:, 1].values), max(merged_era5.iloc[:, 0].values))
	slope_era5, intercept_era5, r_value_era5, p_value_era5, std_err_era5= sp.linregress(merged_era5.iloc[:, 0].values, merged_era5.iloc[:, 1].values)

	merged_erai = hd.merge_data(sim_df=erai_prec, obs_df=erai_run)
	min_value_erai = min(min(merged_erai.iloc[:, 1].values), min(merged_erai.iloc[:, 0].values))
	max_value_erai = max(max(merged_erai.iloc[:, 1].values), max(merged_erai.iloc[:, 0].values))
	slope_erai, intercept_erai, r_value_erai, p_value_erai, std_err_erai = sp.linregress(merged_erai.iloc[:, 0].values, merged_erai.iloc[:, 1].values)

	plt.figure(3)
	plt.figure(figsize=(15, 9))
	plt.plot(merged_era5.iloc[:, 0].values, merged_era5.iloc[:, 1].values, '.', color='red', label='ERA-5')
	plt.plot([min_value_era5, max_value_era5], [slope_era5 * min_value_era5 + intercept_era5, slope_era5 * max_value_era5 + intercept_era5], 'k', color='red', label='linear regression ERA-5')
	plt.plot(merged_erai.iloc[:, 0].values, merged_erai.iloc[:, 1].values, '.', color='blue', label='ERA-Interim')
	plt.plot([min_value_erai, max_value_erai], [slope_erai * min_value_erai + intercept_erai, slope_erai * max_value_erai + intercept_erai], 'k', color='blue', label='linear regression ERA-Interim')
	plt.title('Runoff vs. Precipitation at ' + region)
	plt.xlabel('Precipitation (mm)')
	plt.ylabel('Runoff (mm)')
	plt.legend()
	plt.grid()
	plt.tight_layout()
	plt.savefig('/volumes/files/ECMWF/{0}.png'.format(region))

	plt.close('all')

print ('evaluated for all the regions')