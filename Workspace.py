import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import hydrostats.data as hd

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

#df = pd.read_csv(r'/Users/student/Dropbox/PhD/2019 Winter/Dissertation_v5/Bangladesh/Bangladesh_Selected_Stations.csv')
# #df = pd.read_csv(r'/Volumes/FILES/global_streamflow_backup/reforecasts/South_America_csv/FEWS_Stations_Q.csv')
df = pd.read_csv(r'/Volumes/FILES/global_streamflow_backup/reforecasts/South_America_csv/fews_stations.csv')

#spt_id = df['comid'].tolist()
spt_id = df['COMID'].tolist()
#rivers = df['River'].tolist()
rivers = df['stream'].tolist()
#stations = df['Station'].tolist()
stations = df['name'].tolist()
ids = df['id'].tolist()

for id, station, river, spt in zip(ids, stations, rivers, spt_id):
	#output_path = "/Users/student/Desktop/South_Asia2017_csv/{}".format(spt)
	output_path = "/Users/student/Desktop/output/South_America_csv/{}".format(id)
	print(id, station, river, spt)

	#Observed
	''''
	df = pd.read_csv(output_path + '/{0}.csv'.format(id))
	#dates_observed = df['Date'].tolist()
	dates_observed = df['Datetime'].tolist()
	dates = []
	for date in dates_observed:
		month = int(date[0:2])
		day = int(date[3:5])
		year = int(date[6:8]) + 2000
		dates.append(dt.datetime(year,month,day))
		dates_observed = dates
	#values_observed = df['Discharge m3/s'].tolist()
	values_observed = df['discharge (m3/s)'].tolist()
	'''

	#Water Balance
	df = pd.read_csv(output_path + '/Initialization_Values.csv')
	dates_water_balance = df['Date'].tolist()
	dates = []
	for date in dates_water_balance:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_water_balance = dates
	values_water_balance = df['Initialization (m^3/s)'].tolist()

	#Benchmark
	dates_benchmark_1DayForecast = dates_water_balance[1:]
	values_benchmark_1DayForecast = values_water_balance[0:len(values_water_balance)-1]

	dates_benchmark_2DayForecast = dates_water_balance[2:]
	values_benchmark_2DayForecast = values_water_balance[0:len(values_water_balance) - 2]

	dates_benchmark_3DayForecast = dates_water_balance[3:]
	values_benchmark_3DayForecast = values_water_balance[0:len(values_water_balance) - 3]

	dates_benchmark_4DayForecast = dates_water_balance[4:]
	values_benchmark_4DayForecast = values_water_balance[0:len(values_water_balance) - 4]

	dates_benchmark_5DayForecast = dates_water_balance[5:]
	values_benchmark_5DayForecast = values_water_balance[0:len(values_water_balance) - 5]

	dates_benchmark_6DayForecast = dates_water_balance[6:]
	values_benchmark_6DayForecast = values_water_balance[0:len(values_water_balance) - 6]

	dates_benchmark_7DayForecast = dates_water_balance[7:]
	values_benchmark_7DayForecast = values_water_balance[0:len(values_water_balance) - 7]

	dates_benchmark_8DayForecast = dates_water_balance[8:]
	values_benchmark_8DayForecast = values_water_balance[0:len(values_water_balance) - 8]

	dates_benchmark_9DayForecast = dates_water_balance[9:]
	values_benchmark_9DayForecast = values_water_balance[0:len(values_water_balance) - 9]

	dates_benchmark_10DayForecast = dates_water_balance[10:]
	values_benchmark_10DayForecast = values_water_balance[0:len(values_water_balance) - 10]

	dates_benchmark_11DayForecast = dates_water_balance[11:]
	values_benchmark_11DayForecast = values_water_balance[0:len(values_water_balance) - 11]

	dates_benchmark_12DayForecast = dates_water_balance[12:]
	values_benchmark_12DayForecast = values_water_balance[0:len(values_water_balance) - 12]

	dates_benchmark_13DayForecast = dates_water_balance[13:]
	values_benchmark_13DayForecast = values_water_balance[0:len(values_water_balance) - 13]

	dates_benchmark_14DayForecast = dates_water_balance[14:]
	values_benchmark_14DayForecast = values_water_balance[0:len(values_water_balance) - 14]

	dates_benchmark_15DayForecast = dates_water_balance[15:]
	values_benchmark_15DayForecast = values_water_balance[0:len(values_water_balance) - 15]


	#1 day Forecast
	df = pd.read_csv(output_path + '/1_Day_Forecasts.csv')
	dates_1DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_1DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_1DayForecast = dates
	ensemble_1DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/1_Day_Forecasts_High_Res.csv')
	dates_1DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_1DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_1DayForecast_hr = dates
	high_res_1DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_1DayForecast = ensemble_1DayForecast.mean(axis=1)
	max_series_1DayForecast = ensemble_1DayForecast.max(axis=1)
	min_series_1DayForecast = ensemble_1DayForecast.min(axis=1)
	std_series_1DayForecast = ensemble_1DayForecast.std(axis=1)
	std_dev_upper_series_1DayForecast = avg_series_1DayForecast + std_series_1DayForecast
	std_dev_lower_series_1DayForecast = avg_series_1DayForecast - std_series_1DayForecast

	plt.figure(1)
	plt.figure(figsize=(15, 9))
	#plt.plot(dates_observed, values_observed, 'go', label='Observed Values')
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color = 'red', label='Water Balance')
	plt.plot(dates_benchmark_1DayForecast, values_benchmark_1DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_1DayForecast_hr, high_res_1DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_1DayForecast, avg_series_1DayForecast, 'k', color = 'blue', label='Mean')
	#plt.fill_between(dates_1DayForecast, min_series_1DayForecast, max_series_1DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_1DayForecast, min_series_1DayForecast, max_series_1DayForecast, alpha=0.5, edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	#plt.fill_between(dates_1DayForecast, std_dev_lower_series_1DayForecast, std_dev_upper_series_1DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('1 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_1DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance)-1], dates_1DayForecast[len(dates_1DayForecast)-1])
	xmax2 = max(dates_water_balance[len(dates_water_balance)-1], dates_1DayForecast[len(dates_1DayForecast)-1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/1 Day Forecast for {0} - {1}.png'.format(station, river))

	#2 day Forecast
	df = pd.read_csv(output_path + '/2_Day_Forecasts.csv')
	dates_2DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_2DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_2DayForecast = dates
	ensemble_2DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/2_Day_Forecasts_High_Res.csv')
	dates_2DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_2DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_2DayForecast_hr = dates
	high_res_2DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_2DayForecast = ensemble_2DayForecast.mean(axis=1)
	max_series_2DayForecast = ensemble_2DayForecast.max(axis=1)
	min_series_2DayForecast = ensemble_2DayForecast.min(axis=1)
	std_series_2DayForecast = ensemble_2DayForecast.std(axis=1)
	std_dev_upper_series_2DayForecast = avg_series_2DayForecast + std_series_2DayForecast
	std_dev_lower_series_2DayForecast = avg_series_2DayForecast - std_series_2DayForecast

	plt.figure(2)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_2DayForecast, values_benchmark_2DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_2DayForecast_hr, high_res_2DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_2DayForecast, avg_series_2DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_2DayForecast, min_series_2DayForecast, max_series_2DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_2DayForecast, min_series_2DayForecast, max_series_2DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_2DayForecast, std_dev_lower_series_2DayForecast, std_dev_upper_series_2DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('2 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_2DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_2DayForecast[len(dates_2DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_2DayForecast[len(dates_2DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/2 Day Forecast for {0} - {1}.png'.format(station, river))

	#3 day Forecast
	df = pd.read_csv(output_path + '/3_Day_Forecasts.csv')
	dates_3DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_3DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_3DayForecast = dates
	ensemble_3DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/2_Day_Forecasts_High_Res.csv')
	dates_3DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_3DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_3DayForecast_hr = dates
	high_res_3DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_3DayForecast = ensemble_3DayForecast.mean(axis=1)
	max_series_3DayForecast = ensemble_3DayForecast.max(axis=1)
	min_series_3DayForecast = ensemble_3DayForecast.min(axis=1)
	std_series_3DayForecast = ensemble_3DayForecast.std(axis=1)
	std_dev_upper_series_3DayForecast = avg_series_3DayForecast + std_series_3DayForecast
	std_dev_lower_series_3DayForecast = avg_series_3DayForecast - std_series_3DayForecast

	plt.figure(3)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_3DayForecast, values_benchmark_3DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_3DayForecast_hr, high_res_3DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_3DayForecast, avg_series_3DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_3DayForecast, min_series_3DayForecast, max_series_3DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_3DayForecast, min_series_3DayForecast, max_series_3DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_3DayForecast, std_dev_lower_series_3DayForecast, std_dev_upper_series_3DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('3 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_3DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_3DayForecast[len(dates_3DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_3DayForecast[len(dates_3DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/3 Day Forecast for {0} - {1}.png'.format(station, river))

	#4 day Forecast
	df = pd.read_csv(output_path + '/4_Day_Forecasts.csv')
	dates_4DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_4DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_4DayForecast = dates
	ensemble_4DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/4_Day_Forecasts_High_Res.csv')
	dates_4DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_4DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_4DayForecast_hr = dates
	high_res_4DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_4DayForecast = ensemble_4DayForecast.mean(axis=1)
	max_series_4DayForecast = ensemble_4DayForecast.max(axis=1)
	min_series_4DayForecast = ensemble_4DayForecast.min(axis=1)
	std_series_4DayForecast = ensemble_4DayForecast.std(axis=1)
	std_dev_upper_series_4DayForecast = avg_series_4DayForecast + std_series_4DayForecast
	std_dev_lower_series_4DayForecast = avg_series_4DayForecast - std_series_4DayForecast

	plt.figure(4)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_4DayForecast, values_benchmark_4DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_4DayForecast_hr, high_res_4DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_4DayForecast, avg_series_4DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_4DayForecast, min_series_4DayForecast, max_series_4DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_4DayForecast, min_series_4DayForecast, max_series_4DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_4DayForecast, std_dev_lower_series_4DayForecast, std_dev_upper_series_4DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('4 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_4DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_4DayForecast[len(dates_4DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_4DayForecast[len(dates_4DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/4 Day Forecast for {0} - {1}.png'.format(station, river))

	#5 day Forecast
	df = pd.read_csv(output_path + '/5_Day_Forecasts.csv')
	dates_5DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_5DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_5DayForecast = dates
	ensemble_5DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/5_Day_Forecasts_High_Res.csv')
	dates_5DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_5DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_5DayForecast_hr = dates
	high_res_5DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_5DayForecast = ensemble_5DayForecast.mean(axis=1)
	max_series_5DayForecast = ensemble_5DayForecast.max(axis=1)
	min_series_5DayForecast = ensemble_5DayForecast.min(axis=1)
	std_series_5DayForecast = ensemble_5DayForecast.std(axis=1)
	std_dev_upper_series_5DayForecast = avg_series_5DayForecast + std_series_5DayForecast
	std_dev_lower_series_5DayForecast = avg_series_5DayForecast - std_series_5DayForecast

	plt.figure(5)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_5DayForecast, values_benchmark_5DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_5DayForecast_hr, high_res_5DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_5DayForecast, avg_series_5DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_5DayForecast, min_series_5DayForecast, max_series_5DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_5DayForecast, min_series_5DayForecast, max_series_5DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_5DayForecast, std_dev_lower_series_5DayForecast, std_dev_upper_series_5DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('5 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_5DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_5DayForecast[len(dates_5DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_5DayForecast[len(dates_5DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/5 Day Forecast for {0} - {1}.png'.format(station, river))

	#6 day Forecast
	df = pd.read_csv(output_path + '/6_Day_Forecasts.csv')
	dates_6DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_6DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_6DayForecast = dates
	ensemble_6DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/6_Day_Forecasts_High_Res.csv')
	dates_6DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_6DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_6DayForecast_hr = dates
	high_res_6DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_6DayForecast = ensemble_6DayForecast.mean(axis=1)
	max_series_6DayForecast = ensemble_6DayForecast.max(axis=1)
	min_series_6DayForecast = ensemble_6DayForecast.min(axis=1)
	std_series_6DayForecast = ensemble_6DayForecast.std(axis=1)
	std_dev_upper_series_6DayForecast = avg_series_6DayForecast + std_series_6DayForecast
	std_dev_lower_series_6DayForecast = avg_series_6DayForecast - std_series_6DayForecast

	plt.figure(6)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_6DayForecast, values_benchmark_6DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_6DayForecast_hr, high_res_6DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_6DayForecast, avg_series_6DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_6DayForecast, min_series_6DayForecast, max_series_6DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_6DayForecast, min_series_6DayForecast, max_series_6DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_6DayForecast, std_dev_lower_series_6DayForecast, std_dev_upper_series_6DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('6 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_6DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_6DayForecast[len(dates_6DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_6DayForecast[len(dates_6DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/6 Day Forecast for {0} - {1}.png'.format(station, river))

	#7 day Forecast
	df = pd.read_csv(output_path + '/7_Day_Forecasts.csv')
	dates_7DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_7DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_7DayForecast = dates
	ensemble_7DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/7_Day_Forecasts_High_Res.csv')
	dates_7DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_7DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_7DayForecast_hr = dates
	high_res_7DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_7DayForecast = ensemble_7DayForecast.mean(axis=1)
	max_series_7DayForecast = ensemble_7DayForecast.max(axis=1)
	min_series_7DayForecast = ensemble_7DayForecast.min(axis=1)
	std_series_7DayForecast = ensemble_7DayForecast.std(axis=1)
	std_dev_upper_series_7DayForecast = avg_series_7DayForecast + std_series_7DayForecast
	std_dev_lower_series_7DayForecast = avg_series_7DayForecast - std_series_7DayForecast

	plt.figure(7)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_7DayForecast, values_benchmark_7DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_7DayForecast_hr, high_res_7DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_7DayForecast, avg_series_7DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_7DayForecast, min_series_7DayForecast, max_series_7DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_7DayForecast, min_series_7DayForecast, max_series_7DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_7DayForecast, std_dev_lower_series_7DayForecast, std_dev_upper_series_7DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('7 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_7DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_7DayForecast[len(dates_7DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_7DayForecast[len(dates_7DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/7 Day Forecast for {0} - {1}.png'.format(station, river))

	#8 day Forecast
	df = pd.read_csv(output_path + '/8_Day_Forecasts.csv')
	dates_8DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_8DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_8DayForecast = dates
	ensemble_8DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/8_Day_Forecasts_High_Res.csv')
	dates_8DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_8DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_8DayForecast_hr = dates
	high_res_8DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_8DayForecast = ensemble_8DayForecast.mean(axis=1)
	max_series_8DayForecast = ensemble_8DayForecast.max(axis=1)
	min_series_8DayForecast = ensemble_8DayForecast.min(axis=1)
	std_series_8DayForecast = ensemble_8DayForecast.std(axis=1)
	std_dev_upper_series_8DayForecast = avg_series_8DayForecast + std_series_8DayForecast
	std_dev_lower_series_8DayForecast = avg_series_8DayForecast - std_series_8DayForecast

	plt.figure(8)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_8DayForecast, values_benchmark_8DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_8DayForecast_hr, high_res_8DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_8DayForecast, avg_series_8DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_8DayForecast, min_series_8DayForecast, max_series_8DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_8DayForecast, min_series_8DayForecast, max_series_8DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_8DayForecast, std_dev_lower_series_8DayForecast, std_dev_upper_series_8DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('8 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_8DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_8DayForecast[len(dates_8DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_8DayForecast[len(dates_8DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/8 Day Forecast for {0} - {1}.png'.format(station, river))

	#9 day Forecast
	df = pd.read_csv(output_path + '/9_Day_Forecasts.csv')
	dates_9DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_9DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_9DayForecast = dates
	ensemble_9DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/9_Day_Forecasts_High_Res.csv')
	dates_9DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_9DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_9DayForecast_hr = dates
	high_res_9DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_9DayForecast = ensemble_9DayForecast.mean(axis=1)
	max_series_9DayForecast = ensemble_9DayForecast.max(axis=1)
	min_series_9DayForecast = ensemble_9DayForecast.min(axis=1)
	std_series_9DayForecast = ensemble_9DayForecast.std(axis=1)
	std_dev_upper_series_9DayForecast = avg_series_9DayForecast + std_series_9DayForecast
	std_dev_lower_series_9DayForecast = avg_series_9DayForecast - std_series_9DayForecast

	plt.figure(9)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_9DayForecast, values_benchmark_9DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_9DayForecast_hr, high_res_9DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_9DayForecast, avg_series_9DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_9DayForecast, min_series_9DayForecast, max_series_9DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_9DayForecast, min_series_9DayForecast, max_series_9DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_9DayForecast, std_dev_lower_series_9DayForecast, std_dev_upper_series_9DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('9 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_9DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_9DayForecast[len(dates_9DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_9DayForecast[len(dates_9DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/9 Day Forecast for {0} - {1}.png'.format(station, river))

	#10 day Forecast
	df = pd.read_csv(output_path + '/10_Day_Forecasts.csv')
	dates_10DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_10DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_10DayForecast = dates
	ensemble_10DayForecast = df[df.columns[1:51]]

	df = pd.read_csv(output_path + '/10_Day_Forecasts_High_Res.csv')
	dates_10DayForecast_hr = df['Date'].tolist()
	dates = []
	for date in dates_10DayForecast_hr:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_10DayForecast_hr = dates
	high_res_10DayForecast = df['High Resolution Forecast (m^3/s)'].tolist()

	avg_series_10DayForecast = ensemble_10DayForecast.mean(axis=1)
	max_series_10DayForecast = ensemble_10DayForecast.max(axis=1)
	min_series_10DayForecast = ensemble_10DayForecast.min(axis=1)
	std_series_10DayForecast = ensemble_10DayForecast.std(axis=1)
	std_dev_upper_series_10DayForecast = avg_series_10DayForecast + std_series_10DayForecast
	std_dev_lower_series_10DayForecast = avg_series_10DayForecast - std_series_10DayForecast

	plt.figure(10)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_10DayForecast, values_benchmark_10DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_10DayForecast_hr, high_res_10DayForecast, 'k', color='black', label='HRES')
	plt.plot(dates_10DayForecast, avg_series_10DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_10DayForecast, min_series_10DayForecast, max_series_10DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_10DayForecast, min_series_10DayForecast, max_series_10DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_10DayForecast, std_dev_lower_series_10DayForecast, std_dev_upper_series_10DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('10 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_10DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_10DayForecast[len(dates_10DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_10DayForecast[len(dates_10DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/10 Day Forecast for {0} - {1}.png'.format(station, river))

	#11 day Forecast
	df = pd.read_csv(output_path + '/11_Day_Forecasts.csv')
	dates_11DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_11DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_11DayForecast = dates
	ensemble_11DayForecast = df[df.columns[1:51]]

	avg_series_11DayForecast = ensemble_11DayForecast.mean(axis=1)
	max_series_11DayForecast = ensemble_11DayForecast.max(axis=1)
	min_series_11DayForecast = ensemble_11DayForecast.min(axis=1)
	std_series_11DayForecast = ensemble_11DayForecast.std(axis=1)
	std_dev_upper_series_11DayForecast = avg_series_11DayForecast + std_series_11DayForecast
	std_dev_lower_series_11DayForecast = avg_series_11DayForecast - std_series_11DayForecast

	plt.figure(11)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_11DayForecast, values_benchmark_11DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_11DayForecast, avg_series_11DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_11DayForecast, min_series_11DayForecast, max_series_11DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_11DayForecast, min_series_11DayForecast, max_series_11DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_11DayForecast, std_dev_lower_series_11DayForecast, std_dev_upper_series_11DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('11 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_11DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_11DayForecast[len(dates_11DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_11DayForecast[len(dates_11DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/11 Day Forecast for {0} - {1}.png'.format(station, river))

	#12 day Forecast
	df = pd.read_csv(output_path + '/12_Day_Forecasts.csv')
	dates_12DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_12DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_12DayForecast = dates
	ensemble_12DayForecast = df[df.columns[1:51]]

	avg_series_12DayForecast = ensemble_12DayForecast.mean(axis=1)
	max_series_12DayForecast = ensemble_12DayForecast.max(axis=1)
	min_series_12DayForecast = ensemble_12DayForecast.min(axis=1)
	std_series_12DayForecast = ensemble_12DayForecast.std(axis=1)
	std_dev_upper_series_12DayForecast = avg_series_12DayForecast + std_series_12DayForecast
	std_dev_lower_series_12DayForecast = avg_series_12DayForecast - std_series_12DayForecast

	plt.figure(12)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_12DayForecast, values_benchmark_12DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_12DayForecast, avg_series_12DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_12DayForecast, min_series_12DayForecast, max_series_12DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_12DayForecast, min_series_12DayForecast, max_series_12DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_12DayForecast, std_dev_lower_series_12DayForecast, std_dev_upper_series_12DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('12 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_12DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_12DayForecast[len(dates_12DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_12DayForecast[len(dates_12DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/12 Day Forecast for {0} - {1}.png'.format(station, river))

	#13 day Forecast
	df = pd.read_csv(output_path + '/13_Day_Forecasts.csv')
	dates_13DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_13DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_13DayForecast = dates
	ensemble_13DayForecast = df[df.columns[1:51]]

	avg_series_13DayForecast = ensemble_13DayForecast.mean(axis=1)
	max_series_13DayForecast = ensemble_13DayForecast.max(axis=1)
	min_series_13DayForecast = ensemble_13DayForecast.min(axis=1)
	std_series_13DayForecast = ensemble_13DayForecast.std(axis=1)
	std_dev_upper_series_13DayForecast = avg_series_13DayForecast + std_series_13DayForecast
	std_dev_lower_series_13DayForecast = avg_series_13DayForecast - std_series_13DayForecast

	plt.figure(13)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_13DayForecast, values_benchmark_13DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_13DayForecast, avg_series_13DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_13DayForecast, min_series_13DayForecast, max_series_13DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_13DayForecast, min_series_13DayForecast, max_series_13DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_13DayForecast, std_dev_lower_series_13DayForecast, std_dev_upper_series_13DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('13 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_13DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_13DayForecast[len(dates_13DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_13DayForecast[len(dates_13DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/13 Day Forecast for {0} - {1}.png'.format(station, river))

	#14 day Forecast
	df = pd.read_csv(output_path + '/14_Day_Forecasts.csv')
	dates_14DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_14DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_14DayForecast = dates
	ensemble_14DayForecast = df[df.columns[1:51]]

	avg_series_14DayForecast = ensemble_14DayForecast.mean(axis=1)
	max_series_14DayForecast = ensemble_14DayForecast.max(axis=1)
	min_series_14DayForecast = ensemble_14DayForecast.min(axis=1)
	std_series_14DayForecast = ensemble_14DayForecast.std(axis=1)
	std_dev_upper_series_14DayForecast = avg_series_14DayForecast + std_series_14DayForecast
	std_dev_lower_series_14DayForecast = avg_series_14DayForecast - std_series_14DayForecast

	plt.figure(14)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_14DayForecast, values_benchmark_14DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_14DayForecast, avg_series_14DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_14DayForecast, min_series_14DayForecast, max_series_14DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_14DayForecast, min_series_14DayForecast, max_series_14DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_14DayForecast, std_dev_lower_series_14DayForecast, std_dev_upper_series_14DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('14 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_14DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_14DayForecast[len(dates_14DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_14DayForecast[len(dates_14DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/14 Day Forecast for {0} - {1}.png'.format(station, river))

	#15 day Forecast
	df = pd.read_csv(output_path + '/15_Day_Forecasts.csv')
	dates_15DayForecast = df['Date'].tolist()
	dates = []
	for date in dates_15DayForecast:
		dates.append(dt.datetime.strptime(date, "%Y-%m-%d"))
	dates_15DayForecast = dates
	ensemble_15DayForecast = df[df.columns[1:51]]

	avg_series_15DayForecast = ensemble_15DayForecast.mean(axis=1)
	max_series_15DayForecast = ensemble_15DayForecast.max(axis=1)
	min_series_15DayForecast = ensemble_15DayForecast.min(axis=1)
	std_series_15DayForecast = ensemble_15DayForecast.std(axis=1)
	std_dev_upper_series_15DayForecast = avg_series_15DayForecast + std_series_15DayForecast
	std_dev_lower_series_15DayForecast = avg_series_15DayForecast - std_series_15DayForecast

	plt.figure(15)
	plt.figure(figsize=(15, 9))
	##plt.plot(dates_observed, values_observed, 'g-', label='Observed Values')
	plt.plot(dates_water_balance, values_water_balance, 'k', color='red', label='Water Balance')
	plt.plot(dates_benchmark_15DayForecast, values_benchmark_15DayForecast, ':', color='m', label='Benchmark')
	plt.plot(dates_15DayForecast, avg_series_15DayForecast, 'k', color='blue', label='Mean')
	# plt.fill_between(dates_15DayForecast, min_series_15DayForecast, max_series_15DayForecast, alpha=0.3, facecolor='#228b22', label='Limits')
	plt.fill_between(dates_15DayForecast, min_series_15DayForecast, max_series_15DayForecast, alpha=0.5,
	                 edgecolor='#228b22', facecolor='#98fb98', label='Ensemble')
	# plt.fill_between(dates_15DayForecast, std_dev_lower_series_15DayForecast, std_dev_upper_series_15DayForecast, alpha=0.8, facecolor='#98fb98', label='Std')
	plt.title('15 Day Forecast for {0} - {1}'.format(station, river))
	plt.xlabel('Date')
	plt.ylabel('Streamflow (m$^3$/s)')
	plt.legend()
	plt.grid()
	xmin = max(dates_water_balance[0], dates_15DayForecast[0])
	xmax = min(dates_water_balance[len(dates_water_balance) - 1], dates_15DayForecast[len(dates_15DayForecast) - 1])
	xmax2 = max(dates_water_balance[len(dates_water_balance) - 1], dates_15DayForecast[len(dates_15DayForecast) - 1])
	plt.xlim(xmin, xmax)
	t = pd.date_range(xmin, xmax, periods=7).to_pydatetime()
	plt.xticks(t)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.tight_layout()
	plt.savefig(output_path + '/15 Day Forecast for {0} - {1}.png'.format(station, river))
	plt.close('all')