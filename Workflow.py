import hydrostats.ens_metrics as em
import pandas as pd
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt

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

	print(id, station, river, spt)

	#output_path = "/Users/student/Desktop/output/South_Asia2017_csv/{}".format(spt)
	output_path = "/Users/student/Desktop/output/South_America_csv/{}".format(id)

	# Water Balance
	init_df = pd.read_csv(output_path + '/Initialization_Values.csv', index_col=0)
	init_df.index = pd.to_datetime(init_df.index)

	# Persistence
	persistence_1day_df = init_df.copy()
	persistence_1day_df.index += pd.DateOffset(days=1)
	persistence_1day_df.columns = ["Persistence_1_day"]

	persistence_2day_df = init_df.copy()
	persistence_2day_df.index += pd.DateOffset(days=2)
	persistence_2day_df.columns = ["Persistence_2_days"]

	persistence_3day_df = init_df.copy()
	persistence_3day_df.index += pd.DateOffset(days=3)
	persistence_3day_df.columns = ["Persistence_3_days"]

	persistence_4day_df = init_df.copy()
	persistence_4day_df.index += pd.DateOffset(days=4)
	persistence_4day_df.columns = ["Persistence_4_days"]

	persistence_5day_df = init_df.copy()
	persistence_5day_df.index += pd.DateOffset(days=5)
	persistence_5day_df.columns = ["Persistence_5_days"]

	persistence_6day_df = init_df.copy()
	persistence_6day_df.index += pd.DateOffset(days=6)
	persistence_6day_df.columns = ["Persistence_6_days"]

	persistence_7day_df = init_df.copy()
	persistence_7day_df.index += pd.DateOffset(days=7)
	persistence_7day_df.columns = ["Persistence_7_days"]

	persistence_8day_df = init_df.copy()
	persistence_8day_df.index += pd.DateOffset(days=8)
	persistence_8day_df.columns = ["Persistence_8_days"]

	persistence_9day_df = init_df.copy()
	persistence_9day_df.index += pd.DateOffset(days=9)
	persistence_9day_df.columns = ["Persistence_9_days"]

	persistence_10day_df = init_df.copy()
	persistence_10day_df.index += pd.DateOffset(days=10)
	persistence_10day_df.columns = ["Persistence_10_days"]

	persistence_11day_df = init_df.copy()
	persistence_11day_df.index += pd.DateOffset(days=11)
	persistence_11day_df.columns = ["Persistence_11_days"]

	persistence_12day_df = init_df.copy()
	persistence_12day_df.index += pd.DateOffset(days=12)
	persistence_12day_df.columns = ["Persistence_12_days"]

	persistence_13day_df = init_df.copy()
	persistence_13day_df.index += pd.DateOffset(days=13)
	persistence_13day_df.columns = ["Persistence_13_days"]

	persistence_14day_df = init_df.copy()
	persistence_14day_df.index += pd.DateOffset(days=14)
	persistence_14day_df.columns = ["Persistence_14_days"]

	persistence_15day_df = init_df.copy()
	persistence_15day_df.index += pd.DateOffset(days=15)
	persistence_15day_df.columns = ["Persistence_15_days"]


	#info Plots

	days_forecast = np.linspace(1.0, 15.0, num=15)
	days_forecast = days_forecast.tolist()
	crps_mean = []
	skillScore = []

	# 1 day Forecast
	one_day_forecast_df = pd.read_csv(output_path + '/1_Day_Forecasts.csv', index_col=0)
	one_day_forecast_df.index = pd.to_datetime(one_day_forecast_df.index)

	merged_1day_df = pd.DataFrame.join(init_df, [persistence_1day_df, one_day_forecast_df]).dropna()

	obs_1day = merged_1day_df.iloc[:, 0].values
	bench_1day = merged_1day_df.iloc[:, 1].values
	forecasts_1day = merged_1day_df.iloc[:, 2:].values
	datetime_1day = pd.to_datetime(merged_1day_df.index)

	crps_1DayForecast = em.ens_crps(obs_1day, forecasts_1day)
	crps_bench_1DayForecast = np.mean(np.abs(obs_1day - bench_1day))
	crpss_1DayForecast = em.skill_score(crps_1DayForecast["crpsMean"], crps_bench_1DayForecast, 0)

	crps_mean.append(crps_1DayForecast['crpsMean'])
	skillScore.append(crpss_1DayForecast["skillScore"])


	#2 day Forecasts
	two_day_forecast_df = pd.read_csv(output_path + '/2_Day_Forecasts.csv', index_col=0)
	two_day_forecast_df.index = pd.to_datetime(two_day_forecast_df.index)

	merged_2day_df = pd.DataFrame.join(init_df, [persistence_2day_df, two_day_forecast_df]).dropna()

	obs_2day = merged_2day_df.iloc[:, 0].values
	bench_2day = merged_2day_df.iloc[:, 1].values
	forecasts_2day = merged_2day_df.iloc[:, 2:].values
	datetime_2day = pd.to_datetime(merged_2day_df.index)

	crps_2DayForecast = em.ens_crps(obs_2day, forecasts_2day)
	crps_bench_2DayForecast = np.mean(np.abs(obs_2day - bench_2day))
	crpss_2DayForecast = em.skill_score(crps_2DayForecast["crpsMean"], crps_bench_2DayForecast, 0)

	crps_mean.append(crps_2DayForecast['crpsMean'])
	skillScore.append(crpss_2DayForecast["skillScore"])


	#3 day Forecasts
	three_day_forecast_df = pd.read_csv(output_path + '/3_Day_Forecasts.csv', index_col=0)
	three_day_forecast_df.index = pd.to_datetime(three_day_forecast_df.index)

	merged_3day_df = pd.DataFrame.join(init_df, [persistence_3day_df, three_day_forecast_df]).dropna()

	obs_3day = merged_3day_df.iloc[:, 0].values
	bench_3day = merged_3day_df.iloc[:, 1].values
	forecasts_3day = merged_3day_df.iloc[:, 2:].values
	datetime_3day = pd.to_datetime(merged_3day_df.index)

	crps_3DayForecast = em.ens_crps(obs_3day, forecasts_3day)
	crps_bench_3DayForecast = np.mean(np.abs(obs_3day - bench_3day))
	crpss_3DayForecast = em.skill_score(crps_3DayForecast["crpsMean"], crps_bench_3DayForecast, 0)

	crps_mean.append(crps_3DayForecast['crpsMean'])
	skillScore.append(crpss_3DayForecast["skillScore"])


	#4 day Forecasts
	four_day_forecast_df = pd.read_csv(output_path + '/4_Day_Forecasts.csv', index_col=0)
	four_day_forecast_df.index = pd.to_datetime(four_day_forecast_df.index)

	merged_4day_df = pd.DataFrame.join(init_df, [persistence_4day_df, four_day_forecast_df]).dropna()

	obs_4day = merged_4day_df.iloc[:, 0].values
	bench_4day = merged_4day_df.iloc[:, 1].values
	forecasts_4day = merged_4day_df.iloc[:, 2:].values
	datetime_4day = pd.to_datetime(merged_4day_df.index)

	crps_4DayForecast = em.ens_crps(obs_4day, forecasts_4day)
	crps_bench_4DayForecast = np.mean(np.abs(obs_4day - bench_4day))
	crpss_4DayForecast = em.skill_score(crps_4DayForecast["crpsMean"], crps_bench_4DayForecast, 0)

	crps_mean.append(crps_4DayForecast['crpsMean'])
	skillScore.append(crpss_4DayForecast["skillScore"])


	#5 day Forecasts
	five_day_forecast_df = pd.read_csv(output_path + '/5_Day_Forecasts.csv', index_col=0)
	five_day_forecast_df.index = pd.to_datetime(five_day_forecast_df.index)

	merged_5day_df = pd.DataFrame.join(init_df, [persistence_5day_df, five_day_forecast_df]).dropna()

	obs_5day = merged_5day_df.iloc[:, 0].values
	bench_5day = merged_5day_df.iloc[:, 1].values
	forecasts_5day = merged_5day_df.iloc[:, 2:].values
	datetime_5day = pd.to_datetime(merged_5day_df.index)

	crps_5DayForecast = em.ens_crps(obs_5day, forecasts_5day)
	crps_bench_5DayForecast = np.mean(np.abs(obs_5day - bench_5day))
	crpss_5DayForecast = em.skill_score(crps_5DayForecast["crpsMean"], crps_bench_5DayForecast, 0)

	crps_mean.append(crps_5DayForecast['crpsMean'])
	skillScore.append(crpss_5DayForecast["skillScore"])


	#6 day Forecasts
	six_day_forecast_df = pd.read_csv(output_path + '/6_Day_Forecasts.csv', index_col=0)
	six_day_forecast_df.index = pd.to_datetime(six_day_forecast_df.index)

	merged_6day_df = pd.DataFrame.join(init_df, [persistence_6day_df, six_day_forecast_df]).dropna()

	obs_6day = merged_6day_df.iloc[:, 0].values
	bench_6day = merged_6day_df.iloc[:, 1].values
	forecasts_6day = merged_6day_df.iloc[:, 2:].values
	datetime_6day = pd.to_datetime(merged_6day_df.index)

	crps_6DayForecast = em.ens_crps(obs_6day, forecasts_6day)
	crps_bench_6DayForecast = np.mean(np.abs(obs_6day - bench_6day))
	crpss_6DayForecast = em.skill_score(crps_6DayForecast["crpsMean"], crps_bench_6DayForecast, 0)

	crps_mean.append(crps_6DayForecast['crpsMean'])
	skillScore.append(crpss_6DayForecast["skillScore"])


	#7 day Forecasts
	seven_day_forecast_df = pd.read_csv(output_path + '/7_Day_Forecasts.csv', index_col=0)
	seven_day_forecast_df.index = pd.to_datetime(seven_day_forecast_df.index)

	merged_7day_df = pd.DataFrame.join(init_df, [persistence_7day_df, seven_day_forecast_df]).dropna()

	obs_7day = merged_7day_df.iloc[:, 0].values
	bench_7day = merged_7day_df.iloc[:, 1].values
	forecasts_7day = merged_7day_df.iloc[:, 2:].values
	datetime_7day = pd.to_datetime(merged_7day_df.index)

	crps_7DayForecast = em.ens_crps(obs_7day, forecasts_7day)
	crps_bench_7DayForecast = np.mean(np.abs(obs_7day - bench_7day))
	crpss_7DayForecast = em.skill_score(crps_7DayForecast["crpsMean"], crps_bench_7DayForecast, 0)

	crps_mean.append(crps_7DayForecast['crpsMean'])
	skillScore.append(crpss_7DayForecast["skillScore"])

	#8 day Forecasts
	eight_day_forecast_df = pd.read_csv(output_path + '/8_Day_Forecasts.csv', index_col=0)
	eight_day_forecast_df.index = pd.to_datetime(eight_day_forecast_df.index)

	merged_8day_df = pd.DataFrame.join(init_df, [persistence_8day_df, eight_day_forecast_df]).dropna()

	obs_8day = merged_8day_df.iloc[:, 0].values
	bench_8day = merged_8day_df.iloc[:, 1].values
	forecasts_8day = merged_8day_df.iloc[:, 2:].values
	datetime_8day = pd.to_datetime(merged_8day_df.index)

	crps_8DayForecast = em.ens_crps(obs_8day, forecasts_8day)
	crps_bench_8DayForecast = np.mean(np.abs(obs_8day - bench_8day))
	crpss_8DayForecast = em.skill_score(crps_8DayForecast["crpsMean"], crps_bench_8DayForecast, 0)

	crps_mean.append(crps_8DayForecast['crpsMean'])
	skillScore.append(crpss_8DayForecast["skillScore"])


	#9 day Forecasts
	nine_day_forecast_df = pd.read_csv(output_path + '/9_Day_Forecasts.csv', index_col=0)
	nine_day_forecast_df.index = pd.to_datetime(nine_day_forecast_df.index)

	merged_9day_df = pd.DataFrame.join(init_df, [persistence_9day_df, nine_day_forecast_df]).dropna()

	obs_9day = merged_9day_df.iloc[:, 0].values
	bench_9day = merged_9day_df.iloc[:, 1].values
	forecasts_9day = merged_9day_df.iloc[:, 2:].values
	datetime_9day = pd.to_datetime(merged_9day_df.index)

	crps_9DayForecast = em.ens_crps(obs_9day, forecasts_9day)
	crps_bench_9DayForecast = np.mean(np.abs(obs_9day - bench_9day))
	crpss_9DayForecast = em.skill_score(crps_9DayForecast["crpsMean"], crps_bench_9DayForecast, 0)

	crps_mean.append(crps_9DayForecast['crpsMean'])
	skillScore.append(crpss_9DayForecast["skillScore"])


	#10 day Forecasts
	ten_day_forecast_df = pd.read_csv(output_path + '/10_Day_Forecasts.csv', index_col=0)
	ten_day_forecast_df.index = pd.to_datetime(ten_day_forecast_df.index)

	merged_10day_df = pd.DataFrame.join(init_df, [persistence_10day_df, ten_day_forecast_df]).dropna()

	obs_10day = merged_10day_df.iloc[:, 0].values
	bench_10day = merged_10day_df.iloc[:, 1].values
	forecasts_10day = merged_10day_df.iloc[:, 2:].values
	datetime_10day = pd.to_datetime(merged_10day_df.index)

	crps_10DayForecast = em.ens_crps(obs_10day, forecasts_10day)
	crps_bench_10DayForecast = np.mean(np.abs(obs_10day - bench_10day))
	crpss_10DayForecast = em.skill_score(crps_10DayForecast["crpsMean"], crps_bench_10DayForecast, 0)

	crps_mean.append(crps_10DayForecast['crpsMean'])
	skillScore.append(crpss_10DayForecast["skillScore"])


	#11 day Forecasts
	eleven_day_forecast_df = pd.read_csv(output_path + '/11_Day_Forecasts.csv', index_col=0)
	eleven_day_forecast_df.index = pd.to_datetime(eleven_day_forecast_df.index)

	merged_11day_df = pd.DataFrame.join(init_df, [persistence_11day_df, eleven_day_forecast_df]).dropna()

	obs_11day = merged_11day_df.iloc[:, 0].values
	bench_11day = merged_11day_df.iloc[:, 1].values
	forecasts_11day = merged_11day_df.iloc[:, 2:].values
	datetime_11day = pd.to_datetime(merged_11day_df.index)

	crps_11DayForecast = em.ens_crps(obs_11day, forecasts_11day)
	crps_bench_11DayForecast = np.mean(np.abs(obs_11day - bench_11day))
	crpss_11DayForecast = em.skill_score(crps_11DayForecast["crpsMean"], crps_bench_11DayForecast, 0)

	crps_mean.append(crps_11DayForecast['crpsMean'])
	skillScore.append(crpss_11DayForecast["skillScore"])


	#12 day Forecasts
	twelve_day_forecast_df = pd.read_csv(output_path + '/12_Day_Forecasts.csv', index_col=0)
	twelve_day_forecast_df.index = pd.to_datetime(twelve_day_forecast_df.index)

	merged_12day_df = pd.DataFrame.join(init_df, [persistence_12day_df, twelve_day_forecast_df]).dropna()

	obs_12day = merged_12day_df.iloc[:, 0].values
	bench_12day = merged_12day_df.iloc[:, 1].values
	forecasts_12day = merged_12day_df.iloc[:, 2:].values
	datetime_12day = pd.to_datetime(merged_12day_df.index)

	crps_12DayForecast = em.ens_crps(obs_12day, forecasts_12day)
	crps_bench_12DayForecast = np.mean(np.abs(obs_12day - bench_12day))
	crpss_12DayForecast = em.skill_score(crps_12DayForecast["crpsMean"], crps_bench_12DayForecast, 0)

	crps_mean.append(crps_12DayForecast['crpsMean'])
	skillScore.append(crpss_12DayForecast["skillScore"])


	#13 day Forecasts
	thirteen_day_forecast_df = pd.read_csv(output_path + '/13_Day_Forecasts.csv', index_col=0)
	thirteen_day_forecast_df.index = pd.to_datetime(thirteen_day_forecast_df.index)

	merged_13day_df = pd.DataFrame.join(init_df, [persistence_13day_df, thirteen_day_forecast_df]).dropna()

	obs_13day = merged_13day_df.iloc[:, 0].values
	bench_13day = merged_13day_df.iloc[:, 1].values
	forecasts_13day = merged_13day_df.iloc[:, 2:].values
	datetime_13day = pd.to_datetime(merged_13day_df.index)

	crps_13DayForecast = em.ens_crps(obs_13day, forecasts_13day)
	crps_bench_13DayForecast = np.mean(np.abs(obs_13day - bench_13day))
	crpss_13DayForecast = em.skill_score(crps_13DayForecast["crpsMean"], crps_bench_13DayForecast, 0)

	crps_mean.append(crps_13DayForecast['crpsMean'])
	skillScore.append(crpss_13DayForecast["skillScore"])


	#14 day Forecasts
	fourteen_day_forecast_df = pd.read_csv(output_path + '/14_Day_Forecasts.csv', index_col=0)
	fourteen_day_forecast_df.index = pd.to_datetime(fourteen_day_forecast_df.index)

	merged_14day_df = pd.DataFrame.join(init_df, [persistence_14day_df, fourteen_day_forecast_df]).dropna()

	obs_14day = merged_14day_df.iloc[:, 0].values
	bench_14day = merged_14day_df.iloc[:, 1].values
	forecasts_14day = merged_14day_df.iloc[:, 2:].values
	datetime_14day = pd.to_datetime(merged_14day_df.index)

	crps_14DayForecast = em.ens_crps(obs_14day, forecasts_14day)
	crps_bench_14DayForecast = np.mean(np.abs(obs_14day - bench_14day))
	crpss_14DayForecast = em.skill_score(crps_14DayForecast["crpsMean"], crps_bench_14DayForecast, 0)

	crps_mean.append(crps_14DayForecast['crpsMean'])
	skillScore.append(crpss_14DayForecast["skillScore"])


	#15 day Forecasts
	fifteen_day_forecast_df = pd.read_csv(output_path + '/15_Day_Forecasts.csv', index_col=0)
	fifteen_day_forecast_df.index = pd.to_datetime(fifteen_day_forecast_df.index)

	merged_15day_df = pd.DataFrame.join(init_df, [persistence_15day_df, fifteen_day_forecast_df]).dropna()

	obs_15day = merged_15day_df.iloc[:, 0].values
	bench_15day = merged_15day_df.iloc[:, 1].values
	forecasts_15day = merged_15day_df.iloc[:, 2:].values
	datetime_15day = pd.to_datetime(merged_15day_df.index)

	crps_15DayForecast = em.ens_crps(obs_15day, forecasts_15day)
	crps_bench_15DayForecast = np.mean(np.abs(obs_15day - bench_15day))
	crpss_15DayForecast = em.skill_score(crps_15DayForecast["crpsMean"], crps_bench_15DayForecast, 0)

	crps_mean.append(crps_15DayForecast['crpsMean'])
	skillScore.append(crpss_15DayForecast["skillScore"])


	#CRPS
	plt.figure(1)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, crps_mean, label='CPRS')
	plt.title('Average CPRS at {0} - {1}'.format(station, river))
	plt.xlabel('Days of Forecast')
	plt.ylabel('CPRS')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(output_path + '/Average CPRS at {0} - {1}.png'.format(station, river))

	#Skill Score
	plt.figure(2)
	plt.figure(figsize=(15, 9))
	plt.bar(days_forecast, skillScore, label='Skill Socore')
	plt.title('Skill Score at {0} - {1}'.format(station, river))
	plt.xlabel('Days of Forecast')
	plt.ylabel('Skill Score')
	plt.legend()
	plt.grid()
	plt.xticks(days_forecast)
	plt.savefig(output_path + '/Skill Score at {0} - {1}.png'.format(station, river))
	plt.close('all')