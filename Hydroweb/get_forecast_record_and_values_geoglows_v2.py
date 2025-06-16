import io
import os
import math
import requests
import geoglows
import numpy as np
import pandas as pd
import datetime as dt

import warnings
warnings.filterwarnings('ignore')

#Get Forecast Records
def get_forecast_records(river_id, start_date):
	"""
	Fetches forecast data from a given start date to today, calculates the mean of the first 8 time steps,
	and appends results for consecutive forecast dates.

	Args:
		get_forecast_records (function): A function that retrieves forecast records for a given date and river ID.
		river_id (int): The ID of the river for which forecast data will be retrieved.
		start_date (str): The starting date in YYYYMMDD format.
	Returns:
		pd.DataFrame: A DataFrame with the forecast records.
	"""
	# Initialize an empty DataFrame to hold mean values
	forecast_records_df = pd.DataFrame()

	# Convert start date to a datetime object
	start_date = dt.datetime.strptime(start_date, '%Y%m%d')

	# Iterate over each day from the start date to today
	current_date = start_date
	end_date = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

	while current_date <= end_date:

		yyyy = str(current_date.year)
		mm = current_date.month

		if mm < 10:
			mm ='0{0}'.format(mm)
		else:
			mm = str(mm)
		dd = current_date.day

		if dd < 10:
			dd ='0{0}'.format(dd)
		else:
			dd = str(dd)

		# Fetch forecast data for the current date
		try:
			forecast_data = geoglows.data.forecast_ensembles(river_id, date='{0}{1}{2}'.format(yyyy,mm,dd))
			forecast_data.index = pd.to_datetime(forecast_data.index)
			forecast_data[forecast_data < 0] = 0
			forecast_data.index = forecast_data.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
			forecast_data.index = pd.to_datetime(forecast_data.index)
			forecast_data = forecast_data.drop(['ensemble_52'], axis=1)
			forecast_data.dropna(inplace=True)

			mean_df = forecast_data.mean(axis=1).to_frame()
			mean_df.rename(columns = {0:'average_flow'}, inplace = True)

			if isinstance(mean_df, pd.DataFrame):
				# Extract the first 8 time steps
				summary_df = mean_df.iloc[:8]
				# Append to the main DataFrame
				forecast_records_df = pd.concat([forecast_records_df, summary_df])

		except Exception as e:
			print(e)

		# Move to the next day
		current_date += dt.timedelta(days=1)

	return forecast_records_df

def gumbel_1(std: float, xbar: float, rp: int or float) -> float:
  """
  Solves the Gumbel Type I probability distribution function (pdf) = exp(-exp(-b)) where b is the covariate. Provide
  the standard deviation and mean of the list of annual maximum flows. Compare scipy.stats.gumbel_r
  Args:
  	std (float): the standard deviation of the series
    xbar (float): the mean of the series
    rp (int or float): the return period in years
  Returns:
  	float, the flow corresponding to the return period specified
  """
  # xbar = statistics.mean(year_max_flow_list)
  # std = statistics.stdev(year_max_flow_list, xbar=xbar)
  return -math.log(-math.log(1 - (1 / rp))) * std * .7797 + xbar - (.45 * std)

def return_period_values(time_series_df, comid):

	max_annual_flow = time_series_df.groupby(time_series_df.index.year).max()
	mean_value = np.mean(max_annual_flow.values.flatten())
	std_value = np.std(max_annual_flow.values.flatten())

	return_periods = [2, 5, 10, 25, 50, 100]

	return_periods_values = [gumbel_1(std_value, mean_value, rp) for rp in return_periods]

	d = {'rivid': [comid],
		 2: [return_periods_values[0]],
		 5: [return_periods_values[1]],
		 10: [return_periods_values[2]],
		 25: [return_periods_values[3]],
		 50: [return_periods_values[4]],
		 100: [return_periods_values[5]]}

	rperiods_df = pd.DataFrame(data=d)
	rperiods_df.set_index('rivid', inplace=True)

	rperiods_df = rperiods_df.T

	rperiods_df.columns.name = 'river_id'
	rperiods_df.index.name = 'return_period'

	return rperiods_df

'''Correct Bias Forecasts'''
def fix_forecast(sim_hist, fore_nofix, obs):

	# Selection of monthly simulated data
	monthly_simulated = sim_hist[sim_hist.index.month == (fore_nofix.index[0]).month].dropna()

	# Obtain Min and max value
	min_simulated = monthly_simulated.min().values[0]
	max_simulated = monthly_simulated.max().values[0]

	min_factor_df   = fore_nofix.copy()
	max_factor_df   = fore_nofix.copy()
	forecast_ens_df = fore_nofix.copy()

	for column in fore_nofix.columns:

		# Min Factor
		tmp_array = np.ones(fore_nofix[column].shape[0])
		tmp_array[fore_nofix[column] < min_simulated] = 0
		min_factor = np.where(tmp_array == 0, fore_nofix[column] / min_simulated, tmp_array)

		# Max factor
		tmp_array = np.ones(fore_nofix[column].shape[0])
		tmp_array[fore_nofix[column] > max_simulated] = 0
		max_factor = np.where(tmp_array == 0, fore_nofix[column] / max_simulated, tmp_array)

		# Replace
		tmp_fore_nofix = fore_nofix[column].copy()
		tmp_fore_nofix.mask(tmp_fore_nofix <= min_simulated, min_simulated, inplace=True)
		tmp_fore_nofix.mask(tmp_fore_nofix >= max_simulated, max_simulated, inplace=True)

		# Save data
		forecast_ens_df.update(pd.DataFrame(tmp_fore_nofix, index=fore_nofix.index, columns=[column]))
		min_factor_df.update(pd.DataFrame(min_factor, index=fore_nofix.index, columns=[column]))
		max_factor_df.update(pd.DataFrame(max_factor, index=fore_nofix.index, columns=[column]))

	# Get  Bias Correction
	corrected_ensembles = geoglows.bias.correct_forecast(forecast_ens_df, sim_hist, obs)
	corrected_ensembles = corrected_ensembles.multiply(min_factor_df, axis=0)
	corrected_ensembles = corrected_ensembles.multiply(max_factor_df, axis=0)

	return corrected_ensembles

def forecast_stats(ensembles_df):
	ensemble = ensembles_df.copy()
	high_res_df = ensemble['ensemble_52'].to_frame()
	ensemble.drop(columns=['ensemble_52'], inplace=True)
	ensemble.dropna(inplace=True)
	high_res_df.dropna(inplace=True)

	max_df = ensemble.quantile(1.0, axis=1).to_frame()
	max_df.rename(columns={1.0: 'flow_max'}, inplace=True)

	p75_df = ensemble.quantile(0.75, axis=1).to_frame()
	p75_df.rename(columns={0.75: 'flow_75p'}, inplace=True)

	p50_df = ensemble.quantile(0.50, axis=1).to_frame()
	p50_df.rename(columns={0.50: 'flow_med'}, inplace=True)

	p25_df = ensemble.quantile(0.25, axis=1).to_frame()
	p25_df.rename(columns={0.25: 'flow_25p'}, inplace=True)

	min_df = ensemble.quantile(0, axis=1).to_frame()
	min_df.rename(columns={0.0: 'flow_min'}, inplace=True)

	mean_df = ensemble.mean(axis=1).to_frame()
	mean_df.rename(columns={0: 'flow_avg'}, inplace=True)

	high_res_df.rename(columns={'ensemble_52': 'high_res'}, inplace=True)

	forecast_stats_df = pd.concat([max_df, p75_df, mean_df, p50_df, p25_df, min_df, high_res_df], axis=1)

	return forecast_stats_df

stations_pd = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Selected_GEOGloWS_v2_sat_WL.csv')
stations_pd = stations_pd.sort_values(by="Station", ascending=True)
stations_pd = stations_pd[stations_pd['COMID_v2'] != 0]

IDs = stations_pd['Code'].tolist()
COMIDs = stations_pd['COMID_v2'].astype(int).tolist()
Names = stations_pd['Station'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	''''Using REST API'''
	#era_res = requests.get('https://geoglows.ecmwf.int/api/v2/forecastrecords/{0}?start_date=20250502'.format(comid), verify=False).content
	#simulated_df = pd.read_csv(io.StringIO(era_res.decode('utf-8')), index_col=0)
	simulated_df = get_forecast_records(comid, "20250401")
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	simulated_df.to_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v2\\Forecast_Record\\{0}.csv".format(comid))

	observed_satellite_wl = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Observed_Hydroweb\\{0}.csv'.format(id), index_col=0)
	observed_satellite_wl.index = pd.to_datetime(observed_satellite_wl.index)
	observed_satellite_wl.index = observed_satellite_wl.index.to_series().dt.strftime("%Y-%m-%d")
	observed_satellite_wl.index = pd.to_datetime(observed_satellite_wl.index)

	min_value = observed_satellite_wl['Water Level (m)'].min()
	if min_value >= 0:
		min_value = 0
	observed_adjusted = observed_satellite_wl - min_value

	simulated_streamflow = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Simulated_Data\\GEOGLOWS_v2\\{0}.csv'.format(comid), index_col=0)
	simulated_streamflow.index = pd.to_datetime(simulated_streamflow.index)
	simulated_streamflow.index = simulated_streamflow.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_streamflow.index = pd.to_datetime(simulated_streamflow.index)

	rperiods_q = return_period_values(simulated_streamflow, comid)
	rperiods_q.to_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v2\\Return_Periods\\{}.csv".format(comid))

	corrected_values = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\DWLT\\GEOGLOWS_v2\\{0}-{1}_WL.csv'.format(id, comid), index_col=0)
	corrected_values.index = pd.to_datetime(corrected_values.index)
	corrected_values[corrected_values < 0] = 0
	corrected_values.index = corrected_values.index.to_series().dt.strftime("%Y-%m-%d")
	corrected_values.index = pd.to_datetime(corrected_values.index)

	rperiods_wl = return_period_values(corrected_values, comid)
	rperiods_wl.to_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v2\\Return_Periods\\{}_WL.csv".format(comid))

	#Forecast DWLT
	date_ini = simulated_df.index[0]
	month_ini = date_ini.month

	date_end = simulated_df.index[-1]
	month_end = date_end.month
	meses = np.arange(month_ini, month_end + 1, 1)

	fixed_records_sbwl = pd.DataFrame()

	for mes in meses:
		values = simulated_df.loc[simulated_df.index.month == mes]
		fixed_records_df = values.copy()

		corrected_values = fix_forecast(sim_hist=simulated_streamflow, fore_nofix=fixed_records_df, obs=observed_adjusted)
		fixed_records_sbwl = pd.concat([fixed_records_sbwl, corrected_values])

	fixed_records_sbwl.sort_index(inplace=True)
	fixed_records_sbwl = fixed_records_sbwl + min_value
	column = fixed_records_sbwl.columns[0]
	fixed_records_sbwl.rename(columns={column: "average_water_level"}, inplace = True)

	fixed_records_sbwl.to_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v2\\Forecast_Record\\{0}_WL.csv".format(comid))

	era_res = requests.get('https://geoglows.ecmwf.int/api/v2/forecastensemble/{0}?format=csv'.format(comid), verify=False).content
	forecast_df = pd.read_csv(io.StringIO(era_res.decode('utf-8')), index_col=0)
	forecast_df.index = pd.to_datetime(forecast_df.index)
	forecast_df.index = forecast_df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
	forecast_df.index = pd.to_datetime(forecast_df.index)
	forecast_df.to_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v2\\Forecast\\{0}.csv".format(comid))

	forecast_stats_df = forecast_stats(forecast_df)
	forecast_stats_df*-.to_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v2\\Forecast_Stats\\{0}.csv".format(comid))

	corrected_ensembles_sbwl = fix_forecast(sim_hist=simulated_streamflow, fore_nofix=forecast_df, obs=observed_adjusted)
	corrected_ensembles_sbwl = corrected_ensembles_sbwl + min_value
	corrected_ensembles_sbwl.to_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v2\\Forecast\\{0}_WL.csv".format(comid))
	forecast_stats_wl = forecast_stats(corrected_ensembles_sbwl)
	forecast_stats_wl.to_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v2\\Forecast_Stats\\{0}_WL.csv".format(comid))


