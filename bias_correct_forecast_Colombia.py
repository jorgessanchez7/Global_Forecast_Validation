import os
import math
import warnings
import numpy as np
import pandas as pd
from scipy import interpolate


def _flow_and_probability_mapper(monthly_data: pd.DataFrame, to_probability: bool = False,
                                 to_flow: bool = False, extrapolate: bool = False) -> interpolate.interp1d:
    if not to_flow and not to_probability:
        raise ValueError('You need to specify either to_probability or to_flow as True')

    # get maximum value to bound histogram
    max_val = math.ceil(np.max(monthly_data.max()))
    min_val = math.floor(np.min(monthly_data.min()))

    if max_val == min_val:
        warnings.warn('The observational data has the same max and min value. You may get unanticipated results.')
        max_val += .1

    # determine number of histograms bins needed
    number_of_points = len(monthly_data.values)
    number_of_classes = math.ceil(1 + (3.322 * math.log10(number_of_points)))

    # specify the bin width for histogram (in m3/s)
    step_width = (max_val - min_val) / number_of_classes

    # specify histogram bins
    bins = np.arange(-np.min(step_width), max_val + 2 * np.min(step_width), np.min(step_width))

    if bins[0] == 0:
        bins = np.concatenate((-bins[1], bins))
    elif bins[0] > 0:
        bins = np.concatenate((-bins[0], bins))

    # make the histogram
    counts, bin_edges = np.histogram(monthly_data, bins=bins)

    # adjust the bins to be the center
    bin_edges = bin_edges[1:]

    # normalize the histograms
    counts = counts.astype(float) / monthly_data.size

    # calculate the cdfs
    cdf = np.cumsum(counts)

    # interpolated function to convert simulated streamflow to prob
    if to_probability:
        if extrapolate:
            return interpolate.interp1d(bin_edges, cdf, fill_value='extrapolate')
        return interpolate.interp1d(bin_edges, cdf)
    # interpolated function to convert simulated prob to observed streamflow
    elif to_flow:
        if extrapolate:
            return interpolate.interp1d(cdf, bin_edges, fill_value='extrapolate')
        return interpolate.interp1d(cdf, bin_edges)

df = pd.read_csv('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Stations_Selected_Colombia_v2.csv')

IDs = df['Codigo'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()

obsFiles = []
simFiles = []
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

for id, comid, name in zip(IDs, COMIDs, Names):
	obsFiles.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Observed_Data/Streamflow/' + str(id) + '.csv')
	simFiles.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Historical/Simulated_Data/ERA_5/Daily/' + str(comid) + '_' + str(name) + '.csv')
	initializationFiles.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + 'Initialization_Values.csv')
	forecastFiles_1Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '1_Day_Forecasts.csv')
	forecastFiles_2Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '2_Day_Forecasts.csv')
	forecastFiles_3Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '3_Day_Forecasts.csv')
	forecastFiles_4Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '4_Day_Forecasts.csv')
	forecastFiles_5Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '5_Day_Forecasts.csv')
	forecastFiles_6Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '6_Day_Forecasts.csv')
	forecastFiles_7Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '7_Day_Forecasts.csv')
	forecastFiles_8Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '8_Day_Forecasts.csv')
	forecastFiles_9Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '9_Day_Forecasts.csv')
	forecastFiles_10Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '10_Day_Forecasts.csv')
	forecastFiles_11Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '11_Day_Forecasts.csv')
	forecastFiles_12Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '12_Day_Forecasts.csv')
	forecastFiles_13Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '13_Day_Forecasts.csv')
	forecastFiles_14Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '14_Day_Forecasts.csv')
	forecastFiles_15Day.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '15_Day_Forecasts.csv')
	forecastFiles_1Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '1_Day_Forecasts_High_Res.csv')
	forecastFiles_2Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '2_Day_Forecasts_High_Res.csv')
	forecastFiles_3Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '3_Day_Forecasts_High_Res.csv')
	forecastFiles_4Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '4_Day_Forecasts_High_Res.csv')
	forecastFiles_5Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '5_Day_Forecasts_High_Res.csv')
	forecastFiles_6Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '6_Day_Forecasts_High_Res.csv')
	forecastFiles_7Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '7_Day_Forecasts_High_Res.csv')
	forecastFiles_8Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '8_Day_Forecasts_High_Res.csv')
	forecastFiles_9Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '9_Day_Forecasts_High_Res.csv')
	forecastFiles_10Day_HR.append('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Forecast/Simulated_Data/{0}-{1}/'.format(id, name) + '10_Day_Forecasts_High_Res.csv')

# Making directories for all the Stations

# User Input
country = 'Colombia'
typeOfData = 'Observed_Values'
output_dir = '/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/Bias_Corrected_Forecast/'

# Making directory
if not os.path.isdir(output_dir):
	os.makedirs(output_dir)

forecastDaysAhead = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15']
forecastDaysAhead_HR = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']


# for id, comid, name, rio, obsFile, simFile, initializationFile, forecastFile_1Day, forecastFile_2Day, forecastFile_3Day, forecastFile_4Day, forecastFile_5Day, forecastFile_6Day, forecastFile_7Day, forecastFile_8Day, forecastFile_9Day, forecastFile_10Day, forecastFile_11Day, forecastFile_12Day, forecastFile_13Day, forecastFile_14Day, forecastFile_15Day, forecastFile_1Day_HR, forecastFile_2Day_HR, forecastFile_3Day_HR, forecastFile_4Day_HR, forecastFile_5Day_HR, forecastFile_6Day_HR, forecastFile_7Day_HR, forecastFile_8Day_HR, forecastFile_9Day_HR, forecastFile_10Day_HR in zip(IDs, COMIDs, Names, Rivers, obsFiles, simFiles, initializationFiles, forecastFiles_1Day, forecastFiles_2Day, forecastFiles_3Day, forecastFiles_4Day, forecastFiles_5Day, forecastFiles_6Day, forecastFiles_7Day, forecastFiles_8Day, forecastFiles_9Day, forecastFiles_10Day, forecastFiles_11Day, forecastFiles_12Day, forecastFiles_13Day, forecastFiles_14Day, forecastFiles_15Day, forecastFiles_1Day_HR, forecastFiles_2Day_HR, forecastFiles_3Day_HR, forecastFiles_4Day_HR, forecastFiles_5Day_HR, forecastFiles_6Day_HR, forecastFiles_7Day_HR, forecastFiles_8Day_HR, forecastFiles_9Day_HR, forecastFiles_10Day_HR):
# 	print(id, comid, name, rio, 'Initialization')
#
# 	'''Historic Observed Values'''
# 	observed_df = pd.read_csv(obsFile, index_col=0)
# 	observed_df[observed_df < 0] = 0
# 	observed_df.index = pd.to_datetime(observed_df.index)
# 	observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
# 	observed_df.index = pd.to_datetime(observed_df.index)
#
# 	'''Historic Simulated Values'''
# 	simulated_df = pd.read_csv(simFile, index_col=0)
# 	simulated_df[simulated_df < 0] = 0
# 	simulated_df.index = pd.to_datetime(simulated_df.index)
# 	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
# 	simulated_df.index = pd.to_datetime(simulated_df.index)
#
# 	'''Initialization Values'''
# 	initialization_df = pd.read_csv(initializationFile, index_col=0)
# 	initialization_df[initialization_df < 0] = 0
# 	initialization_df.index = pd.to_datetime(initialization_df.index)
# 	initialization_df.index = initialization_df.index.to_series().dt.strftime("%Y-%m-%d")
# 	initialization_df.index = pd.to_datetime(initialization_df.index)
#
# 	'''Bias Corrected Initialization'''
#
# 	corrected_initialization = pd.DataFrame()
#
# 	meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
#
# 	for mes in meses:
# 		monthly_initialization = initialization_df[initialization_df.index.month == mes].dropna()
# 		monthly_simulated = simulated_df[simulated_df.index.month == mes].dropna()
# 		monthly_observed = observed_df[observed_df.index.month == mes].dropna()
# 		to_prob = _flow_and_probability_mapper(monthly_simulated, to_probability=True, extrapolate=True)
# 		to_flow = _flow_and_probability_mapper(monthly_observed, to_flow=True, extrapolate=True)
#
# 		min_simulated = np.min(monthly_simulated.iloc[:,0].to_list())
# 		max_simulated = np.max(monthly_simulated.iloc[:, 0].to_list())
#
# 		for column in monthly_initialization.columns:
# 			tmp = monthly_initialization[column].dropna().to_frame()
# 			min_factor = tmp.copy()
# 			max_factor = tmp.copy()
# 			min_factor.loc[min_factor[column] >= min_simulated, column] = 1
# 			min_index_value = tmp[tmp[column] < min_simulated].index.tolist()
# 			for element in min_index_value:
# 				min_factor[column].loc[min_factor.index == element] = tmp[column].loc[tmp.index == element] / min_simulated
# 			max_factor.loc[max_factor[column] <= max_simulated, column] = 1
# 			max_index_value = tmp[tmp[column] > max_simulated].index.tolist()
# 			for element in max_index_value:
# 				max_factor[column].loc[max_factor.index == element] = tmp[column].loc[tmp.index == element] / max_simulated
# 			tmp.loc[tmp[column] <= min_simulated, column] = min_simulated
# 			tmp.loc[tmp[column] >= max_simulated, column] = max_simulated
# 			monthly_initialization.update(pd.DataFrame(to_flow(to_prob(tmp[column].values)), index=tmp.index, columns=[column]))
# 			monthly_initialization = monthly_initialization.multiply(min_factor[column], axis=0)
# 			monthly_initialization = monthly_initialization.multiply(max_factor[column], axis=0)
#
#
# 		corrected_initialization = corrected_initialization.append(monthly_initialization)
# 	corrected_initialization.sort_index(inplace=True)
#
# 	ouput_folder = os.path.join(output_dir, 'Simulated_Data/{0}-{1}'.format(id, name))
# 	if not os.path.isdir(ouput_folder):
# 		os.makedirs(ouput_folder)
#
# 	corrected_initialization.to_csv(ouput_folder + '/Initialization_Values_BC.csv')

# for daysAhead in forecastDaysAhead_HR:
#
# 	for id, comid, name, rio, obsFile, simFile, initializationFile, forecastFile_1Day, forecastFile_2Day, forecastFile_3Day, forecastFile_4Day, forecastFile_5Day, forecastFile_6Day, forecastFile_7Day, forecastFile_8Day, forecastFile_9Day, forecastFile_10Day, forecastFile_11Day, forecastFile_12Day, forecastFile_13Day, forecastFile_14Day, forecastFile_15Day, forecastFile_1Day_HR, forecastFile_2Day_HR, forecastFile_3Day_HR, forecastFile_4Day_HR, forecastFile_5Day_HR, forecastFile_6Day_HR, forecastFile_7Day_HR, forecastFile_8Day_HR, forecastFile_9Day_HR, forecastFile_10Day_HR in zip(IDs, COMIDs, Names, Rivers, obsFiles, simFiles, initializationFiles, forecastFiles_1Day, forecastFiles_2Day, forecastFiles_3Day, forecastFiles_4Day, forecastFiles_5Day, forecastFiles_6Day, forecastFiles_7Day, forecastFiles_8Day, forecastFiles_9Day, forecastFiles_10Day, forecastFiles_11Day, forecastFiles_12Day, forecastFiles_13Day, forecastFiles_14Day, forecastFiles_15Day, forecastFiles_1Day_HR, forecastFiles_2Day_HR, forecastFiles_3Day_HR, forecastFiles_4Day_HR, forecastFiles_5Day_HR, forecastFiles_6Day_HR, forecastFiles_7Day_HR, forecastFiles_8Day_HR, forecastFiles_9Day_HR, forecastFiles_10Day_HR):
#
# 		print(id, comid, name, rio, '{}_Day_Forecast_HR'.format(daysAhead))
#
# 		'''Historic Observed Values'''
# 		observed_df = pd.read_csv(obsFile, index_col=0)
# 		observed_df[observed_df < 0] = 0
# 		observed_df.index = pd.to_datetime(observed_df.index)
# 		observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
# 		observed_df.index = pd.to_datetime(observed_df.index)
#
# 		'''Historic Simulated Values'''
# 		simulated_df = pd.read_csv(simFile, index_col=0)
# 		simulated_df[simulated_df < 0] = 0
# 		simulated_df.index = pd.to_datetime(simulated_df.index)
# 		simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
# 		simulated_df.index = pd.to_datetime(simulated_df.index)
#
# 		'''Forecast Values (High Res)'''
# 		forecast_hr_df = pd.read_csv(globals()['forecastFile_{}Day_HR'.format(daysAhead)], index_col=0)
# 		forecast_hr_df[forecast_hr_df < 0] = 0
# 		forecast_hr_df.index = pd.to_datetime(forecast_hr_df.index)
# 		forecast_hr_df.index = forecast_hr_df.index.to_series().dt.strftime("%Y-%m-%d")
# 		forecast_hr_df.index = pd.to_datetime(forecast_hr_df.index)
#
# 		'''Bias Corrected Forecast Values (High Res)'''
#
# 		corrected_forecast_hr = pd.DataFrame()
#
# 		meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
#
# 		for mes in meses:
# 			monthly_forecast_hr = forecast_hr_df[forecast_hr_df.index.month == mes].dropna()
# 			monthly_simulated = simulated_df[simulated_df.index.month == mes].dropna()
# 			monthly_observed = observed_df[observed_df.index.month == mes].dropna()
# 			to_prob = _flow_and_probability_mapper(monthly_simulated, to_probability=True, extrapolate=True)
# 			to_flow = _flow_and_probability_mapper(monthly_observed, to_flow=True, extrapolate=True)
#
# 			min_simulated = np.min(monthly_simulated.iloc[:, 0].to_list())
# 			max_simulated = np.max(monthly_simulated.iloc[:, 0].to_list())
#
# 			for column in monthly_forecast_hr.columns:
# 				tmp = monthly_forecast_hr[column].dropna().to_frame()
# 				min_factor = tmp.copy()
# 				max_factor = tmp.copy()
# 				min_factor.loc[min_factor[column] >= min_simulated, column] = 1
# 				min_index_value = tmp[tmp[column] < min_simulated].index.tolist()
# 				for element in min_index_value:
# 					min_factor[column].loc[min_factor.index == element] = tmp[column].loc[tmp.index == element] / min_simulated
# 				max_factor.loc[max_factor[column] <= max_simulated, column] = 1
# 				max_index_value = tmp[tmp[column] > max_simulated].index.tolist()
# 				for element in max_index_value:
# 					max_factor[column].loc[max_factor.index == element] = tmp[column].loc[tmp.index == element] / max_simulated
# 				tmp.loc[tmp[column] <= min_simulated, column] = min_simulated
# 				tmp.loc[tmp[column] >= max_simulated, column] = max_simulated
# 				monthly_forecast_hr.update(pd.DataFrame(to_flow(to_prob(tmp[column].values)), index=tmp.index, columns=[column]))
# 				monthly_forecast_hr = monthly_forecast_hr.multiply(min_factor[column], axis=0)
# 				monthly_forecast_hr = monthly_forecast_hr.multiply(max_factor[column], axis=0)
#
# 			corrected_forecast_hr = corrected_forecast_hr.append(monthly_forecast_hr)
#
# 		corrected_forecast_hr.sort_index(inplace=True)
#
# 		ouput_folder = os.path.join(output_dir, 'Simulated_Data/{0}-{1}'.format(id, name))
# 		if not os.path.isdir(ouput_folder):
# 			os.makedirs(ouput_folder)
#
# 		corrected_forecast_hr.to_csv(ouput_folder + '/{}_Day_Forecasts_High_Res_BC.csv'.format(daysAhead))

for daysAhead in forecastDaysAhead:

	for id, comid, name, rio, obsFile, simFile, initializationFile, forecastFile_1Day, forecastFile_2Day, forecastFile_3Day, forecastFile_4Day, forecastFile_5Day, forecastFile_6Day, forecastFile_7Day, forecastFile_8Day, forecastFile_9Day, forecastFile_10Day, forecastFile_11Day, forecastFile_12Day, forecastFile_13Day, forecastFile_14Day, forecastFile_15Day, forecastFile_1Day_HR, forecastFile_2Day_HR, forecastFile_3Day_HR, forecastFile_4Day_HR, forecastFile_5Day_HR, forecastFile_6Day_HR, forecastFile_7Day_HR, forecastFile_8Day_HR, forecastFile_9Day_HR, forecastFile_10Day_HR in zip(IDs, COMIDs, Names, Rivers, obsFiles, simFiles, initializationFiles, forecastFiles_1Day, forecastFiles_2Day, forecastFiles_3Day, forecastFiles_4Day, forecastFiles_5Day, forecastFiles_6Day, forecastFiles_7Day, forecastFiles_8Day, forecastFiles_9Day, forecastFiles_10Day, forecastFiles_11Day, forecastFiles_12Day, forecastFiles_13Day, forecastFiles_14Day, forecastFiles_15Day, forecastFiles_1Day_HR, forecastFiles_2Day_HR, forecastFiles_3Day_HR, forecastFiles_4Day_HR, forecastFiles_5Day_HR, forecastFiles_6Day_HR, forecastFiles_7Day_HR, forecastFiles_8Day_HR, forecastFiles_9Day_HR, forecastFiles_10Day_HR):

		print(id, comid, name, rio, '{}_Day_Forecast'.format(daysAhead))

		'''Observed Values'''
		observed_df = pd.read_csv(obsFile, index_col=0)
		observed_df[observed_df < 0] = 0
		observed_df.index = pd.to_datetime(observed_df.index)
		observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
		observed_df.index = pd.to_datetime(observed_df.index)

		'''Historic Simulated Values'''
		simulated_df = pd.read_csv(simFile, index_col=0)
		simulated_df[simulated_df < 0] = 0
		simulated_df.index = pd.to_datetime(simulated_df.index)
		simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
		simulated_df.index = pd.to_datetime(simulated_df.index)

		'''Ensemble Forecast Values'''
		forecast_df = pd.read_csv(globals()['forecastFile_{}Day'.format(daysAhead)], index_col=0)
		forecast_df[forecast_df < 0] = 0
		forecast_df.index = pd.to_datetime(forecast_df.index)
		forecast_df.index = forecast_df.index.to_series().dt.strftime("%Y-%m-%d")
		forecast_df.index = pd.to_datetime(forecast_df.index)

		'''Bias Corrected Forecast Values (High Res)'''

		corrected_forecast = pd.DataFrame()

		meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

		for mes in meses:
			monthly_forecast = forecast_df[forecast_df.index.month == mes].dropna()
			monthly_simulated = simulated_df[simulated_df.index.month == mes].dropna()
			monthly_observed = observed_df[observed_df.index.month == mes].dropna()
			to_prob = _flow_and_probability_mapper(monthly_simulated, to_probability=True, extrapolate=True)
			to_flow = _flow_and_probability_mapper(monthly_observed, to_flow=True, extrapolate=True)

			min_simulated = np.min(monthly_simulated.iloc[:, 0].to_list())
			max_simulated = np.max(monthly_simulated.iloc[:, 0].to_list())

			for column in monthly_forecast.columns:
				tmp = monthly_forecast[column].dropna().to_frame()
				min_factor = tmp.copy()
				max_factor = tmp.copy()
				min_factor.loc[min_factor[column] >= min_simulated, column] = 1
				min_index_value = min_factor[min_factor[column] != 1].index.tolist()
				#min_index_value = tmp[tmp[column] < min_simulated].index.tolist()
				for element in min_index_value:
					min_factor[column].loc[min_factor.index == element] = tmp[column].loc[tmp.index == element] / min_simulated
				max_factor.loc[max_factor[column] <= max_simulated, column] = 1
				max_index_value = max_factor[max_factor[column] != 1].index.tolist()
				#max_index_value = tmp[tmp[column] > max_simulated].index.tolist()
				for element in max_index_value:
					max_factor[column].loc[max_factor.index == element] = tmp[column].loc[tmp.index == element] / max_simulated
				tmp.loc[tmp[column] <= min_simulated, column] = min_simulated
				tmp.loc[tmp[column] >= max_simulated, column] = max_simulated
				tmp_bc = pd.DataFrame(to_flow(to_prob(tmp[column].values)), index=tmp.index, columns=[column])
				tmp_bc = tmp_bc.multiply(min_factor[column], axis=0)
				tmp_bc = tmp_bc.multiply(max_factor[column], axis=0)
				monthly_forecast.update(pd.DataFrame(tmp_bc[column].values, index=tmp_bc.index, columns=[column]))

			corrected_forecast = corrected_forecast.append(monthly_forecast)

		corrected_forecast.sort_index(inplace=True)

		ouput_folder = os.path.join(output_dir, 'Simulated_Data/{0}-{1}'.format(id, name))
		if not os.path.isdir(ouput_folder):
			os.makedirs(ouput_folder)

		corrected_forecast.to_csv(ouput_folder + '/{}_Day_Forecasts_BC.csv'.format(daysAhead))