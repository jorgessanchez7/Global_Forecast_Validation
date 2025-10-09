import math
import geoglows
import numpy as np
import pandas as pd
from scipy import interpolate

import warnings
warnings.filterwarnings('ignore')

def correct_bias(simulated_data: pd.DataFrame, observed_data: pd.DataFrame):
	"""
	    Accepts a historically simulated flow timeseries and observed flow timeseries and attempts to correct biases in the
	    simulation on a monthly basis.
	    Args:
	        simulated_data: A dataframe with a datetime index and a single column of streamflow values
	        observed_data: A dataframe with a datetime index and a single column of streamflow values
	    Returns:
	        pandas DataFrame with a datetime index and a single column of streamflow values
	"""
	# list of the unique months in the historical simulation. should always be 1->12 but just in case...
	unique_simulation_months = sorted(set(simulated_data.index.strftime('%m')))
	dates = []
	values = []

	for month in unique_simulation_months:
		# filter historic data to only be current month
		monthly_simulated = simulated_data[simulated_data.index.month == int(month)].dropna()
		to_prob = _flow_and_probability_mapper(monthly_simulated, to_probability=True)
		# filter the observations to current month
		monthly_observed = observed_data[observed_data.index.month == int(month)].dropna()
		negative_values = monthly_observed[monthly_observed < 0]
		negative_values.dropna(inplace=True)
		'''
		if (len(negative_values.index) > 0):
			print(month)
			print(negative_values)
		'''
		to_flow = _flow_and_probability_mapper(monthly_observed, to_flow=True)

		#to_prob = _flow_and_probability_mapper(monthly_simulated, monthly_observed, to_probability=True)
		#to_flow = _flow_and_probability_mapper(monthly_simulated, monthly_observed, to_flow=True)

		dates += monthly_simulated.index.to_list()
		value = to_flow(to_prob(monthly_simulated.values))
		values += value.tolist()

	corrected = pd.DataFrame(data=values, index=dates, columns=['Corrected Simulated Streamflow'])
	corrected.sort_index(inplace=True)
	return corrected

def _flow_and_probability_mapper(monthly_data: pd.DataFrame, to_probability: bool = False, to_flow: bool = False, extrapolate: bool = False):
	if not to_flow and not to_probability:
		raise ValueError('You need to specify either to_probability or to_flow as True')

	monthly_data[monthly_data < 0] = np.nan
	monthly_data.dropna(inplace=True)

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
			func = interpolate.interp1d(bin_edges, cdf, fill_value='extrapolate')
		else:
			func = interpolate.interp1d(bin_edges, cdf)
		return lambda x: np.clip(func(x), 0, 1)
	# interpolated function to convert simulated prob to observed streamflow
	elif to_flow:
		if extrapolate:
			return interpolate.interp1d(cdf, bin_edges, fill_value='extrapolate')
		return interpolate.interp1d(cdf, bin_edges)

stations_pd = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\World_Stations.csv')
stations_pd = stations_pd[stations_pd['COMID_v2'] != 0]
stations_pd = stations_pd[stations_pd['Q'] == 'YES']

Folders = stations_pd['Folder'].tolist()
Sources = stations_pd['Data_Source'].tolist()
IDs = stations_pd['samplingFeatureCode'].tolist()
COMIDs = stations_pd['COMID_v2'].tolist()
Names = stations_pd['name'].tolist()

for id, name, comid, folder, source in zip(IDs, Names, COMIDs, Folders, Sources):

	print(id, ' - ', name, ' - ', comid, ' - Q')

	#Observed Data
	df = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_Q.csv'.format(folder, source, id), na_values=-9999, index_col=0)
	df.index = pd.to_datetime(df.index)
	observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)
	observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
	observed_df.index = pd.to_datetime(observed_df.index)

	#Simulated Data
	simulated_df = pd.read_csv('E:\\GEOGloWS\\01_Simulated_Values\\v2\\{}.csv'.format(comid), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	#Getting the Bias Corrected Simulation
	try:
		corrected_df = geoglows.bias.correct_historical(simulated_df, observed_df)
		#corrected_df = correct_bias(simulated_df, observed_df)
		corrected_df.to_csv('E:\\GEOGloWS\\02_Corrected_Data\\v2\\{0}-{1}_Q.csv'.format(id, comid))
	except Exception as e:
		print(e)

stations_pd = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\World_Stations.csv')
stations_pd = stations_pd[stations_pd['COMID_v2'] != 0]
stations_pd = stations_pd[stations_pd['WL'] == 'YES']

Folders = stations_pd['Folder'].tolist()
Sources = stations_pd['Data_Source'].tolist()
IDs = stations_pd['samplingFeatureCode'].tolist()
COMIDs = stations_pd['COMID_v2'].tolist()
Names = stations_pd['name'].tolist()

for id, name, comid, folder, source in zip(IDs, Names, COMIDs, Folders, Sources):

	print(id, ' - ', name, ' - ', comid, ' - WL')

	#Observed Data
	df = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_WL.csv'.format(folder, source, id), na_values=-9999, index_col=0)
	df.index = pd.to_datetime(df.index)
	observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)
	observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
	observed_df.index = pd.to_datetime(observed_df.index)

	min_value = observed_df[observed_df.columns[0]].min()

	if min_value >= 0:
		min_value = 0

	observed_adjusted = observed_df - min_value

	#Simulated Data
	simulated_df = pd.read_csv('E:\\GEOGloWS\\01_Simulated_Values\\v2\\{}.csv'.format(comid), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df = simulated_df.loc[simulated_df.index >= pd.to_datetime("1941-01-01")]

	#Getting the Bias Corrected Simulation
	try:
		corrected_df = geoglows.bias.correct_historical(simulated_df, observed_adjusted)
		#corrected_df = correct_bias(simulated_df, observed_adjusted)
		corrected_df = corrected_df + min_value
		corrected_df.to_csv('E:\\GEOGloWS\\03_Transformed_Data\\v2\\{0}-{1}_WL.csv'.format(id, comid))
	except Exception as e:
		print(e)