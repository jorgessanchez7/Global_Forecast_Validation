import math
import geoglows
import numpy as np
import pandas as pd
from scipy import interpolate

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv('/Users/grad/Github/Global_Forecast_Validation/Global/World_Stations_WL_Sat.csv')

COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()

for name, comid in zip(Names, COMIDs):

	print(name, ' - ', comid, ' - WL (Sat)')

	#Observed Data
	observed_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Hydroweb/Data/{}.csv'.format(name), index_col=0)
	observed_df.index = pd.to_datetime(observed_df.index)
	observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
	observed_df.index = pd.to_datetime(observed_df.index)

	min_value = observed_df[observed_df.columns[0]].min()

	if min_value >= 0:
		min_value = 0

	observed_adjusted = observed_df - min_value

	#Simulated Data
	simulated_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Simulated_Data/GEOGLOWS_v1/{}.csv'.format(comid), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df = simulated_df.loc[simulated_df.index >= pd.to_datetime("1980-01-01")]

	#Getting the Bias Corrected Simulation
	try:
		corrected_df = geoglows.bias.correct_historical(simulated_df, observed_adjusted)
		corrected_df = corrected_df + min_value
		corrected_df.to_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Corrected_Data/Hydroweb-GEOGLOWS_v1/{0}-{1}_WL.csv'.format(name, comid))
	except Exception as e:
		print(e)
