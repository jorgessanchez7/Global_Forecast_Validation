import geoglows
import pandas as pd

import warnings
warnings.filterwarnings('ignore')


stations_pd = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Hydroweb_Stations_row_SHP.csv')
stations_pd = stations_pd[stations_pd['COMID_v2'] != 0]

IDs = stations_pd['ID'].tolist()
COMIDs = stations_pd['COMID_v2'].tolist()
Names = stations_pd['Name'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid, ' - WL')

	#Observed Data
	df = pd.read_csv('E:\\GEOGloWS\\04_Observed_Satellite_Water_Levels\\Hydroweb\\{0}.csv'.format(id), index_col=0)
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
	simulated_df = pd.read_csv('E:\\GEOGloWS\\01_Simulated_Values\\hydroweb_v2\\{}.csv'.format(comid), index_col=0)
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
		corrected_df.to_csv('E:\\GEOGloWS\\03_Transformed_Data\\hydroweb_v2\\{0}-{1}_WL.csv'.format(id, comid))
	except Exception as e:
		print(e)
