import geoglows
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/Hydroweb_v3.csv')

COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()

for name, comid in zip(Names, COMIDs):

	print(name, ' - ', comid)

	#Simulated Data
	simulated_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Simulated_Data/{}.csv'.format(comid), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df = simulated_df.loc[simulated_df.index >= pd.to_datetime('1980-01-01')]

	datasets = ['max', 'mean', 'min']

	for dataset in datasets:

		# Observed Data
		df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/{0}/{1}_{0}.csv'.format(dataset, name), index_col=0)
		#df[df < 0] = 0
		df.index = pd.to_datetime(df.index)
		observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
		observed_df.index = pd.to_datetime(observed_df.index)

		min_value = observed_df['Water Level (m)'].min()

		if min_value >= 0:
			min_value = 0

		observed_adjusted = observed_df - min_value

		#Getting the Bias Corrected Simulation
		corrected_adjusted = geoglows.bias.correct_historical(simulated_df, observed_adjusted)
		corrected_df = corrected_adjusted + min_value
		corrected_df.index.name = 'Datetime'
		corrected_df.rename(columns={'Corrected Simulated Streamflow': 'Simulated Water Level (m)'}, inplace=True)
		corrected_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Corrected_Data/{0}/{1}-{2}_{0}.csv'.format(dataset, name, comid))