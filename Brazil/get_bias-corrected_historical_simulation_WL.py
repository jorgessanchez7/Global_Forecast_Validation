import geoglows
import pandas as pd

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Brazil/Selected_Stations_Brazil_WL_v1.csv')

IDs = stations_pd['CodEstacao'].tolist()
COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['NomeEstaca'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	comid = int(comid)

	print(id, ' - ', name, ' - ', comid)

	#Observed Data
	df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Brazil/data/historical/Observed_Data_WL/{}.csv'.format(id), index_col=0)
	df.index = pd.to_datetime(df.index)
	observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)

	min_value = observed_df['Water Level (cm)'].min()

	if min_value >= 0:
		min_value = 0

	observed_adjusted = observed_df - min_value

	#Simulated Data
	simulated_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Brazil/data/historical/Simulated_Data_WL/{}.csv'.format(comid), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	simulated_df = simulated_df.loc[simulated_df.index >= pd.to_datetime('1980-01-01')]

	#Getting the Bias Corrected Simulation
	corrected_adjusted = geoglows.bias.correct_historical(simulated_df, observed_adjusted)
	corrected_df = corrected_adjusted + min_value
	corrected_df.rename(columns={'Corrected Simulated Streamflow': 'Simulated Water Level (cm)'}, inplace=True)
	corrected_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Brazil/data/historical/Corrected_Data_WL/{0}-{1}.csv'.format(id, comid))