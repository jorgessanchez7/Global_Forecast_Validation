import geoglows
import pandas as pd

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD (1)/2022_Winter/Dissertation_v13/South_America/Venezuela/Selected_Stations_Venezuela_WL_v0.csv')

IDs = stations_pd['SERIAL'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['ESTACION'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	#Observed Data
	df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD (1)/2022_Winter/Dissertation_v13/South_America/Venezuela/data/historical/Observed_Data_WL/{}.csv'.format(id), index_col=0)
	df[df < 0] = 0
	df.index = pd.to_datetime(df.index)
	observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)

	#Simulated Data
	simulated_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD (1)/2022_Winter/Dissertation_v13/South_America/Venezuela/data/historical/Simulated_Data_WL/{}.csv'.format(comid), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	#Getting the Bias Corrected Simulation
	corrected_df = geoglows.bias.correct_historical(simulated_df, observed_df)

	corrected_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD (1)/2022_Winter/Dissertation_v13/South_America/Venezuela/data/historical/Corrected_Data_WL/{0}-{1}.csv'.format(id, comid))
