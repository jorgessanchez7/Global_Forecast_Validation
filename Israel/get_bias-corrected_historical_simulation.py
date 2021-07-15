import geoglows
import pandas as pd

stations_pd = pd.read_csv('/Users/student/Dropbox/PhD/2021_Fall/Dissertation_v12/Middle_East/Israel/Israel_Selected_Stations.csv')

IDs = stations_pd['statid'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Name'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	#Observed Data
	df = pd.read_csv('/Users/student/Dropbox/PhD/2021_Fall/Dissertation_v12/Middle_East/Israel/Historical/Observed_Data/{}.csv'.format(id), index_col=0)
	df[df < 0] = 0
	df.index = pd.to_datetime(df.index)
	observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)

	#Simulated Data
	simulated_df = pd.read_csv('/Users/student/Dropbox/PhD/2021_Fall/Dissertation_v12/Middle_East/Israel/Historical/Simulated_Data/{}.csv'.format(comid), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	#Getting the Bias Corrected Simulation
	corrected_df = geoglows.bias.correct_historical(simulated_df, observed_df)

	corrected_df.to_csv('/Users/student/Dropbox/PhD/2021_Fall/Dissertation_v12/Middle_East/Israel/Historical/Corrected_Data/{}.csv'.format(comid))






