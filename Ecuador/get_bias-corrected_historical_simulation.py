import geoglows
import pandas as pd

stations_pd = pd.read_csv('/Users/student/Dropbox/PhD/2021_Fall/Dissertation_v12/South_America/Ecuador/Ecuador_Selected_Stations.csv')

IDs = stations_pd['Codigo'].tolist()
COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['Nombre_de'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	#Observed Data
	df = pd.read_csv('/Users/student/Dropbox/PhD/2021_Fall/Dissertation_v12/South_America/Ecuador/Historical/Observed_Data/{}.csv'.format(id), index_col=0)
	df[df < 0] = 0
	df.index = pd.to_datetime(df.index)
	observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)

	#Simulated Data
	simulated_df = pd.read_csv('/Users/student/Dropbox/PhD/2021_Fall/Dissertation_v12/South_America/Ecuador/Historical/Simulated_Data/{}.csv'.format(comid), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	#Getting the Bias Corrected Simulation
	corrected_df = geoglows.bias.correct_historical(simulated_df, observed_df)

	corrected_df.to_csv('/Users/student/Dropbox/PhD/2021_Fall/Dissertation_v12/South_America/Ecuador/Historical/Corrected_Data/{}.csv'.format(comid))






