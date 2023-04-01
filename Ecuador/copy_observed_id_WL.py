import pandas as pd

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Ecuador/Selected_Stations_Ecuador_WL_v0.csv')

IDs = stations_pd['Codigo'].tolist()
COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['Nombre_de'].tolist()


for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	observed = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Ecuador/data/historical/Observed_Data_WL/{}_WL.csv'.format(id), index_col=0)
	observed.index = pd.to_datetime(observed.index)
	observed.index = observed.index.to_series().dt.strftime("%Y-%m-%d")
	observed.index = pd.to_datetime(observed.index)
	observed.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Ecuador/data/historical/Observed_Data_WL/{}.csv'.format(id))