import pandas as pd

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Peru/Total_Stations_Peru_Q_v0.csv')

IDs = stations_pd['id_Estacio'].tolist()
CODEs = stations_pd['Codigo'].tolist()
COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['Estacion'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	observed_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2020_Fall/Dissertation_v10/South_America/Peru/Data/observed/{0}_{1}.csv'.format(id, name), index_col=0)
	observed_df[observed_df < 0] = 0
	observed_df.index = pd.to_datetime(observed_df.index)
	observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
	observed_df.index = pd.to_datetime(observed_df.index)

	observed_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Peru/data/historical/Observed_Data/{}.csv'.format(id))