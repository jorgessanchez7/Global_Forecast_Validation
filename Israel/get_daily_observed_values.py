import pandas as pd

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Middle_East/Israel/Selected_Stations_Israel_Q_v0.csv')

IDs = stations_pd['statid'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Name'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	observed_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Middle_East/Israel/data/historical/Observed_Data_/{}.csv'.format(id), index_col=0)
	observed_df.index = pd.to_datetime(observed_df.index)
	obs_df = observed_df.groupby(observed_df.index.strftime("%Y-%m-%d")).mean()
	obs_df.index = pd.to_datetime(obs_df.index)
	obs_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Middle_East/Israel/data/historical/Observed_Data/{}.csv'.format(id))
