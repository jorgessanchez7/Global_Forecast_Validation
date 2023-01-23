import pandas as pd

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/Total_Stations_Australia_Q.csv')

CODEs = stations_pd['Code'].tolist()
IDs = stations_pd['ts_id'].tolist()
#IDs = stations_pd['ts_id_water_level'].tolist() #water level
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()


for id, code, name, comid in zip(IDs, CODEs, Names, COMIDs):

	print(id, ' - ', code, ' - ', name, ' - ', comid)

	observed = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/data/historical/Observed_Data/{}.csv'.format(code), index_col=0)
	observed.index = pd.to_datetime(observed.index)
	observed.index = observed.index.to_series().dt.strftime("%Y-%m-%d")
	observed.index = pd.to_datetime(observed.index)
	observed.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/data/historical/Observed_Data/{}.csv'.format(id))