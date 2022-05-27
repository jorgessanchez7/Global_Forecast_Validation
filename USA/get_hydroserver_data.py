import pandas as pd

#Water Level
stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/USA/Selected_Stations_USA_WL_v0.csv')

IDs = stations_pd['STAID'].tolist()
COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['STANAME'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	if id < 10000000:
		station_id = '0' + str(id)
	else:
		station_id = str(id)

	print(station_id, ' - ', name, ' - ', comid, '- WL')

	waterLevel = pd.read_csv("/Volumes/GoogleDrive/My Drive/MSc_Darlly_Rojas/2022_Winter/CE EN 699R - Master's Thesis/Bias-Correction_Water-Level-Data/USA/{}.csv".format(station_id), index_col=0)
	waterLevel.index = pd.to_datetime(waterLevel.index)
	waterLevel.index = waterLevel.index.to_series().dt.strftime("%Y-%m-%d")
	waterLevel.index = pd.to_datetime(waterLevel.index)
	waterLevel.dropna(inplace=True)
	waterLevel.rename(columns={"Water Level (cm)": "DataValue"}, inplace=True)
	waterLevel.index.name = 'LocalDateTime'

	waterLevel.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/data_test_Canada_and_USA/{}_WL.csv'.format(id))

print('Done with Water Levels!')
print('')

#Streamflow
stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/USA/Selected_Stations_USA_Q_v0.csv')

IDs = stations_pd['STAID'].tolist()
COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['STANAME'].tolist()


for id, name, comid in zip(IDs, Names, COMIDs):

	if id < 10000000:
		station_id = '0' + str(id)
	else:
		station_id = str(id)

	print(station_id, ' - ', name, ' - ', comid, '- Q')

	streamflow = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/USA/data/historical/Observed_Data/{}.csv'.format(station_id), index_col=0)
	streamflow.index = pd.to_datetime(streamflow.index)
	streamflow.index = streamflow.index.to_series().dt.strftime("%Y-%m-%d")
	streamflow.index = pd.to_datetime(streamflow.index)
	streamflow.dropna(inplace=True)
	streamflow.rename(columns={"Streamflow (m3/s)": "DataValue"}, inplace=True)
	streamflow.index.name = 'LocalDateTime'

	streamflow.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/data_test_Canada_and_USA/{}_Q.csv'.format(id))

print('Done with Streamflows!')
print('')