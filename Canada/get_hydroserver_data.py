import pandas as pd

#Water Level
stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/Canada/Selected_Stations_Canada_WL_v0.csv')

IDs = stations_pd['STATION_NUMBER'].tolist()
Names = stations_pd['STATION_NAME'].tolist()

for id, name in zip(IDs, Names):

	print(id, ' - ', name, ' - ', '- WL')

	waterLevel = pd.read_csv("/Volumes/GoogleDrive/My Drive/MSc_Darlly_Rojas/2022_Winter/CE EN 699R - Master's Thesis/Bias-Correction_Water-Level-Data/Canada/{}.csv".format(id), index_col=0)
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
stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/Canada/Selected_Stations_Canada_Q_v0.csv')

IDs = stations_pd['STATION_NUMBER'].tolist()
Names = stations_pd['STATION_NAME'].tolist()

for id, name in zip(IDs, Names):

	print(id, ' - ', name, ' - ', '- Q')

	streamflow = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/Canada/data/historical/Observed_Data/{}.csv'.format(id), index_col=0)
	streamflow.index = pd.to_datetime(streamflow.index)
	streamflow.index = streamflow.index.to_series().dt.strftime("%Y-%m-%d")
	streamflow.index = pd.to_datetime(streamflow.index)
	streamflow.dropna(inplace=True)
	streamflow.rename(columns={"Streamflow (m3/s)": "DataValue"}, inplace=True)
	streamflow.index.name = 'LocalDateTime'

	streamflow.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/data_test_Canada_and_USA/{}_Q.csv'.format(id))

print('Done with Streamflows!')
print('')