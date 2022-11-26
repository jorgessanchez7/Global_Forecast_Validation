import geoglows
import requests
import io
import pandas as pd
import datetime as dt

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/USA/Selected_Stations_USA_WL_v0.csv')

IDs = stations_pd['STAID'].tolist()
COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['STANAME'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	if id < 10000000:
		station_id = '0' + str(id)
	else:
		station_id = str(id)

	print(id, ' - ', name, ' - ', comid)

	'''Using GEOGloWS Package'''
	simulated_df = geoglows.streamflow.historic_simulation(comid, forcing='era_5', return_format='csv')

	'''Using REST API'''
	#era_res =  requests.get('https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id=' + str(comid) + '&return_format=csv', verify=False).content
	#simulated_df = pd.read_csv(io.StringIO(era_res.decode('utf-8')), index_col=0)

	# Removing Negative Values
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	simulated_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/USA/data/historical/Simulated_Data_WL/{}.csv'.format(comid))