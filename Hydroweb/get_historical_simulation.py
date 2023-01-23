import io
import requests
import geoglows
import pandas as pd
import datetime as dt

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/Hydroweb_v3.csv')
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()

for name, comid in zip(Names, COMIDs):

	print(name, ' - ', comid)

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

	simulated_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Simulated_Data/{}.csv'.format(comid))