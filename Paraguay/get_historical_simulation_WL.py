import io
import os
import xarray
import geoglows
import requests
import pandas as pd
import netCDF4 as nc
import datetime as dt

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Paraguay/Selected_Stations_Paraguay_WL_v0.csv')

IDs = stations_pd['ID'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Estacion'].tolist()

#def get_units_title(unit_type):
#	"""
#	Get the title for units
#	"""
#	if unit_type == 'metric':
#		return 'm', 'meters'
#	elif unit_type == 'english':
#		return 'ft', 'feet'

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, '-', name, ' - ', comid)

	'''Using GEOGloWS Package'''
	simulated_df = geoglows.streamflow.historic_simulation(int(comid), forcing='era_5', return_format='csv')

	'''Using REST API'''
	#era_res =  requests.get('https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id=' + str(comid) + '&return_format=csv', verify=False).content
	#simulated_df = pd.read_csv(io.StringIO(era_res.decode('utf-8')), index_col=0)

	'''Reading the netcdf file'''
	#unit_type = 'metric'

	#date_ini = dt.datetime(1979,1,1)
	#date_end = dt.datetime(2021,11,30)

	#fechas = pd.date_range(date_ini, date_end, freq='D')

	#qout_nc = nc.Dataset('/Volumes/GoogleDrive/My Drive/ECMWF/ECMWF_GEOGloWS_Streamflow/rapid-io/output/output/era5/south_america-geoglows/Qout_era5_t640_24hr_19790101to20211130.nc')

	#try:
	#
	#	values = qout_nc['Qout'][:, list(qout_nc['rivid'][:]).index(comid)]
	#
	#	pairs = [list(a) for a in zip(fechas, values)]
	#	pairs = sorted(pairs, key=lambda x: x[0])
	#
	#	simulated_df = pd.DataFrame(pairs, columns=['datetime', 'streamflow_m^3/s'])
	#	simulated_df.set_index('datetime', inplace=True)
	#
	#	qout_nc.close()
	#except Exception as e:
	#	qout_nc.close()
	#	raise e

	# Removing Negative Values
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	simulated_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Paraguay/data/historical/Simulated_Data_WL/{}.csv'.format(comid))