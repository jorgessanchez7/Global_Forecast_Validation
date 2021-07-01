import pandas as pd
import requests
import io
import numpy as np
import geoglows


#df = pd.read_csv('/Users/student/Dropbox/PhD/2020_Winter/Dissertation_v9/South_America/Colombia/IDEAM_Stations_v2.csv')
df = pd.read_csv('C:\\Users\\jsanch3z\\Dropbox\\PhD\\2020_Winter\\Dissertation_v9\\South_America\\Colombia\\Stations_Selected_Colombia_v3.csv')

IDs = df['Codigo'].tolist()
#COMIDs = df['COMID'].tolist()
COMIDs = df['new_COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()

# #data = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/Excel_2021_06_03.csv')
# data = pd.read_csv('C:\\Users\\jsanch3z\\Dropbox\\PhD\\2020_Winter\\Dissertation_v9\\South_America\\Colombia\\row_data\\Excel_2021_06_03.csv')
# data.rename(columns={'Fecha': 'Datetime'}, inplace=True)
# data.set_index(['Datetime'], inplace=True, drop=True)
# data.index = pd.to_datetime(data.index)
#
# for id in IDs:
#
# 	print(id)
# 	station_data = data.loc[data['CodigoEstacion'] == id]
# 	station_data = station_data.drop(['CodigoEstacion', 'NombreEstacion', 'Latitud', 'Longitud', 'Altitud'], axis=1)
# 	station_data.rename(columns={'Valor': 'Streamflow (m3/s)'}, inplace=True)
#
# 	index = pd.date_range(station_data.index[0], station_data.index[len(station_data.index) - 1], freq='D')
# 	data_nan = [np.nan] * len(index)
# 	pairs = [list(a) for a in zip(index, data_nan)]
# 	df2 = pd.DataFrame(pairs, columns=['Datetime', 'Values'])
# 	df2.set_index(['Datetime'], inplace=True, drop=True)
#
# 	result = pd.concat([df2, station_data], axis=1, sort=False)
# 	result = result.drop(['Values'], axis=1)
#
# 	#result.to_csv("/Users/student/Github/Bias_Correction/Colombia/Updated/{0}.csv".format(id))
# 	result.to_csv("C:\\Users\\jsanch3z\\Dropbox\\PhD\\2020_Winter\\Dissertation_v9\\South_America\\Colombia\\Forecast\\Observed_Data\\Streamflow\\{0}.csv".format(id))
#
# print('Terminado con los observados')

for comid in COMIDs:

	print(comid)

	url = 'https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id={0}&return_format=csv'.format(comid)
	s = requests.get(url, verify=False).content
	simulated_df = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)

	#simulated_df = geoglows.streamflow.historic_simulation(comid, forcing='era_5', return_format='csv')
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	simulated_df.to_csv("C:\\Users\\jsanch3z\\Dropbox\\PhD\\2020_Winter\\Dissertation_v9\\South_America\\Colombia\\Historical\\Simulated_Data\\ERA_5\\{0}.csv".format(comid))

print('Terminado con los simulados')



