import pandas as pd
import numpy as np


df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/IDEAM_Stations_v3.csv')

IDs = df['ID'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Name'].tolist()
Rivers = df['Stream_Nam'].tolist()

data = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/Colombia.csv')
data.rename(columns={'Fecha': 'Datetime'}, inplace=True)
data.set_index(['Datetime'], inplace=True, drop=True)
data.index = pd.to_datetime(data.index)

for id in IDs:

	print(id)
	station_data = data.loc[data['CodigoEstacion'] == id]
	station_data = station_data.drop(['CodigoEstacion', 'NombreEstacion', 'Latitud', 'Longitud', 'Altitud', 'Departamento', 'Municipio'], axis=1)
	station_data.rename(columns={'Valor': 'Streamflow (m3/s)'}, inplace=True)

	index = pd.date_range(station_data.index[0], station_data.index[len(station_data.index) - 1], freq='D')
	data_nan = [np.nan] * len(index)
	pairs = [list(a) for a in zip(index, data_nan)]
	df2 = pd.DataFrame(pairs, columns=['Datetime', 'Values'])
	df2.set_index(['Datetime'], inplace=True, drop=True)

	result = pd.concat([df2, station_data], axis=1, sort=False)
	result = result.drop(['Values'], axis=1)

	result.to_csv("/Users/student/Github/Bias_Correction/Colombia/Updated/{0}.csv".format(id))

print('Terminado')