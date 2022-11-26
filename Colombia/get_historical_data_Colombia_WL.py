import pandas as pd

df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Stations_Selected_Colombia.csv')

IDs = df['Codigo'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()

'''Get Observed Data'''

observed_dir = '/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/his'

dischargeData = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/NIVEL.tr5.csv', index_col=1)
dischargeData.index = pd.to_datetime(dischargeData.index)
fechas = dischargeData.index.tolist()
estaciones = dischargeData['ESTACION'].tolist()
valores = dischargeData['VALOR'].tolist()


for id in IDs:

	discharge = []
	dates = []

	for i in range (0, len(estaciones)):
		if (id == estaciones[i]):
			discharge.append(valores[i])
			dates.append(fechas[i])

	pairs = [list(a) for a in zip(dates, discharge)]
	pd.DataFrame(pairs, columns= ['Datetime', 'Water Level (cm)']).to_csv(observed_dir + "/{}_historic_observed_wl.csv".format(id), encoding='utf-8', header=True, index=0)
	print("{}_historic_observed_wl.csv".format(id))