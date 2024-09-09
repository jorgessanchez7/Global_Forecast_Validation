import pandas as pd
import os

df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Stations_Selected_Colombia_Recent1.csv')

IDs = df['Codigo'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()


'''Get Observed Data'''
observed_dir = '/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/rec_1/hourly'

daily_observed_dir = '/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/rec_1/daily'

dischargeData = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/CAUDAL-DHIME.csv', index_col=9)
dischargeData.index = pd.to_datetime(dischargeData.index)
fechas = dischargeData.index.tolist()
estaciones = dischargeData['CodigoEstacion'].tolist()
valores = dischargeData['Valor'].tolist()



for id in IDs:

	discharge = []
	dates = []

	for i in range (0, len(estaciones)):
		if (id == estaciones[i]):
			discharge.append(valores[i])
			dates.append(fechas[i])

	pairs = [list(a) for a in zip(dates, discharge)]

	if not os.path.exists(observed_dir):
		os.mkdir(observed_dir)

	pd.DataFrame(pairs, columns=['Datetime', 'oberved streamflow (m3/s)']).to_csv(observed_dir + "/{}_real_time_observed.csv".format(id), encoding='utf-8', header=True, index=0)

	data = pd.read_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/rec_1/hourly/{0}_real_time_observed.csv".format(id), index_col=0)

	data.index = pd.to_datetime(data.index)

	daily_df = data.groupby(data.index.strftime("%Y/%m/%d")).mean()
	daily_df.index = pd.to_datetime(daily_df.index)

	if not os.path.exists(daily_observed_dir):
		os.mkdir(daily_observed_dir)

	daily_df.to_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/rec_1/daily/{0}_real_time_observed.csv".format(id), index_label="Datetime")

	print("{}_real_time_observed.csv".format(id))