import pandas as pd

df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Stations_Selected_Colombia_Recent1_WL.csv')

IDs = df['Codigo'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()


for id, name, river in zip(IDs, Names, Rivers):

	print (id, '-', name, '-', river)

	data_historic = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/his/{}_historic_observed_wl.csv'.format(id), index_col=0)

	datesHistoric = data_historic.index.tolist()
	valuesHistoric = data_historic.iloc[:, 0].values
	valuesHistoric.tolist()

	data_rec1 = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/rec_1/daily/{0}_real_time_observed_wl.csv'.format(id), index_col=0)

	dates_rec1 = data_rec1.index.tolist()
	values_rec1 = data_rec1.iloc[:, 0].values
	values_rec1.tolist()

	datesTemp = []
	valuesTemp = []

	if (len(datesHistoric) == 0):
		for k in range(0, len(dates_rec1)):
			datesTemp.append(dates_rec1[k])
			valuesTemp.append(values_rec1[k])
	else:
		for j in range(0, len(datesHistoric)):
			if (len(dates_rec1) == 0):
				datesTemp.append(datesHistoric[j])
				valuesTemp.append(valuesHistoric[j])
			elif (dates_rec1[0] == datesHistoric[j]):
				for k in range(0, len(dates_rec1)):
					datesTemp.append(dates_rec1[k])
					valuesTemp.append(values_rec1[k])
				break
			elif (datesHistoric[len(datesHistoric) - 1] < dates_rec1[0]):
				for k in range(0, len(datesHistoric)):
					datesTemp.append(datesHistoric[k])
					valuesTemp.append(valuesHistoric[k])
				for k in range(0, len(dates_rec1)):
					datesTemp.append(dates_rec1[k])
					valuesTemp.append(values_rec1[k])
				break
			else:
				datesTemp.append(datesHistoric[j])
				valuesTemp.append(valuesHistoric[j])

	dates_historic = datesTemp
	values_historic = valuesTemp

	data_rec2 = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/row_data/rec_2/daily/{0}_real_time_observed_wl.csv'.format(id), index_col=0)

	dates_rec2 = data_rec2.index.tolist()
	values_rec2 = data_rec2.iloc[:, 0].values
	values_rec2.tolist()

	datesTemp = []
	valuesTemp = []

	if (len(dates_historic) == 0):
		for k in range(0, len(dates_rec2)):
			datesTemp.append(dates_rec2[k])
			valuesTemp.append(values_rec2[k])
	else:
		for j in range(0, len(dates_historic)):
			if (len(dates_rec2) == 0):
				datesTemp.append(dates_historic[j])
				valuesTemp.append(values_historic[j])
			elif (dates_rec2[0] == dates_historic[j]):
				for k in range(0, len(dates_rec2)):
					datesTemp.append(dates_rec2[k])
					valuesTemp.append(values_rec2[k])
				break
			elif (dates_historic[len(dates_historic) - 1] < dates_rec2[0]):
				for k in range(0, len(dates_historic)):
					datesTemp.append(dates_historic[k])
					valuesTemp.append(values_historic[k])
				for k in range(0, len(dates_rec2)):
					datesTemp.append(dates_rec2[k])
					valuesTemp.append(values_rec2[k])
				break
			else:
				datesTemp.append(dates_historic[j])
				valuesTemp.append(values_historic[j])

	pairs = [list(a) for a in zip(datesTemp, valuesTemp)]

	pd.DataFrame(pairs, columns=['Datetime', 'Streamflow (m3/s)']).to_csv(
		'/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Historical/Observed_Data/Water_Level/{0}.csv'.format(
			id), encoding='utf-8', header=True, index=0)

	pd.DataFrame(pairs, columns=['Datetime', 'Streamflow (m3/s)']).to_csv(
		'/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Forecast/Observed_Data/Water_Level/{0}.csv'.format(
			id), encoding='utf-8', header=True, index=0)