import json
import requests
import pandas as pd
import numpy as np
import datetime as dt

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/Total_Stations_Australia_Q.csv')

CODEs = stations_pd['Code'].tolist()
IDs = stations_pd['ts_id'].tolist()
#IDs = stations_pd['ts_id_water_level'].tolist() #water level
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()


for id, code, name, comid in zip(IDs, CODEs, Names, COMIDs):

	print(id, ' - ', code, ' - ', name, ' - ', comid)

	now = dt.datetime.now()
	yyyy = str(now.year)
	mm = str(now.month)
	dd = str(now.day)

	url = 'http://www.bom.gov.au/waterdata/services?service=kisters&type=queryServices&request=getTimeseriesValues&datasource=0&format=dajson&ts_id={0}&from=1900-01-01T00:00:00.000%2B09:30&to={1}-{2}-{3}T00:00:00.000%2B09:30&metadata=true&useprecision=false&timezone=GMT%2B09:30&md_returnfields=ts_id,ts_precision&userId=pub'.format(id, yyyy, mm, dd)

	#print(url)

	f = requests.get(url, verify=False)

	if f.status_code == 200:

		data = f.json()
		data = data[0]
		data = json.dumps(data)
		data = json.loads(data)
		data = (data.get('data'))

		datesObservedDischarge = [row[0] for row in data]
		observedDischarge = [row[1] for row in data]

		dates = []
		discharge = []

		for i in range(0, len(datesObservedDischarge) - 1):
			year = int(datesObservedDischarge[i][0:4])
			month = int(datesObservedDischarge[i][5:7])
			day = int(datesObservedDischarge[i][8:10])
			hh = int(datesObservedDischarge[i][11:13])
			mm = int(datesObservedDischarge[i][14:16])
			dates.append(dt.datetime(year, month, day, hh, mm))
			discharge.append(observedDischarge[i])

		datesObservedDischarge = dates
		observedDischarge = discharge

		# convert request into pandas DF
		pairs = [list(a) for a in zip(datesObservedDischarge, observedDischarge)]
		df1 = pd.DataFrame(pairs, columns=['Datetime', 'Streamflow (m3/s)'])
		df1.set_index('Datetime', inplace=True)

		df1 = df1.groupby(df1.index.strftime("%Y/%m/%d")).mean()
		df1.index = pd.to_datetime(df1.index)

		# Removing Negative Values
		df1[df1 < 0] = 0

		while np.isnan(df1.iloc[:, 0].values[0]):
			df1 = df1.iloc[1:]

		df1.dropna(inplace=True)
		#print(df1)

		datesObservedDischarge = pd.date_range(df1.index[0], df1.index[len(df1.index) - 1], freq='D')
		df2 = pd.DataFrame(np.nan, index=datesObservedDischarge, columns=['Streamflow (m3/s)'])
		df2.index.name = 'Datetime'

		#print(df2)

		df3 = df2.fillna(df1)

		#print(df3)

		df3.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/data/historical/Observed_Data/{}.csv'.format(code))