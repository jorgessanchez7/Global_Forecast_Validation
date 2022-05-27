import requests
import xmltodict
import pandas as pd
import numpy as np
import datetime as dt

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Central_America/Dominican_Republic/Selected_Stations_Dominican_Republic_Q_v0.csv')

IDs = stations_pd['Code'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Name'].tolist()


for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', ' - ', name, ' - ', comid)

	url = 'http://128.187.106.131/app/index.php/dr/services/cuahsi_1_1.asmx/GetValuesObject?location={0}&variable=Q&startDate=1900-01-01&endDate=2021-12-31&version=1.1'.format(id)

	r = requests.get(url, verify=False)
	c = xmltodict.parse(r.content)

	y = []
	x = []

	for i in c['timeSeriesResponse']['timeSeries']['values']['value']:
		y.append(float((i['#text'])))
		x.append(dt.datetime.strptime((i['@dateTime']), "%Y-%m-%dT%H:%M:%S"))

	df = pd.DataFrame(data=y, index=x, columns=['Streamflow'])
	df.head()

	datesDischarge = df.index.tolist()
	dataDischarge = df.iloc[:, 0].values

	if isinstance(dataDischarge[0], str):
		dataDischarge = map(float, dataDischarge)

	df1 = pd.DataFrame(data=dataDischarge, index=datesDischarge, columns=['Streamflow (m3/s)'])

	datesObservedDischarge = pd.date_range(df1.index[0], df1.index[len(df1.index) - 1], freq='D')
	df2 = pd.DataFrame(np.nan, index=datesObservedDischarge, columns=['Streamflow (m3/s)'])
	df2.index.name = 'Datetime'

	df3 = df2.fillna(df1)

	df3.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Central_America/Dominican_Republic/data/historical/Observed_Data/{}.csv'.format(id))