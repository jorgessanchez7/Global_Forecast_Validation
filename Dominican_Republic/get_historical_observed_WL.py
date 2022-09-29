import requests
import xmltodict
import pandas as pd
import numpy as np
import datetime as dt

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD (1)/2022_Winter/Dissertation_v13/Central_America/Dominican_Republic/Selected_Stations_Dominican_Republic_WL_v0.csv')

IDs = stations_pd['SiteCode'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['SiteName'].tolist()


for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', ' - ', name, ' - ', comid)

	url = 'http://128.187.106.131/app/index.php/dr/services/cuahsi_1_1.asmx/GetValuesObject?location={0}&variable=RES-EL&startDate=1900-01-01&endDate=2022-12-31&version=1.1'.format(id)
	print(url)

	r = requests.get(url, verify=False)
	c = xmltodict.parse(r.content)

	y = []
	x = []

	for i in c['timeSeriesResponse']['timeSeries']['values']['value']:
		y.append(float((i['#text'])))
		x.append(dt.datetime.strptime((i['@dateTime']), "%Y-%m-%dT%H:%M:%S"))

	df = pd.DataFrame(data=y, index=x, columns=['Water Level (m)'])
	df.head()

	datesDischarge = df.index.tolist()
	dataDischarge = df.iloc[:, 0].values

	if isinstance(dataDischarge[0], str):
		dataDischarge = map(float, dataDischarge)

	df1 = pd.DataFrame(data=dataDischarge, index=datesDischarge, columns=['Water Level (m)'])
	daily_df = df1.groupby(df1.index.strftime("%Y-%m-%d")).mean()
	daily_df.index = pd.to_datetime(daily_df.index)

	datesObservedDischarge = pd.date_range(df1.index[0], df1.index[len(df1.index) - 1], freq='D')
	df2 = pd.DataFrame(np.nan, index=datesObservedDischarge, columns=['Water Level (m)'])
	df2.index.name = 'Datetime'

	df3 = df2.fillna(daily_df)

	df3.to_csv('/Volumes/GoogleDrive/My Drive/PhD (1)/2022_Winter/Dissertation_v13/Central_America/Dominican_Republic/data/historical/Observed_Data_WL/{}.csv'.format(id))