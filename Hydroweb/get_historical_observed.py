import io
import json
import geoglows
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/Hydroweb_v3.csv')

COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()

for name, comid in zip(Names, COMIDs):

	print(name, ' - ', comid)

	'''Using Web Scrapping'''
	name1 = name.upper()

	url = 'https://hydroweb.theia-land.fr/hydroweb/view/R_{}?lang=en'.format(name1)
	print(url)

	r = requests.get(url)
	html_doc = r.text

	soup1 = BeautifulSoup(html_doc, features='html.parser')

	dataValues = soup1.find_all('script')
	dataValues = dataValues[11]
	dataValues = dataValues.get_text()
	dataValues = dataValues.split()
	dataValues = dataValues[3]
	dataValues = dataValues[0:-1]
	dataValues = dataValues.split(':[')

	dates = '[' + dataValues[1][0:-4]
	dates = json.loads(dates)

	fechas = []
	for date in dates:
		fechas.append(datetime.fromisoformat(date))

	values = '[' + dataValues[2][0:-1]
	values = json.loads(values)

	mean_values = []
	min_values = []
	max_values = []

	for value in values:
		min_values.append(value[0] - value[1])
		mean_values.append(value[0])
		max_values.append(value[0] + value[1])

	pairs = [list(a) for a in zip(fechas, mean_values)]
	mean_wl = pd.DataFrame(pairs, columns=['Datetime', 'Water Level (m)'])
	mean_wl.set_index('Datetime', inplace=True)
	mean_wl.index = pd.to_datetime(mean_wl.index)

	mean_wl.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/mean/{}_mean.csv'.format(name))

	pairs = [list(a) for a in zip(fechas, min_values)]
	min_wl = pd.DataFrame(pairs, columns=['Datetime', 'Water Level (m)'])
	min_wl.set_index('Datetime', inplace=True)
	min_wl.index = pd.to_datetime(mean_wl.index)

	min_wl.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/min/{}_min.csv'.format(name))

	pairs = [list(a) for a in zip(fechas, max_values)]
	max_wl = pd.DataFrame(pairs, columns=['Datetime', 'Water Level (m)'])
	max_wl.set_index('Datetime', inplace=True)
	max_wl.index = pd.to_datetime(mean_wl.index)

	max_wl.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/max/{}_max.csv'.format(name))
