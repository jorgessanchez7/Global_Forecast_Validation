import random
import urllib
import requests
import pandas as pd

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Europe/GRDC_Stations_Europe.csv')

IDs = stations_pd['grdc_no'].tolist()
Names = stations_pd['station'].tolist()
Rivers = stations_pd['river'].tolist()
Years_ini = stations_pd['d_start'].tolist()
Years_end = stations_pd['d_end'].tolist()

ts_paths = [-999, 42]


user_agent_list = [
	'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
	'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
	'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582',
]

for id, name, river, y_ini, y_end in zip(IDs, Names, Rivers, Years_ini, Years_end):

	print(id, ' - ', name, ' - ', river)

	if (y_end - y_ini) < 10:

		for ts_path in ts_paths:

			print (ts_path)

			try:

				# Pick a random user agent
				user_agent = random.choice(user_agent_list)

				# Set the headers
				headers = {'User-Agent': user_agent}

				url = 'https://portal.grdc.bafg.de/KiWebPortal/rest/wiski7/dataSources/grdc/timeSeries/data?params=%7B%22from%22%3A%22{0}-12-29T00%3A00%3A00Z%22%2C%22returnfields%22%3A%22Timestamp%2CValue%22%2C%22to%22%3A%22{1}-01-03T00%3A00%3A00Z%22%2C%22ts_path%22%3A%22{2}%2F{3}%2FQ%2FDay.Cmd%22%2C%22valuesasstring%22%3Atrue%7D'.format(y_ini-1, y_end+1, ts_path, id)
				print(url)
				table = requests.get(url, headers=headers)
				print(table.text)

				f = urllib.request.urlopen(url)
				myfile = f.read()
				print(myfile)

				table_df = pd.read_html(table.text, skiprows=3)
				print('Tables found:', len(table_df))
				print(table_df)

			except Exception as e:

				print(e)