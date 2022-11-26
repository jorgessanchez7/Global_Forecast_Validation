import pandas as pd
import requests
from io import StringIO
from os import path

df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Stations_Selected_Colombia.csv')

IDs = df['Codigo'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()

'''Get Simulated Data'''

# Define watershed parameters 
watershed = 'South America'
subbasin = 'Continental'
tethys_token = '4248936a4f5a45e2f551f91ead1085f3cbc8f465'

simulated_dir = '/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Historical/Simulated_Data/ERA_Interim'

for name, comid in zip(Names, COMIDs):
	request_params = dict(watershed_name=watershed, subbasin_name=subbasin, reach_id=comid, return_format='csv')
	request_headers = dict(Authorization='Token ' + tethys_token)
	res = requests.get('http://tethys2.byu.edu/apps/streamflow-prediction-tool/api/GetHistoricData/', params=request_params, headers=request_headers)
	csv = res.content
	csv = csv.decode('utf-8')
	csvfile = path.join("{0}_{1}.csv".format(comid, name))
	data = StringIO(csv)
	df_data = pd.read_csv(data, sep=',', header=None, names=['Streamflow (m3/s)'], index_col=0, infer_datetime_format=True, skiprows=1)
	df_data.to_csv(path.join(simulated_dir, csvfile), sep=',', index_label='Datetime')
	print("{0}_{1}".format(comid, name))