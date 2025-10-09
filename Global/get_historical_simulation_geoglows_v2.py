import io
import requests
import pandas as pd
import datetime as dt

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\World_Stations.csv')


stations_pd = stations_pd[stations_pd['COMID_v2'] != 0]
IDs = stations_pd['samplingFeatureCode'].tolist()
COMIDs = stations_pd['COMID_v2'].tolist()
Names = stations_pd['name'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	''''Using REST API'''
	era_res = requests.get('https://geoglows.ecmwf.int/api/v2/retrospectivedaily/{0}?format=csv'.format(comid), verify=False).content
	simulated_df = pd.read_csv(io.StringIO(era_res.decode('utf-8')), index_col=0)
	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	simulated_df.to_csv('E:\\GEOGloWS\\01_Simulated_Values\\v2\\{}.csv'.format(comid))