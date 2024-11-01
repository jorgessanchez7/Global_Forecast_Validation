import numpy as np
import pandas as pd

stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Observed_Data/Europe/GRDC/GRDC_Stations_Englands_WL.csv')

CODEs = stations_pd['CSV File Names'].tolist()
IDs = stations_pd['grdc_id'].tolist() #water level

for id, code in zip(IDs, CODEs):

	print(id, ' - ', code)

	df1 = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/flow-l/{0}.csv'.format(code), index_col=0)
	df1.index = pd.to_datetime(df1.index)
	df1.index = df1.index.to_series().dt.strftime("%Y-%m-%d")
	df1.index = pd.to_datetime(df1.index)
	df1.dropna(inplace=True)
	df1.rename(columns={"value": "Water Level (m)"}, inplace=True)
	df1.index.names = ['Datetime']

	#print(df1)

	datesObservedDischarge = pd.date_range(df1.index[0], df1.index[len(df1.index) - 1], freq='D')
	df2 = pd.DataFrame(np.nan, index=datesObservedDischarge, columns=['Water Level (m)'])
	df2.index.name = 'Datetime'

	#print(df2)

	df3 = df2.fillna(df1)

	#print(df3)

	df3.to_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/Europe/GRDC/data/historical/Observed_Data_WL/{}.csv'.format(id))

	df4 = df3.fillna(-9999)

	df4.to_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/Europe/GRDC/{0}_WL.csv'.format(id))

	#print(df4)