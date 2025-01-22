import numpy as np
import pandas as pd

stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Observed_Data/Europe/GRDC/GRDC_Stations_Spain_WL.csv')

CODEs = stations_pd['samplingFeatureCode'].tolist()

for code in CODEs:

	print(code)

	df1 = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/Europe/GRDC/{0}_WL.csv'.format(code), index_col=0)
	df1.index = pd.to_datetime(df1.index)
	df1.index = df1.index.to_series().dt.strftime("%Y-%m-%d")
	df1.index = pd.to_datetime(df1.index)
	df1.dropna(inplace=True)
	df1.rename(columns={"value": "Water Level (m)"}, inplace=True)
	df1.index.names = ['Datetime']

	print(df1)