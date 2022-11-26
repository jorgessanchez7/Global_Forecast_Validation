import numpy as np
import pandas as pd

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Central_America/GRDC/Selected_Stations_GRDC_Q_v0.csv')

IDs = stations_pd['grdc_no'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['station'].tolist()


for id, name, comid in zip(IDs, Names, COMIDs):

	comid = int(comid)

	print(id, ' - ', name, ' - ', comid)

	#Observed Data
	df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Central_America/GRDC/data/historical/Observed_Data_Row/{}.csv'.format(id), index_col=0)
	df[df < 0] = np.nan
	df.index = pd.to_datetime(df.index)
	observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
	observed_df.index = pd.to_datetime(observed_df.index)

	observed_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Central_America/GRDC/data/historical/Observed_Data/{}.csv'.format(id))