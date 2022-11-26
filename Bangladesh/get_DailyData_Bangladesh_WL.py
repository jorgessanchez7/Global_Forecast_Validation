import pandas as pd
import datetime as dt
import numpy as np

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_Asia/Bangladesh/Selected_Stations_Bangladesh_WL_v0.csv')

IDs = stations_pd['Code'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()

for id in IDs:

	hourly_df = pd.read_csv("/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_Asia/Bangladesh/data/row_data/hourly_WL/{0}.csv".format(id), index_col=0)
	hourly_df.index = pd.to_datetime(hourly_df.index)

	daily_df = hourly_df.groupby(hourly_df.index.strftime("%Y-%m-%d")).mean()
	daily_df.index = pd.to_datetime(daily_df.index)
	print(daily_df)

	daily_df.to_csv("/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_Asia/Bangladesh/data/row_data/daily_WL/{0}.csv".format(id), index_label="Datetime")
