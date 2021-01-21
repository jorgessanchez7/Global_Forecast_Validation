import pandas as pd
import datetime as dt
import numpy as np


df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Fall/Dissertation_v10/South_Asia/Bangladesh/Bangladesh_Stations.csv')

IDs = df['Code'].tolist()
COMIDs = df['new_COMID'].tolist()
Names = df['Station'].tolist()
Rivers = df['River'].tolist()

df2 = pd.read_csv('/Users/student/Dropbox/PhD/2020 Fall/Dissertation_v10/South_Asia/Bangladesh/Bangladesh_Stations_RT.csv')

IDs_RT = df2['Code'].tolist()
COMIDs_RT = df2['new_COMID'].tolist()
Names_RT = df2['Station'].tolist()
Rivers_RT = df2['River'].tolist()

for name, id in zip(Names, IDs):

	data = pd.read_csv("/Users/student/Dropbox/PhD/2019 Winter/Dissertation_v5/Bangladesh/Historical_Data/{0}.csv".format(name), index_col=0)
	data.index = pd.to_datetime(data.index)

	dates = pd.date_range(data.index[0], data.index[len(data.index)-1], freq='D')

	df = pd.DataFrame(np.nan, index=dates, columns=['Streamflow (m3/s)'])
	df.index.name = 'Datetime'
	df2 = df.fillna(data)
	data = df2.copy()
	print(data)

	if name in Names_RT:

		hourly_df = pd.read_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_Asia/Bangladesh/Data/recent_streamflow/hourly/{0}.csv".format(name), index_col=0)
		hourly_df.index = pd.to_datetime(hourly_df.index)

		daily_df = hourly_df.groupby(hourly_df.index.strftime("%Y-%m-%d")).mean()
		daily_df.index = pd.to_datetime(daily_df.index)
		print(daily_df)

		daily_df.to_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_Asia/Bangladesh/Data/recent_streamflow/daily/{0}.csv".format(name), index_label="Datetime")

		observed_df = data.fillna(daily_df)
		observed_df.index = pd.to_datetime(observed_df.index)

	else:
		observed_df = data.copy()

	observed_df.to_csv('/Users/student/Dropbox/PhD/2020 Fall/Dissertation_v10/South_Asia/Bangladesh/Historical/observed_data/{}.csv')
