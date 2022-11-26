import pandas as pd
import datetime as dt
import numpy as np

IDs = ['SW34', 'SW62', 'SW110', 'SW131.5', 'SW140', 'SW168', 'SW172', 'SW201', 'SW251', 'SW263', 'SW291.5R']

for id in IDs:

	hourly_df = pd.read_csv("/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_Asia/Bangladesh/data/row_data/hourly/{0}.csv".format(id), index_col=0)
	hourly_df.index = pd.to_datetime(hourly_df.index)

	daily_df = hourly_df.groupby(hourly_df.index.strftime("%Y-%m-%d")).mean()
	daily_df.index = pd.to_datetime(daily_df.index)
	print(daily_df)

	daily_df.to_csv("/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_Asia/Bangladesh/data/row_data/daily/{0}.csv".format(id), index_label="Datetime")
