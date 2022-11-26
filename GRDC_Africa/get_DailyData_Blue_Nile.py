import pandas as pd
import os
import csv
from csv import writer as csv_writer


df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Blue_Nile_Stations.csv')

COMIDs = df['COMID'].tolist()
Names = df['Station'].tolist()
Rivers = df['Stream'].tolist()

for name, comid in zip(Names, COMIDs):

	data = pd.read_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/simulated_data/ERA_5/Hourly/{0}_{1}.csv".format(comid,name), index_col=0)

	data.index = pd.to_datetime(data.index)

	daily_df = data.groupby(data.index.strftime("%Y/%m/%d")).mean()
	daily_df.index = pd.to_datetime(daily_df.index)
	print(daily_df)

	daily_df.to_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/simulated_data/ERA_5/Daily/{0}_{1}.csv".format(comid,name), index_label="Datetime")