import pandas as pd
import os
import csv
from csv import writer as csv_writer


df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Stations_Selected_Colombia.csv')

IDs = df['Codigo'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()

for name, comid in zip(Names, COMIDs):

	data = pd.read_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Historical/Simulated_Data/ERA_5/Hourly/{0}_{1}.csv".format(comid,name), index_col=0)

	data.index = pd.to_datetime(data.index)

	daily_df = data.groupby(data.index.strftime("%Y/%m/%d")).mean()
	daily_df.index = pd.to_datetime(daily_df.index)
	print(daily_df)

	daily_df.to_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Historical/Simulated_Data/ERA_5/Daily/{0}_{1}.csv".format(comid,name), index_label="Datetime")