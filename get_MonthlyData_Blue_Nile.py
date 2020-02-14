import pandas as pd
import os
import csv
from csv import writer as csv_writer


df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Blue_Nile_Stations.csv')

COMIDs = df['COMID'].tolist()
Names = df['Station'].tolist()
Rivers = df['Stream'].tolist()

files = ['ERA_5', 'ERA_Interim']

for name, comid in zip(Names, COMIDs):

	for file in files:

		data = pd.read_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/simulated_data/{0}/Daily/{1}_{2}.csv".format(file, comid, name), index_col=0)

		data.index = pd.to_datetime(data.index)

		monthly_df = data.groupby(data.index.strftime("%Y/%m")).mean()
		monthly_df.index = pd.to_datetime(monthly_df.index)
		print(monthly_df)

		monthly_df.to_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/Africa/Blue_Nile/Data/Historical/simulated_data/{0}/Monthly/{1}_{2}.csv".format(file, comid, name), index_label="Datetime")
