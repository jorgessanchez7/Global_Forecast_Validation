import pandas as pd
import os
import csv
from csv import writer as csv_writer


df = pd.read_csv(r'/Volumes/FILES/global_streamflow_backup/reforecasts/South_America_csv/FEWS_Stations_Q.csv')

spt_id = df['id'].tolist()
names = df['name'].tolist()
rivers = df['stream'].tolist()
stations = df['COMID'].tolist()

for station, river, name, spt in zip(stations, rivers, names, spt_id):

	print(spt, name, river, station)

	data = pd.read_csv("/Volumes/FILES/global_streamflow_backup/reforecasts/South_America_csv/Qobs_12hr/{0}_Q_obs.csv".format(spt), index_col=0)

	data.index = pd.to_datetime(data.index)

	daily_df = data.groupby(data.index.strftime("%Y/%m/%d")).mean()
	daily_df.index = pd.to_datetime(daily_df.index)
	print(daily_df)

	daily_df.to_csv("/Users/student/Desktop/output/South_America_csv/{0}/{1}.csv".format(spt, spt), index_label="Datetime")