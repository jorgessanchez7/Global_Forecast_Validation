import pandas as pd
import os
import csv
from csv import writer as csv_writer


df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/North_America/USA/United_States_Stations.csv')

COMIDs = df['COMID'].tolist()
Names = df['Station Name'].tolist()
IDs = df['Code'].tolist()
Rivers = df['Stream'].tolist()

for name, id, comid in zip(Names, IDs, COMIDs):
	if id/10000000 < 1:
		data = pd.read_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/North_America/USA/Historical/Observed_Data/English_Units/0{0}_{1}.csv".format(id,name), index_col=0)
		data.index = pd.to_datetime(data.index)
	else:
		data = pd.read_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/North_America/USA/Historical/Observed_Data/English_Units/{0}_{1}.csv".format(id, name), index_col=0)
		data.index = pd.to_datetime(data.index)

	data["Streamflow (cfs)"] = 0.0283168 * data["Streamflow (cfs)"]
	data.rename(columns={"Streamflow (cfs)": "Streamflow (m3/s)"}, inplace=True)

	if id / 10000000 < 1:
		data.to_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/North_America/USA/Historical/Observed_Data/SI/0{0}_{1}.csv".format(id,name), index_label="Datetime")
	else:
		data.to_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/North_America/USA/Historical/Observed_Data/SI/{0}_{1}.csv".format(id, name), index_label="Datetime")