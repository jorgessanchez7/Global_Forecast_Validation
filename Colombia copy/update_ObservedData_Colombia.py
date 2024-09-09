import pandas as pd
import numpy as np

'''
df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/IDEAM_Stations_v3.csv')

IDs = df['ID'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Name'].tolist()
Rivers = df['Stream_Nam'].tolist()
'''

#df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/list_1.csv')
df = pd.read_csv('/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/list_2.csv')

IDs = df['ID'].tolist()


stations = []

for id in IDs:

	data_new = pd.read_csv("/Users/student/Github/Bias_Correction/Colombia/Updated/{0}.csv".format(id), index_col=0)
	dates_data_new = data_new.index.tolist()

	#data_old = pd.read_csv("/Users/student/Github/Bias_Correction/Colombia/Observed/{0}.csv".format(id), index_col=0)
	#data_old = pd.read_csv("/Users/student/Dropbox/PhD/2020 Winter/Dissertation_v9/South_America/Colombia/Historical/Observed_Data/Streamflow/{0}.csv".format(id), index_col=0)
	data_old = pd.read_csv('/Users/student/Dropbox/PhD/2019 Winter/Dissertation_v5/GetData_Colombia/Daily_Data/Q/obs/{}_Q_obs.csv'.format(id), index_col=0)
	dates_data_old = data_old.index.tolist()

	if (dates_data_new[len(dates_data_new)-1] < dates_data_old[len(dates_data_old)-1]):

		print(id)
		stations.append(id)
		print(dates_data_new[len(dates_data_new) - 1])
		print(dates_data_old[len(dates_data_old) - 1])

print(stations)