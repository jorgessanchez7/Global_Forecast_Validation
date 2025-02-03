import numpy as np
import pandas as pd

stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Stations.csv')
stations_pd = stations_pd.loc[stations_pd["Q"]=='YES']

UIDS = stations_pd['uid'].tolist()
CODEs = stations_pd['samplingFeatureCode'].tolist()
Names = stations_pd['name'].tolist()
Folders = stations_pd['Folder'].tolist()
DataSources = stations_pd['Data_Source'].tolist()
Countries = stations_pd['country_name'].tolist()

for uid, code, name, folder, data_source, country in zip(UIDS, CODEs, Names, Folders, DataSources, Countries):

	print(uid, ' - ', code, ' - ', name, ' - ', country)

	file_path = '/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_Q.csv'.format(folder, data_source, code)
	df1 = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_Q.csv'.format(folder, data_source, code), index_col=0)
	#file_path = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Hydroserver/Observed_Data/{0}/{1}/{2}_Q.csv'.format(folder, data_source, code)
	#df1 = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Hydroserver/Observed_Data/{0}/{1}/{2}_Q.csv'.format(folder, data_source, code), index_col=0)
	
	df1.index = pd.to_datetime(df1.index)
	df1.index = df1.index.to_series().dt.strftime("%Y-%m-%d")
	df1.index = pd.to_datetime(df1.index)

	# Check if any NaN values exist after reading with na_values
	if df1.isnull().values.any():
		# If NaN values are present, it means the file originally had empty spaces
		# Replace NaN with -9999
		df1 = df1.fillna(-9999)

		# Rewrite the CSV file with -9999 for missing values
		df1.to_csv(file_path, na_rep='-9999')  # na_rep ensures -9999 is used for NaN in the output

	#print(df1)