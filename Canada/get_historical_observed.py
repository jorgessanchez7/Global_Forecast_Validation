import pandas as pd
import numpy as np
import datetime as dt
from itertools import *

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/Canada/Selected_Stations_Canada_Q_v1.csv')

IDs = stations_pd['STATION_NUMBER'].tolist()
#COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['STATION_NAME'].tolist()

'''
df = pd.read_csv(r'/Volumes/GoogleDrive/My Drive/PhD/2021_Fall/Dissertation_v12/North_America/Canada/streamflow.csv',encoding='unicode_escape')


print(df)

df.drop(columns=['Unnamed: 0', 'MONTHLY_MEAN', 'MONTHLY_TOTAL', 'FIRST_DAY_MIN', 'MIN', 'FIRST_DAY_MAX', 'MAX', 'FLOW_SYMBOL1', 'FLOW_SYMBOL2'], inplace=True)
df.drop(columns=['FLOW_SYMBOL3', 'FLOW_SYMBOL4', 'FLOW_SYMBOL5', 'FLOW_SYMBOL6', 'FLOW_SYMBOL7', 'FLOW_SYMBOL8', 'FLOW_SYMBOL9', 'FLOW_SYMBOL10'], inplace=True)
df.drop(columns=['FLOW_SYMBOL11', 'FLOW_SYMBOL12', 'FLOW_SYMBOL13', 'FLOW_SYMBOL14', 'FLOW_SYMBOL15', 'FLOW_SYMBOL16', 'FLOW_SYMBOL17'], inplace=True)
df.drop(columns=['FLOW_SYMBOL18', 'FLOW_SYMBOL19', 'FLOW_SYMBOL20', 'FLOW_SYMBOL21', 'FLOW_SYMBOL22', 'FLOW_SYMBOL23', 'FLOW_SYMBOL24'], inplace=True)
df.drop(columns=['FLOW_SYMBOL25', 'FLOW_SYMBOL26', 'FLOW_SYMBOL27', 'FLOW_SYMBOL28', 'FLOW_SYMBOL29', 'FLOW_SYMBOL30', 'FLOW_SYMBOL31'], inplace=True)

df.set_index(['STATION_NUMBER'], inplace=True)

print(df)

df.to_csv(r'/Volumes/GoogleDrive/My Drive/PhD/2021_Fall/Dissertation_v12/North_America/Canada/streamflow_v2.csv')
'''

df = pd.read_csv(r'/Volumes/GoogleDrive/My Drive/PhD/2021_Fall/Dissertation_v12/North_America/Canada/streamflow_v2.csv',encoding='unicode_escape')


for id, name in zip(IDs, Names):

	print(id, ' - ', name)

	data = df[df['STATION_NUMBER'] == id]

	# Get years, months, and parameters from dataset
	YEARs = data['YEAR'].tolist()
	MONTHs = data['MONTH'].tolist()

	dates = []
	values = []
	long_months = [1, 3, 5, 7, 8, 10, 12]
	short_months = [4, 6, 9, 11]

	for year, month in zip(YEARs, MONTHs):
		# Make a list of days depending on length of month
		if month in long_months:  # 31 day month
			days = list(range(1, 32))
		elif month in short_months:  # 30 day month
			days = list(range(1, 31))
		elif month == 2:  # February
			year = int(year)
			if ((year % 100 != 0) and (year % 4 == 0)):  # Leap year, 29 days
				days = list(range(1, 30))
			else:  # Non leap year, 28 days
				days = list(range(1, 29))

		# Add current month's dates to datetime list
		for day in days:
			dates.append(dt.datetime(year, month, day))

		# Get dataset for just the current month and year
		year = int(year)
		year_df = data[data['YEAR'] == year]
		month = int(month)
		month_df = year_df[year_df['MONTH'] == month]

		# Get a list of the streamflow values for the current month
		month_values = month_df.iloc[:, 5:len(days) + 5]
		#month_values = month_values.replace(-1, np.NaN)
		month_values = month_values.values

		# Add current month's data to list
		for value in month_values:
			values.append(value)

	# Convert multi-dimensional list to 1D list
	values = chain.from_iterable(values)

	# Create dataframe from values and datetime lists, and export to .csv
	final_df = pd.DataFrame(values, index=dates, columns=['Streamflow (m3/s)'])
	final_df.index.name = 'Datetime'
	while np.isnan(final_df.iloc[:, 0].values[0]):
		final_df = final_df.iloc[1:]
	while np.isnan(final_df.iloc[:, 0].values[len(final_df.iloc[:, 0].values) - 1]):
		final_df = final_df.iloc[:len(final_df.iloc[:, 0].values) - 2]

	datesObservedDischarge = pd.date_range(final_df.index[0], final_df.index[len(final_df.index) - 1], freq='D')
	df2 = pd.DataFrame(np.nan, index=datesObservedDischarge, columns=['Streamflow (m3/s)'])
	df2.index.name = 'Datetime'

	#print(df2)

	df3 = df2.fillna(final_df)

	#print(df3)

	df3.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/Canada/data/historical/Observed_Data/{}.csv'.format(id))