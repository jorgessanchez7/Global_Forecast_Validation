import pandas as pd
import numpy as np
import datetime as dt
from itertools import *

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/Canada/Selected_Stations_Canada_Q_v12.csv')

IDs = stations_pd['STATION_NU'].tolist()
#COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['STATION_NA'].tolist()

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

	#df3.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/Canada/data/historical/Observed_Data/{}.csv'.format(id))
	df3.to_csv('/Users/student/tethysdev/historical_validation_tool_canada/tethysapp/historical_validation_tool_canada/workspaces/app_workspace/Discharge_Data/{}.csv'.format(id))