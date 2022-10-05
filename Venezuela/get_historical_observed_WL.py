import numpy as np
import pandas as pd
import datetime as dt
from itertools import *

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD (1)/2022_Winter/Dissertation_v13/South_America/Venezuela/Total_Stations_Venezuela_WL_v0.csv')

IDs = stations_pd['SERIAL'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['ESTACION'].tolist()

#Read in streamflow data

df = pd.read_csv(r'/Volumes/GoogleDrive/My Drive/PhD (1)/2020_Winter/Dissertation_v9/South_America/Venezuela/NIVEL.csv',encoding='unicode_escape')

for id, name, comid in zip(IDs, Names, COMIDs):

	print(id, ' - ', name, ' - ', comid)

	# Get dataset for the current station
	id = int(id)
	data = df[df['SERIAL'] == id]

	# Get years, months, and parameters from dataset
	YEARs = data['ANO'].tolist()
	MONTHs = data['MES'].tolist()
	PARAs = data['PARA'].tolist()

	dates = []
	values = []
	long_months = [1, 3, 5, 7, 8, 10, 12]
	short_months = [4, 6, 9, 11]

	for year, month, para in zip(YEARs, MONTHs, PARAs):
		# Make a list of days depending on length of month
		if month in long_months:  # 31 day month
			days = list(range(1, 32))
		elif month in short_months:  # 30 day month
			days = list(range(1, 31))
		elif month == 2:  # February
			year = int(year)
			if year % 4 == 0:  # Leap year, 29 days
				days = list(range(1, 30))
			elif year % 4 != 0:  # Non leap year, 28 days
				days = list(range(1, 29))

		# Add current month's dates to datetime list
		for day in days:
			dates.append(dt.datetime(year, month, day))

		# Get dataset for just the current month and year
		year = int(year)
		year_df = data[data['ANO'] == year]
		month = int(month)
		month_df = year_df[year_df['MES'] == month]

		# Get a list of the streamflow values for the current month
		month_values = month_df.iloc[:, 5:len(days) + 5]
		month_values = month_values.replace(-1, np.NaN)
		month_values = month_values.values

		# Parameter conversion
		if para == 8150:
			month_values = month_values / 1000
		elif para == 8155:
			month_values = month_values * 10

		# Add current month's data to list
		for value in month_values:
			values.append(value)

	# Convert multi-dimensional list to 1D list
	values = chain.from_iterable(values)

	# Create dataframe from values and datetime lists, and export to .csv
	final_df = pd.DataFrame(values, index=dates, columns=['Water Level (cm)'])
	final_df.index.name = 'Datetime'
	while np.isnan(final_df.iloc[:, 0].values[0]):
		final_df = final_df.iloc[1:]
	while np.isnan(final_df.iloc[:, 0].values[len(final_df.iloc[:, 0].values) - 1]):
		final_df = final_df.iloc[:len(final_df.iloc[:, 0].values) - 2]

	datesObservedDischarge = pd.date_range(final_df.index[0], final_df.index[len(final_df.index) - 1], freq='D')
	df2 = pd.DataFrame(np.nan, index=datesObservedDischarge, columns=['Water Level (cm)'])
	df2.index.name = 'Datetime'

	#print(df2)

	df3 = df2.fillna(final_df)

	#print(df3)

	df3.to_csv('/Volumes/GoogleDrive/My Drive/PhD (1)/2022_Winter/Dissertation_v13/South_America/Venezuela/data/historical/Observed_Data_WL/{}.csv'.format(id))