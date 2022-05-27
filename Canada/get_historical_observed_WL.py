import pandas as pd
import numpy as np
import datetime as dt
from itertools import *

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/Canada/Selected_Stations_Canada_WL_v0.csv')

IDs = stations_pd['STATION_NUMBER'].tolist()
#COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['STATION_NAME'].tolist()

'''

df = pd.read_csv(r'/Volumes/GoogleDrive/My Drive/PhD/2021_Fall/Dissertation_v12/North_America/Canada/waterLevel.csv',encoding='unicode_escape')


print(df)

df.drop(columns=['Unnamed: 0', 'PRECISION_CODE', 'MONTHLY_MEAN', 'MONTHLY_TOTAL', 'FIRST_DAY_MIN', 'MIN', 'FIRST_DAY_MAX', 'MAX', 'LEVEL_SYMBOL1', 'LEVEL_SYMBOL2'], inplace=True)
df.drop(columns=['LEVEL_SYMBOL3', 'LEVEL_SYMBOL4', 'LEVEL_SYMBOL5', 'LEVEL_SYMBOL6', 'LEVEL_SYMBOL7', 'LEVEL_SYMBOL8', 'LEVEL_SYMBOL9', 'LEVEL_SYMBOL10'], inplace=True)
df.drop(columns=['LEVEL_SYMBOL11', 'LEVEL_SYMBOL12', 'LEVEL_SYMBOL13', 'LEVEL_SYMBOL14', 'LEVEL_SYMBOL15', 'LEVEL_SYMBOL16', 'LEVEL_SYMBOL17'], inplace=True)
df.drop(columns=['LEVEL_SYMBOL18', 'LEVEL_SYMBOL19', 'LEVEL_SYMBOL20', 'LEVEL_SYMBOL21', 'LEVEL_SYMBOL22', 'LEVEL_SYMBOL23', 'LEVEL_SYMBOL24'], inplace=True)
df.drop(columns=['LEVEL_SYMBOL25', 'LEVEL_SYMBOL26', 'LEVEL_SYMBOL27', 'LEVEL_SYMBOL28', 'LEVEL_SYMBOL29', 'LEVEL_SYMBOL30', 'LEVEL_SYMBOL31'], inplace=True)

df.set_index(['STATION_NUMBER'], inplace=True)

print(df)

df.to_csv(r'/Volumes/GoogleDrive/My Drive/PhD/2021_Fall/Dissertation_v12/North_America/Canada/waterLevel_v2.csv')

'''

df = pd.read_csv(r'/Volumes/GoogleDrive/My Drive/PhD/2021_Fall/Dissertation_v12/North_America/Canada/waterLevel_v2.csv',encoding='unicode_escape')


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
	final_df = pd.DataFrame(values, index=dates, columns=['Water Level (m)'])
	final_df.index.name = 'Datetime'
	while np.isnan(final_df.iloc[:, 0].values[0]):
		final_df = final_df.iloc[1:]
	while np.isnan(final_df.iloc[:, 0].values[len(final_df.iloc[:, 0].values) - 1]):
		final_df = final_df.iloc[:len(final_df.iloc[:, 0].values) - 2]

	datesObservedWaterLevel = pd.date_range(final_df.index[0], final_df.index[len(final_df.index) - 1], freq='D')
	df2 = pd.DataFrame(np.nan, index=datesObservedWaterLevel, columns=['Water Level (m)'])
	df2.index.name = 'Datetime'

	#print(df2)

	df3 = df2.fillna(final_df)

	#print(df3)

	df3.to_csv("/Volumes/GoogleDrive/My Drive/MSc_Darlly_Rojas/2022_Winter/CE EN 699R - Master's Thesis/Bias-Correction_Water-Level-Data/Canada/{}.csv".format(id))

