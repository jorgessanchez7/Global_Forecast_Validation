import dataretrieval.nwis as nwis
from climata.usgs import DailyValueIO
import pandas as pd
import numpy as np
import datetime as dt

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/USA/Selected_Stations_USA_WL_v0.csv')

IDs = stations_pd['STAID'].tolist()
COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['STANAME'].tolist()

for id, name, comid in zip(IDs, Names, COMIDs):

	if id < 10000000:
		station_id = '0' + str(id)
	else:
		station_id = str(id)
	#param_id = "00060" #streamflow
	param_id = "00065"  #water level

	print(station_id, ' - ', name, ' - ', comid)

	datelist = pd.date_range(dt.datetime(1900, 1, 1), dt.datetime.today(), freq='D').tolist()

	data = DailyValueIO(
		start_date=datelist[0],
		end_date=datelist[-1],
		station=station_id,
		parameter=param_id,
	)

	for series in data:
		wl = [r[1] for r in series.data]
		dates = [r[0] for r in series.data]

	wl_cm = []

	for level in wl:
		if level == -999999:
			wl_cm.append(np.nan)
		else:
			wl_cm.append(level * 30.48)

	df = pd.DataFrame(wl_cm, index=dates, columns=['Water Level (cm)'])
	df.dropna(inplace=True)

	datesObservedWL = pd.date_range(df.index[0], df.index[len(df.index) - 1], freq='D')
	df2 = pd.DataFrame(np.nan, index=datesObservedWL, columns=['Water Level (cm)'])
	df2.index.name = 'Datetime'

	observed_df = df2.fillna(df)
	observed_df[observed_df < 0] = 0
	observed_df.index = pd.to_datetime(observed_df.index)
	observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
	observed_df.index = pd.to_datetime(observed_df.index)

	observed_df.to_csv("/Volumes/GoogleDrive/My Drive/MSc_Darlly_Rojas/2022_Winter/CE EN 699R - Master's Thesis/Bias-Correction_Water-Level-Data/USA/{}.csv".format(station_id))