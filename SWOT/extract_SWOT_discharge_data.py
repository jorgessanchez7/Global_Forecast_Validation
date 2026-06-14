import numpy as np
import pandas as pd
from netCDF4 import Dataset

ds = Dataset(r"E:\SWOT\Discharge\03\af_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc")
all_reach_ids = ds['reaches']['reach_id'][:]
#print(ds)
#ds.groups['consensus']
#ds.groups['consensus']['consensus_q']
#ds.groups['consensus']['consensus_q'][:]
#ds.groups['reaches']
#ds.groups['reaches']['reach_id']
#ds.groups['reaches']['reach_id'][:]


stations_uganda = pd.read_csv(r"E:\GEOGloWS\SWOT_Intersection\Uganda_SWORD_reaches_total.csv", index_col=0)

swot_ids = stations_uganda['reach_id'].to_list()

for swot_id in swot_ids:

    print(swot_id)

    reach_index = np.where(all_reach_ids == swot_id)[0][0]

    discharge = ds['consensus']['consensus_q'][reach_index]
    mask = discharge != ds['consensus']['consensus_q'].missing_value
    discharge = discharge[mask]

    time_s = ds['consensus']['time_int'][reach_index]
    time_s = time_s[mask]
    epoch = np.datetime64('2000-01-01')
    datetime = epoch + time_s.astype('timedelta64[s]')

    df = pd.DataFrame({"consensus_q": discharge}, index=pd.to_datetime(datetime))
    df = df.sort_index()
    df.index.name = 'Datetime'
    df.index = pd.to_datetime(df.index)
    df.index = df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    df.index = pd.to_datetime(df.index)

    if df.empty:
        print("  -> empty dataframe")
        continue

    print(df)

    df.to_csv(r"G:\.shortcut-targets-by-id\12pIthB8qownac-a_q2pCe0gd__8LrDoD\HydroServer\Data\swot\Discharge\{}.csv".format(swot_id))








