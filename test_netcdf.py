#from netCDF4 import Dataset
import xarray as xr
import numpy as np


'''
dataset1 = Dataset('/Users/student/Downloads/06-02/1.runoff.nc')
dataset2 = Dataset('/Users/student/Downloads/06-02/52.runoff.nc')
dataset3 = Dataset('/Users/student/Downloads/07-02/1.runoff.nc')
#dataset4 = Dataset('/Users/student/Downloads/07-02/52.runoff.nc')
'''
dataset1 = xr.open_dataset('/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/africa-continental/20170101.00/Qout_africa_continental_1.nc')
dataset2 = xr.open_dataset('/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/africa-continental/20170101.00/Qout_africa_continental_18.nc')
dataset3 = xr.open_dataset('/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/africa-continental/20170101.00/Qout_africa_continental_31.nc')
dataset4 = xr.open_dataset('/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/africa-continental/20170101.00/Qout_africa_continental_51.nc')

forecast_day_indices = np.array([0, 8, 16, 24, 32, 40, 48, 52, 56, 60, 64, 68, 72, 76, 80, 84], dtype=np.int8)


rivid = 126232

streamflow1 = dataset1['Qout'].data
streamflow2 = dataset2['Qout'].data
streamflow3 = dataset3['Qout'].data
streamflow4 = dataset4['Qout'].data
streamflow1 = streamflow1[:, forecast_day_indices]
streamflow2 = streamflow2[:, forecast_day_indices]
streamflow3 = streamflow3[:, forecast_day_indices]
streamflow4 = streamflow4[:, forecast_day_indices]

'''
print('2/6/2017')
print('1.runoff.nc')

print (dataset3.dimensions.keys())
print (dataset3.variables.keys())


qout1 = dataset3.variables['Qout']
print(type(qout1))



lon1 = dataset3.variables['lon']
print('lon')
print (lon1)
lat1 = dataset3.variables['lat']
print('lat')
print (lat1)
time1 = time1[:]
print('time array')
print (time1)
lon1 = lon1[:]
print('lon array')
print (lon1)
lat1 = lat1[:]
print('lat array')
print (lat1)
'''