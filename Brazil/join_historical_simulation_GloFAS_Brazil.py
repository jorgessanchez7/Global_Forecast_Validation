import xarray as xr

#Brazil
ds_1979_2000 = xr.open_dataset('/Users/grad/Google Drive/My Drive/GloFAS/Historical_Simulation/Brazil_v3.1/Brazil_1979-2000.nc', engine='netcdf4')
ds_2001_2022 = xr.open_dataset('/Users/grad/Google Drive/My Drive/GloFAS/Historical_Simulation/Brazil_v3.1/Brazil_2001-2022.nc', engine='netcdf4')

ds_final = xr.concat([ds_1979_2000, ds_2001_2022], dim='time')

ds_final.to_netcdf('/Users/grad/Google Drive/My Drive/GloFAS/Historical_Simulation/Brazil_v3.1/Brazil_1979-2022.nc')