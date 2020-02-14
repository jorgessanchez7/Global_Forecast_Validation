import xarray as xr
from pandas import to_datetime, date_range, DateOffset
import numpy as np
import os


def compress_netcfd(folder_path, start_date, out_folder, file_name, num_of_rivids):
    """
    Takes the 52 individual ensembles and combines them into one compact NetCDF file, saving disk space in the process.

    Parameters
    ----------
    folder_path: str
        The path to the folder containing the 52 ensemble forecast files in NetCDF format
    start_date: str
        The start date in YYYYMMDD format.
    out_folder: str
        The path to the folder that you want the more compact NetCDF file in.
    file_name: str
        The name of the region. For example, if the files followed the pattern of "Qout_africa_continental_1.nc,
        this argument would be "Qout_africa_continental"
    num_of_rivids: int
        The number of streams that are contained in the region.
    """

    # Based on 15 day forecast
    forecast_day_indices = np.array([0, 8, 16, 24, 32, 40, 48, 52, 56, 60, 64, 68, 72, 76, 80, 84], dtype=np.int8)

    # Based on 10 day forecast
    # Excluding the first day because we already have initialization from the normal forecasts
    #high_res_forecast_day_indices = np.array([24, 48, 72, 92, 100, 108, 112, 116, 120, 124])

    start_datetime = to_datetime(start_date, infer_datetime_format=True)
    dates = date_range(start_datetime + DateOffset(1), periods=15)
    #high_res_dates = date_range(start_datetime + DateOffset(1), periods=10)

    # Ensemble Dimensions
    #  1) Rivid
    #  2) Number of forecast days (i.e. 15 in a 15 day forecast)
    #  3) Number of ensembles

    ensembles = np.zeros((num_of_rivids, 15, 51), dtype=np.float32)
    initialization = np.zeros((num_of_rivids,), dtype=np.float32)

    for forecast_number in range(1, 52):
        file = os.path.join(folder_path, "{}_{}.nc".format(file_name, forecast_number))

        tmp_dataset = xr.open_dataset(file)
        streamflow = tmp_dataset['Qout'].data
        streamflow = streamflow[:, forecast_day_indices]

        if forecast_number == 1:
            initialization[:] = streamflow[:, 0]
            rivids = tmp_dataset['rivid'].data
            lat = tmp_dataset['lat'].data
            lon = tmp_dataset['lon'].data
            z = tmp_dataset['z'].data

        ensembles[:, :, forecast_number - 1] = streamflow[:, 1:]

        tmp_dataset.close()

    # High Res Forecast
    #file = os.path.join(folder_path, "{}_52.nc".format(file_name))

    #tmp_dataset = xr.open_dataset(file)

    #high_res_forecast_data = tmp_dataset["Qout"].data
    #high_res_forecast_data = high_res_forecast_data[:, high_res_forecast_day_indices]

    #tmp_dataset.close()

    #data_variables = {
    #    "Qout": (['rivid', 'date', 'ensemble_number'], ensembles),
    #    "Qout_high_res": (['rivid', 'date_high_res'], high_res_forecast_data)
    #}

    data_variables = {
        "Qout": (['rivid', 'date', 'ensemble_number'], ensembles)
    }

    #coords = {
    #    'rivid': rivids,
    #    'date': dates,
    #    'date_high_res': high_res_dates,
    #    'ensemble_number': np.arange(1, 52, dtype=np.uint8),
    #    'initialization_values': ('rivid', initialization),
    #    'lat': ('rivid', lat),
    #    'lon': ('rivid', lon),
    #    'z': ('rivid', z),
    #    'start_date': start_datetime
    #}

    coords = {
        'rivid': rivids,
        'date': dates,
        'ensemble_number': np.arange(1, 52, dtype=np.uint8),
        'initialization_values': ('rivid', initialization),
        'lat': ('rivid', lat),
        'lon': ('rivid', lon),
        'z': ('rivid', z),
        'start_date': start_datetime
    }

    xarray_dataset = xr.Dataset(data_variables, coords)
    xarray_dataset.to_netcdf(path=os.path.join(out_folder, '{}.nc'.format(start_date)), format='NETCDF4')


if __name__ == "__main__":

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/africa-continental/20170101.00/Qout_africa_continental_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)


    print('Africa')
    pass
    num_rivids = 53118
    file_name = "Qout_africa_continental"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/africa-continental"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/africa-continental"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[70:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
