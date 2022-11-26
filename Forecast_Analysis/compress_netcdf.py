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
    high_res_forecast_day_indices = np.array([24, 48, 72, 92, 100, 108, 112, 116, 120, 124])

    start_datetime = to_datetime(start_date, infer_datetime_format=True)
    dates = date_range(start_datetime + DateOffset(1), periods=15)
    high_res_dates = date_range(start_datetime + DateOffset(1), periods=10)

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
    file = os.path.join(folder_path, "{}_52.nc".format(file_name))

    tmp_dataset = xr.open_dataset(file)

    high_res_forecast_data = tmp_dataset["Qout"].data
    high_res_forecast_data = high_res_forecast_data[:, high_res_forecast_day_indices]

    tmp_dataset.close()

    data_variables = {
        "Qout": (['rivid', 'date', 'ensemble_number'], ensembles),
        "Qout_high_res": (['rivid', 'date_high_res'], high_res_forecast_data)
    }

    coords = {
        'rivid': rivids,
        'date': dates,
        'date_high_res': high_res_dates,
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

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/asia-middle_east/20170101.00/Qout_asia_middle_east_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/asia-north_asia/20170101.00/Qout_asia_north_asia_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/australasia-continental/20170101.00/Qout_australasia_continental_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/australia-geoglows/20170101.00/Qout_australia_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/central_america-geoglows/20140101.00/Qout_central_america_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/central_asia-geoglows/20140101.00/Qout_central_asia_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/east_asia-geoglows/20140101.00/Qout_east_asia_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/cuba-national/20170101.00/Qout_cuba_national_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/dominican_republic-hand/20170101.00/Qout_dominican_republic_hand_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/dominican_republic-national/20170101.00/Qout_dominican_republic_national_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/europe-global/20170101.00/Qout_europe_global_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/europe-geoglows/20140101.00/Qout_europe_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/islands-geoglows/20140101.00/Qout_islands_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/japan-geoglows/20140101.00/Qout_japan_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/middle_east-geoglows/20140101.00/Qout_middle_east_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_20140101-20191231/middle_east-geoglows/20140101.00/Qout_middle_east_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/north_america-continental/20170101.00/Qout_north_america_continental_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/north_america-geoglows/20140101.00/Qout_north_america_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/south_america-continental/20170101.00/Qout_south_america_continental_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/south_america-geoglows/20140101.00/Qout_south_america_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/south_asia-mainland/20170101.00/Qout_south_asia_mainland_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/south_asia-sri_lanka/20170101.00/Qout_south_asia_sri_lanka_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/south_asia-geoglows/20140101.00/Qout_south_asia_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    #path = r"/Volumes/files/ECMWF/output_20140101-20141231/west_asia-geoglows/20140101.00/Qout_west_asia_geoglows_1.nc"
    #ds = xr.open_dataset(path)
    #print(ds)

    '''
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
    '''

    '''
    print('Asia Middle East')
    pass
    num_rivids = 57394
    file_name = "Qout_asia_middle_east"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/asia-middle_east"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/asia-middle_east"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[68:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('North Asia')
    pass
    num_rivids = 48717
    file_name = "Qout_asia_north_asia"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/asia-north_asia"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/asia-north_asia"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[67:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Australia')
    pass
    num_rivids = 62987
    file_name = "Qout_australia_geoglows"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/australia-geoglows"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/australia-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[70:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Australasia')
    pass
    num_rivids = 38320
    file_name = "Qout_australasia_continental"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/australasia-continental"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/australasia-continental"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[75:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Central America')
    pass
    num_rivids = 55358
    file_name = "Qout_central_america_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/central_america-geoglows"

    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/central_america-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[71:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Central America')
    pass
    num_rivids = 55358
    file_name = "Qout_central_america_geoglows"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/central_america-geoglows"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_20140101-20191231/central_america-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[94:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Central Asia')
    pass
    num_rivids = 55621
    file_name = "Qout_central_asia_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/central_asia-geoglows"
    
    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/central_asia-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]
    
    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[68:-3]
        
        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Cuba')
    pass
    num_rivids = 1401
    file_name = "Qout_cuba_national"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/cuba-national"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/cuba-national"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[65:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Dominican Republic')
    pass
    num_rivids = 1365
    file_name = "Qout_dominican_republic_hand"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/dominican_republic-hand"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/dominican_republic-hand"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[75:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Dominican Republic')
    pass
    num_rivids = 1500
    file_name = "Qout_dominican_republic_national"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/dominican_republic-national"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/dominican_republic-national"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[79:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('East Asia')
    pass
    num_rivids = 79931
    file_name = "Qout_east_asia_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/east_asia-geoglows"

    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/east_asia-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[65:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Europe')
    pass
    num_rivids = 30133
    file_name = "Qout_europe_global"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/europe-global"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/europe-global"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[65:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")

    '''

    '''
    print('Europe')
    pass
    num_rivids = 80769
    file_name = "Qout_europe_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/europe-geoglows"

    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/europe-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[62:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Islands')
    pass
    num_rivids = 29829
    file_name = "Qout_islands_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/islands-geoglows"

    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/islands-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[63:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Islands')
    pass
    num_rivids = 29829
    file_name = "Qout_islands_geoglows"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/islands-geoglows"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_20140101-20191231/islands-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[86:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Japan')
    pass
    num_rivids = 5302
    file_name = "Qout_japan_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/japan-geoglows"

    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/japan-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[61:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''


    print('Middle East')
    pass
    num_rivids = 41041
    file_name = "Qout_middle_east_geoglows"
    #out_path = "/Volumes/files/ECMWF/output_compressed_2014/middle_east-geoglows"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/middle_east-geoglows"

    #base_path = "/Volumes/files/ECMWF/output_20140101-20141231/middle_east-geoglows"
    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_20140101-20191231/middle_east-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        #date_string = folder_path[67:-3]
        date_string = folder_path[90:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")


    '''
    print('North America')
    pass
    num_rivids = 62788
    file_name = "Qout_north_america_continental"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/north_america-continental"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/north_america-continental"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[77:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('North America')
    pass
    num_rivids = 82912
    file_name = "Qout_north_america_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/north_america-geoglows"

    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/north_america-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[69:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''


    '''
    print('South America')
    pass
    num_rivids = 62317
    file_name = "Qout_south_america_continental"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/south_america-continental"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/south_america-continental"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[77:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('South America')
    pass
    num_rivids = 149384
    file_name = "Qout_south_america_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/south_america-geoglows"

    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/south_america-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[69:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('South Asia')
    pass
    num_rivids = 84095
    file_name = "Qout_south_asia_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/south_asia-geoglows"
    
    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/south_asia-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]
    
    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[66:-3]
        
        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('South Asia')
    pass
    num_rivids = 24328
    file_name = "Qout_south_asia_mainland"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/south_asia-mainland"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/south_asia-mainland"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[71:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''

    '''
    print('Sri Lanka')
    pass
    num_rivids = 2040
    file_name = "Qout_south_asia_sri_lanka"
    out_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output_compressed/south_asia-sri_lanka"

    base_path = "/Volumes/storage/ECMWF_Gridded_Runoff_Files/output/south_asia-sri_lanka"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[72:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")

    '''

    '''
    print('West Asia')
    pass
    num_rivids = 94025
    file_name = "Qout_west_asia_geoglows"
    out_path = "/Volumes/files/ECMWF/output_compressed_2014/west_asia-geoglows"

    base_path = "/Volumes/files/ECMWF/output_20140101-20141231/west_asia-geoglows"
    folder_paths = [os.path.join(base_path, x) for x in os.listdir(base_path) if x.endswith(".00")]

    folder_paths.sort()
    for folder_path in folder_paths:
        date_string = folder_path[65:-3]

        print("Starting: ", date_string)
        compress_netcfd(folder_path, date_string, out_path, file_name, num_rivids)
        print("Finished")
    '''