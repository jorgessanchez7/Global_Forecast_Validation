import numpy as np
import unittest
from compress_netcdf import compress_netcfd
import os
import xarray as xr


class TestCompressNetcdf(unittest.TestCase):
    """
    Tests the functions included in this repository to make sure that they are working correctly with
    your python version.
    """

    def setUp(self):
        pass

    def test_compress_netcdf(self):
        start_date = "20190104"
        folder_path = os.path.join(os.getcwd(), 'Test_files/Individual_Ensembles')
        out_path = os.path.join(os.getcwd(), 'Test_files')
        compress_netcfd(folder_path, start_date, out_path, "Qout_south_america_continental", 10)

        ds = xr.open_dataset(os.path.join(os.getcwd(), out_path, start_date + ".nc"))
        flow_array = ds["Qout"].data
        flow_array_high_res = ds["Qout_high_res"].data
        inititilization_values = ds["initialization_values"].data
        date = ds["date"].data
        date_high_res = ds["date_high_res"].data
        ensemble_number = ds["ensemble_number"].data

        # TODO: Create files containing the values that should be in the arrays, double check to make sure it is correct
        benchmark_flow_array = np.load("Test_files/Comparison_Files/benchmark_flow_array.npy")
        # benchmark_array_high_res = np.zeros((10, 10))
        # benchmark_initialization_values = np.zeros((10))
        benchmark_date = np.array(['2019-01-05', '2019-01-06', '2019-01-07', '2019-01-08',
                                   '2019-01-09', '2019-01-10', '2019-01-11', '2019-01-12',
                                   '2019-01-13', '2019-01-14', '2019-01-15', '2019-01-16',
                                   '2019-01-17', '2019-01-18', '2019-01-19'],
                                  dtype='datetime64[ns]')

        print("Testing")
        self.assertTrue(np.all(np.isclose(flow_array, benchmark_flow_array)))

        self.assertTrue(np.all(date == benchmark_date))

        ds.close()

    def tearDown(self):
        os.remove("Test_files/20190104.nc")


if __name__ == '__main__':
    unittest.main(verbosity=2)
