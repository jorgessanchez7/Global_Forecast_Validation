import numpy as np
import unittest
from compress_netcdf import compress_netcfd
import os
import xarray as xr


class TestCompressNetcdf(unittest.TestCase):
    """
    Tests the functions included in compress_netcdf.py to make sure that they are working correctly with
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
        initialization_values = ds["initialization_values"].data
        date = ds["date"].data
        date_high_res = ds["date_high_res"].data
        rivids = ds["rivid"].data

        benchmark_flow_array = np.load("Test_files/Comparison_Files/benchmark_flow_array.npy")
        benchmark_array_high_res = np.load("Test_files/Comparison_Files/benchmark_array_high_res.npy")
        benchmark_initialization_values = np.array([1.9156765937805176, 1.7540310621261597, 1.5276174545288086,
                                                    2.0937132835388184, 3.4932050704956055, 3.678524971008301,
                                                    0.854943037033081, 1.688418984413147, 13.452759742736816,
                                                    3.7613487243652344], dtype=np.float32)
        benchmark_date = np.array(['2019-01-05', '2019-01-06', '2019-01-07', '2019-01-08',
                                   '2019-01-09', '2019-01-10', '2019-01-11', '2019-01-12',
                                   '2019-01-13', '2019-01-14', '2019-01-15', '2019-01-16',
                                   '2019-01-17', '2019-01-18', '2019-01-19'],
                                  dtype='datetime64[ns]')
        benchmark_date_high_res = np.array(['2019-01-05', '2019-01-06', '2019-01-07', '2019-01-08',
                                            '2019-01-09', '2019-01-10', '2019-01-11', '2019-01-12',
                                            '2019-01-13', '2019-01-14'], dtype='datetime64[ns]')
        benchmark_rivids = np.array([192474, 192473, 192470, 192469, 192454, 192452, 192448, 192446, 192451, 192450],
                                    dtype=np.int32)

        print("Testing")
        self.assertTrue(np.all(np.isclose(flow_array, benchmark_flow_array)))
        self.assertTrue(np.all(np.isclose(flow_array_high_res, benchmark_array_high_res)))
        self.assertTrue(np.all(np.isclose(initialization_values, benchmark_initialization_values)))
        self.assertTrue(np.all(date == benchmark_date))
        self.assertTrue(np.all(date_high_res == benchmark_date_high_res))
        self.assertTrue(np.all(rivids == benchmark_rivids))

        ds.close()

    def tearDown(self):
        os.remove("Test_files/20190104.nc")


class TestValidateForecasts(unittest.TestCase):

    def setUp(self):
        pass

    def test_compress_netcdf(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main(verbosity=2)
