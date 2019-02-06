import xarray as xr
import pandas as pd
import numpy as np
import os
import numba as nb
import time
import dask.array as da


def compute_all(work_dir, memory_to_allocate_gb, date_strings):
    array_size_bytes = 3060  # Based on 15 x 51 member array
    memory_to_allocate_bytes = memory_to_allocate_gb * 1e9

    files = [os.path.join(work_dir, i + ".nc") for i in date_strings]

    chunk_size = int(np.floor(memory_to_allocate_bytes / ((array_size_bytes * len(files)) + len(files))))

    print("Chunk Size:", chunk_size)

    list_of_dask_q_arrays = []
    list_of_dask_init_arrays = []

    # Creating a large dask array with all of the data in it
    start = time.time()
    for file in files:
        ds = xr.open_dataset(file, chunks={"rivid": chunk_size})

        tmp_dask_q_array = ds["Qout"].data
        list_of_dask_q_arrays.append(tmp_dask_q_array)

        tmp_dask_init_array = ds["initialization_values"].data
        list_of_dask_init_arrays.append(tmp_dask_init_array)

        ds.close()
    end = time.time()

    big_dask_q_array = da.stack(list_of_dask_q_arrays)
    big_dask_init_array = da.stack(list_of_dask_init_arrays)

    print(big_dask_q_array.shape)
    print(big_dask_init_array.shape)

    print("Time to create dask arrays: ", end - start)

    # Retrieving the number of streams and their corresponding Rivids
    tmp_dataset = xr.open_dataset(files[0])

    num_of_streams = tmp_dataset['rivid'].size
    rivids = tmp_dataset['rivid'].data

    tmp_dataset.close()

    num_chunk_iterations = int(np.ceil(num_of_streams / chunk_size))
    start_chunk = 0
    end_chunk = chunk_size
    list_of_tuples_with_metrics = []

    for chunk_number in range(num_chunk_iterations):

        start = time.time()
        big_forecast_data_array = np.asarray(big_dask_q_array[:, start_chunk:end_chunk, :, :])
        big_init_data_array = np.asarray(big_dask_init_array[:, start_chunk:end_chunk])
        end = time.time()
        print("Time to read from disk:", end - start)

        rivids_chunk = rivids[start_chunk:end_chunk]

        start = time.time()
        results_array = numba_calculate_metrics(
            big_forecast_data_array, big_init_data_array, len(files), big_forecast_data_array.shape[1], 15
        )
        end = time.time()
        print("Numba Calculation Time: ", end - start)

        for rivid in range(results_array.shape[1]):
            for forecast_day in range(results_array.shape[0]):
                tmp_array = results_array[forecast_day, rivid, :]
                tuple_to_append = (rivids_chunk[rivid], '{} Day Forecast'.format(str(forecast_day + 1).zfill(2)),
                                   tmp_array[0], tmp_array[1], tmp_array[2])
                list_of_tuples_with_metrics.append(tuple_to_append)

        start_chunk += chunk_size
        end_chunk += chunk_size

    final_df = pd.DataFrame(list_of_tuples_with_metrics,
                            columns=['Rivid', 'Forecast Day', 'CRPS', 'CRPS BENCH', 'CRPSS'])
    final_df.to_csv(r'/Users/wade/PycharmProjects/Forecast_Validation/South_America_Test_DF.csv', index=False)


@nb.njit(parallel=True)
def numba_calculate_metrics(forecast_array, initialization_array, number_of_start_dates, number_of_streams,
                            num_forecast_days):
    """
    Parameters
    ----------

    forecast_array: 4D ndarray
        A 4 dimensional numPy array with the following dimensions: 1) Start Date Number (365 if there are a year's
        forecasts), 2) Unique stream ID, 3) Forecast Days (e.g. 1-15 in a 15 day forecast), 4) Ensembles

    initialization_array: 2D ndarray
        A 2 dimenensional NumPy array with the following dimensions: 1) Start Dates, 2) Unique stream ID

    number_of_start_dates: The number of start dates for the analysis to perform

    number_of_streams:
        The number of streams in the analysis

    num_forecast_days:
        The number of forecast days in the analysis

    Returns
    -------
    ndarray
        An ndarray with the folowing dimenstions:
        1) Forecast Day: 1-15 in the case of a 15 day forecast
        2) Rivid: The stream unique ID
        3) Metrics: CRPS, CRPS_BENCH, CRPSS

    """
    return_array = np.zeros((num_forecast_days, number_of_streams, 3), dtype=np.float32)

    for stream in nb.prange(number_of_streams):
        for forecast_day in range(num_forecast_days):
            initialization_vals = initialization_array[(forecast_day + 1):, stream]
            # np.savetxt("init_test.txt", initialization_vals)
            forecasts = forecast_array[:(number_of_start_dates - (forecast_day + 1)), stream, forecast_day, :]
            # np.savetxt("forecasts_test.txt", forecasts)
            benchmark_forecasts = initialization_array[:(number_of_start_dates - (forecast_day + 1)), stream]
            # np.savetxt("benchmark_forecasts_test.txt", benchmark_forecasts)

            crps = ens_crps(initialization_vals, forecasts)
            crps_bench = mae(initialization_vals, benchmark_forecasts)
            if crps_bench == 0:
                crpss = np.inf
                print("Warning: Division by zero on: ", stream)
            else:
                crpss = 1 - crps / crps_bench

            return_array[forecast_day, stream, 0] = crps
            return_array[forecast_day, stream, 1] = crps_bench
            return_array[forecast_day, stream, 2] = crpss

            # print(crps, crps_bench, crpss)
        if (stream % 1000) == 0:
            print("Count: ", stream)

    return return_array


@nb.njit()
def mae(sim, obs):
    return np.mean(np.abs(sim - obs))


@nb.njit()
def ens_crps(obs, fcst_ens, adj=np.nan):

    rows = obs.size
    cols = fcst_ens.shape[1]

    col_len_array = np.ones(rows) * cols
    sad_ens_half = np.zeros(rows)
    sad_obs = np.zeros(rows)
    crps = np.zeros(rows)

    crps = numba_crps(
        fcst_ens, obs, rows, cols, col_len_array, sad_ens_half, sad_obs, crps, np.float64(adj)
    )

    # Calc mean crps as simple mean across crps[i]
    crps_mean = np.mean(crps)

    return crps_mean


@nb.njit()
def numba_crps(ens, obs, rows, cols, col_len_array, sad_ens_half, sad_obs, crps, adj):
    for i in range(rows):
        the_obs = obs[i]
        the_ens = ens[i, :]
        the_ens = np.sort(the_ens)
        sum_xj = 0.
        sum_jxj = 0.

        j = 0
        while j < cols:
            sad_obs[i] += np.abs(the_ens[j] - the_obs)
            sum_xj += the_ens[j]
            sum_jxj += (j + 1) * the_ens[j]
            j += 1

        sad_ens_half[i] = 2.0 * sum_jxj - (col_len_array[i] + 1) * sum_xj

    if np.isnan(adj):
        for i in range(rows):
            crps[i] = sad_obs[i] / col_len_array[i] - sad_ens_half[i] / \
                      (col_len_array[i] * col_len_array[i])
    elif adj > 1:
        for i in range(rows):
            crps[i] = sad_obs[i] / col_len_array[i] - sad_ens_half[i] / \
                      (col_len_array[i] * (col_len_array[i] - 1)) * (1 - 1 / adj)
    elif adj == 1:
        for i in range(rows):
            crps[i] = sad_obs[i] / col_len_array[i]
    else:
        for i in range(rows):
            crps[i] = np.nan

    return crps


if __name__ == "__main__":

    starting_date = "2018-08-19"
    ending_date = "2018-12-16"

    dates_range = pd.date_range(starting_date, ending_date)
    dates_strings = dates_range.strftime("%Y%m%d").tolist()

    workspace = r'/Users/wade/Documents/South_America_Forecasts'
    MEMORY_TO_ALLOCATE = 1.0  # GB

    start = time.time()
    compute_all(workspace, MEMORY_TO_ALLOCATE, dates_strings)
    end = time.time()
    print(end - start)
