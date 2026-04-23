import xarray as xr
from pathlib import Path
from ecmwfapi import ECMWFService

server = ECMWFService("mars")

dates = ["20260213", "20260214", "20260215", "20260216"]

for fecha in dates:
    y = fecha[0:4]
    m = fecha[4:6]
    d = fecha[6:8]

    tmp1 = f"{y}-{m}-{d}_1200_Control_forecast_part1.grib"
    tmp2 = f"{y}-{m}-{d}_1200_Control_forecast_part2.grib"
    final = f"{y}-{m}-{d}_1200_Control_forecast.grib"

    print(f"Downloading part 1: {tmp1}")
    server.execute(
        {
            "class": "od",
            "type": "cf",
            "stream": "enfo",
            "expver": "0001",
            "levtype": "sfc",
            "param": "205.128",
            "date": fecha,
            "time": "0000",
            "step": "0/to/144/by/3",
            "grid": "F1280",
        },
        tmp1,
    )

    print(f"Downloading part 2: {tmp2}")
    server.execute(
        {
            "class": "od",
            "type": "cf",
            "stream": "enfo",
            "expver": "0001",
            "levtype": "sfc",
            "param": "205.128",
            "date": fecha,
            "time": "0000",
            "step": "150/to/360/by/6",
            "grid": "F1280",
        },
        tmp2,
    )

    print(f"Merging into: {final}")
    with open(final, "wb") as fout:
        for fname in [tmp1, tmp2]:
            with open(fname, "rb") as fin:
                fout.write(fin.read())

    # borrar archivos temporales
    Path(tmp1).unlink()
    Path(tmp2).unlink()

    gribfile = Path(f"{y}-{m}-{d}_1200_Control_forecast.grib")

    nc_file = gribfile.with_suffix(".nc")
    print(f"Converting: {gribfile.name} -> {nc_file.name}")

    ds = xr.open_dataset(gribfile, engine="cfgrib")
    ds.to_netcdf(nc_file)
    ds.close()

    #gribfile.unlink()

    print(f"Done: {final}")

print("All downloads finished.")