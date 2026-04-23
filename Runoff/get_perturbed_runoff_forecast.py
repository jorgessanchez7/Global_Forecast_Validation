import xarray as xr
from pathlib import Path
from ecmwfapi import ECMWFService

server = ECMWFService("mars")

dates = ["20260213", "20260214", "20260215", "20260216"]
#numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
#           31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50]
numbers = [1]

for fecha in dates:
    y = fecha[0:4]
    m = fecha[4:6]
    d = fecha[6:8]

    for numero in numbers:
        numero_name = f"{numero:02d}"

        tmp1 = f"{y}-{m}-{d}_1200_Perturbed_forecast_{numero_name}_part1.grib"
        tmp2 = f"{y}-{m}-{d}_1200_Perturbed_forecast_{numero_name}_part2.grib"
        final = f"{y}-{m}-{d}_1200_Perturbed_forecast_{numero_name}.grib"

        print(f"Downloading part 1: {tmp1}")

        server.execute(
            {
                "class": "od",
                "type": "pf",
                "stream": "enfo",
                "expver": "0001",
                "levtype": "sfc",
                "param": "205.128",
                "date": fecha,
                "time": "1200",
                "number": str(numero),
                "step": "0/to/144/by/3",
                "grid": "F1280",
            },
            tmp1,
        )

        print(f"Downloading part 2: {tmp2}")

        server.execute(
            {
                "class": "od",
                "type": "pf",
                "stream": "enfo",
                "expver": "0001",
                "levtype": "sfc",
                "param": "205.128",
                "date": fecha,
                "time": "1200",
                "number": str(numero),
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

        for f in [tmp1, tmp2]:
            p = Path(f)
            if p.exists():
                p.unlink()

        gribfile = Path(f"{y}-{m}-{d}_1200_Perturbed_forecast_{numero_name}.grib")

        nc_file = gribfile.with_suffix(".nc")
        print(f"Converting: {gribfile.name} -> {nc_file.name}")

        ds = xr.open_dataset(gribfile, engine="cfgrib")
        ds.to_netcdf(nc_file)
        ds.close()

        #gribfile.unlink()

        print(f"Done: {final}")

print("All downloads finished.")