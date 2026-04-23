import xarray as xr
from pathlib import Path
from ecmwfapi import ECMWFService

server = ECMWFService("mars")

dates = ['20260213', '20260214', '20260215', '20260216']

for fecha in dates:

    outfile = f"{fecha[0:4]}-{fecha[4:6]}-{fecha[6:8]}_1200_HRES_runoff.grib"

    print(f"Downloading {outfile}")

    server.execute(
        {
            "class": "od",
            "type": "fc",
            "stream": "oper",
            "expver": "0001",
            "repres": "gg",
            "levtype": "sfc",
            "param": "205.128",
            "date": fecha,
            "time": "0000",
            "step": (
                "0/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15/16/17/18/19/20/21/22/23/24/25/"
                "26/27/28/29/30/31/32/33/34/35/36/37/38/39/40/41/42/43/44/45/46/47/48/"
                "49/50/51/52/53/54/55/56/57/58/59/60/61/62/63/64/65/66/67/68/69/70/71/"
                "72/73/74/75/76/77/78/79/80/81/82/83/84/85/86/87/88/89/90/93/96/99/102/"
                "105/108/111/114/117/120/123/126/129/132/135/138/141/144/150/156/162/"
                "168/174/180/186/192/198/204/210/216/222/228/234/240"
            ),
            "domain": "g",
            "resol": "auto",
            "grid": "F1280",
        },
        outfile,
    )

    gribfile = Path(f"{fecha[0:4]}-{fecha[4:6]}-{fecha[6:8]}_1200_HRES_runoff.grib")

    nc_file = gribfile.with_suffix(".nc")
    print(f"Converting: {gribfile.name} -> {nc_file.name}")

    ds = xr.open_dataset(gribfile, engine="cfgrib")
    ds.to_netcdf(nc_file)
    ds.close()

    #gribfile.unlink()

