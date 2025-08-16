import cdsapi

client = cdsapi.Client()

dataset = "cems-glofas-historical"
request = {
    "system_version": ["version_4_0"],
    "hydrological_model": ["lisflood"],
    "product_type": ["intermediate"],
    "variable": ["river_discharge_in_the_last_24_hours"],
    "hyear": ["2025"],
    "hmonth": ["06", "07", "08"],
    "hday": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
             "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"],
    "data_format": "netcdf",
    "download_format": "zip"
}

client.retrieve(dataset, request).download()