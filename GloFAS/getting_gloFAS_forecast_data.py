import cdsapi

dataset = "cems-glofas-forecast"
request = {
    "system_version": ["operational"],
    "hydrological_model": ["lisflood"],
    "product_type": [
        "control_forecast",
        "ensemble_perturbed_forecasts"
    ],
    "variable": "river_discharge_in_the_last_24_hours",
    "year": ["2025"],
    "month": ["06"],
    "day": ["19"],
    "leadtime_hour": ["24", "48", "72", "96", "120", "144", "168", "192", "216", "240", "264", "288", "312", "336",
                      "360", "384", "408", "432", "456", "480", "504", "528", "552", "576", "600", "624", "648", "672",
                      "696", "720"],
    "data_format": "netcdf",
    "download_format": "zip"
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()