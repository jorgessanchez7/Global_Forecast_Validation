import cdsapi

years = ['1979', '1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992',
         '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006',
         '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020',
         '2021', '2022', '2023', '2024']

dataset = "cems-glofas-historical"

for year in years:

    request = {
        "system_version": ["version_4_0"],
        "hydrological_model": ["lisflood"],
        "product_type": ["consolidated"],
        "variable": ["river_discharge_in_the_last_24_hours"],
        "hyear": ["{0}".format(year)],
        "hmonth": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
        "hday": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
                 "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"],
        "data_format": "netcdf",
        "download_format": "unarchived"
    }
    client = cdsapi.Client()
    target = f"D:\\GloFAS_Retrospective\\{year}.nc"
    client.retrieve(dataset, request, target=str(target))
