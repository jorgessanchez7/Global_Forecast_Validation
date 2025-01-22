import unicodedata
import pandas as pd
from io import StringIO
from pyproj import Proj, transform

def romanize(text):
    """Remove accents and special characters from Spanish text."""
    if not isinstance(text, str):
        return text  # Return the value as-is if it's not a string
    return ' '.join(
        ''.join((c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')).split()
    )  # Normalize spaces and strip leading/trailing spaces

def convert_to_decimal(coord):
    """Convert integer coordinates to decimal degrees."""
    coord_str = str(abs(coord))  # Get absolute value as string
    if len(coord_str) > 4:
        degrees = int(coord_str[:-4])  # Extract degrees
        minutes = int(coord_str[-4:-2])  # Extract minutes
        seconds = int(coord_str[-2:])  # Extract seconds
    elif len(coord_str) == 4:
        degrees = int(coord_str[:-2])  # Extract degrees
        minutes = int(coord_str[-2:])  # Extract minutes
        seconds = 0  # Default seconds to 0
    elif len(coord_str) <= 2:
        degrees = int(coord_str)  # Degrees only
        minutes = 0
        seconds = 0
    else:
        degrees = int(coord_str[:-2])  # Extract degrees for shorter coordinates
        minutes = int(coord_str[-2:])  # Extract minutes
        seconds = 0  # Default seconds to 0

    decimal = degrees + minutes / 60 + seconds / 3600
    return -decimal if coord < 0 else decimal

def convert_utm_to_wgs84(xutm, yutm, zone=30, northern=True):
    """Convert UTM coordinates to WGS84 (latitude, longitude).

    Args:
        xutm (float): UTM X coordinate.
        yutm (float): UTM Y coordinate.
        zone (int): UTM zone number.
        northern (bool): True if the UTM zone is in the northern hemisphere.

    Returns:
        tuple: (latitude, longitude) in WGS84.
    """
    utm_proj = Proj(proj='utm', zone=zone, ellps='WGS84', northern=northern)
    wgs84_proj = Proj(proj='latlong', ellps='WGS84')
    lon, lat = transform(utm_proj, wgs84_proj, xutm, yutm)
    return lat, lon


def process_watershed_data(long_name, short_name):
    """Process watershed data from a CSV file.

    Args:
        long_name (str): Long name of the watershed organization.
        short_name (str): Short name used to locate the CSV file.

    Returns:
        pd.DataFrame: Processed DataFrame with selected and transformed columns.
    """

    df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Spain/Station_Network/row_data/estaf_{0}.csv'.format(short_name), sep=';', skipinitialspace=True,  encoding='latin1')

    # Select and rename the desired columns
    df_filtered = df[["indroea", "lugar", "alti", "latwgs84", "longwgs84", "xutm30", "yutm30"]].copy()
    df_filtered.rename(columns={
        "indroea": "ID",
        "lugar": "Station Name",
        "alti": "Elevation",
        "latwgs84": "Latitude",
        "longwgs84": "Longitude",
        "xutm30": "UTM X",
        "yutm30": "UTM Y"
    }, inplace=True)

    # Apply the transformations
    df_filtered["Station Name"] = df_filtered["Station Name"].apply(romanize)
    df_filtered["Latitude"] = df_filtered["Latitude"].apply(convert_to_decimal)
    df_filtered["Longitude"] = df_filtered["Longitude"].apply(convert_to_decimal)

    # Convert UTM to WGS84
    df_filtered[["Latitude WGS84", "Longitude WGS84"]] = df_filtered.apply(
        lambda row: pd.Series(convert_utm_to_wgs84(row["UTM X"], row["UTM Y"])), axis=1
    )

    # Add a new column "Watershed Organization"
    df_filtered["Watershed Organization"] = long_name
    df_filtered["Organization"] = short_name

    df_filtered.set_index('ID', inplace=True)

    return df_filtered

organizations = ['CANTABRICO', 'DUERO', 'EBRO', 'GALICIA', 'GUADALQUIVIR', 'GUADIANA', 'JUCAR', 'MINO-SIL', 'SEGURA', 'TAJO']

organizations_names = ['C.H. CANTABRICO', 'C.H. DUERO', 'C.H. EBRO', 'AUGAS DE GALICIA – XUNTA DE GALICIA', 'C.H. GUADALQUIVIR',
                       'C.H. GUADIANA', 'C.H. JUCAR', 'C.H. MINO-SIL', 'C.H. SEGURA', 'C.H. TAJO']

total_stations = pd.DataFrame()

for organization, organization_name in zip(organizations, organizations_names):

    print(organization, ' - ', organization_name)

    result_df = process_watershed_data(organization_name, organization)

    total_stations = pd.concat([total_stations, result_df])

total_stations.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Spain/Station_Network/Total_Stations_Spain.csv")