import pandas as pd
import json
import numpy as np
import glob
import os


def get_neighborhood() -> pd.DataFrame:
    with open('data/nyu_2451_34572-geojson.json') as json_data:
        newyork_data = json.load(json_data)
    neighborhoods_data = newyork_data['features']

    column_names = ['Borough', 'Neighborhood', 'Latitude', 'Longitude']
    # instantiate the dataframe
    neighborhoods = pd.DataFrame(columns=column_names)

    for data in neighborhoods_data:
        borough = neighborhood_name = data['properties']['borough']
        neighborhood_name = data['properties']['name']

        neighborhood_latlon = data['geometry']['coordinates']
        neighborhood_lat = neighborhood_latlon[1]
        neighborhood_lon = neighborhood_latlon[0]

        neighborhoods = neighborhoods.append({'Borough': borough.upper(),
                                              'Neighborhood': neighborhood_name,
                                              'Latitude': neighborhood_lat,
                                              'Longitude': neighborhood_lon}, ignore_index=True)
    return neighborhoods


def find_neighborhood(location: tuple, borough: str, neighborhoods: pd.DataFrame) -> str:
    bor = neighborhoods.loc[neighborhoods.Borough == borough, ['Borough', 'Neighborhood', 'Latitude', 'Longitude']]
    bor['Distance'] = np.square(location[0] - bor['Latitude']) + np.square(location[1] - bor['Longitude'])
    min_index = bor['Distance'].idxmin()
    return bor.loc[min_index, 'Neighborhood']


def attach_NBH(data: pd.DataFrame, crime: bool, neighborhoods: pd.DataFrame):
    for index, row in data.iterrows():
        if crime:
            location = (row['Latitude'], row['Longitude'])
            boro = getNameforBorough(row['ARREST_BORO'])
        else:
            location = (row['LATITUDE'], row['LONGITUDE'])
            boro = row['BOROUGH']
        nbh = find_neighborhood(location, boro, neighborhoods)
        data.loc[index, 'NBH'] = nbh


def getNameforBorough(letter: str) -> str:
    if letter == 'M':
        borough = 'MANHATTAN'
    elif letter == 'B':
        borough = 'BRONX'
    elif letter == 'K':
        borough = 'BROOKLYN'
    elif letter == 'S':
        borough = 'STATEN ISLAND'
    else:
        borough = 'QUEENS'
    return borough


def mergeBoroughData(boroughName):
    files = os.path.join(f"Data\{boroughName}", f"*.csv")
    print(files)
    files = glob.glob(files)
    print(files)
    df = pd.concat(map(pd.read_csv, files), ignore_index=True)
    df.replace(0, np.nan, inplace=True)
    df.dropna()
    AppendUnitPrice(df)
    df.dropna()
    df.to_csv(f"Data\{boroughName}-mergeData.csv")


def AppendUnitPrice(data: pd.DataFrame):
    for index, row in data.iterrows():
        print(index)
        area = row["Land Square Feet"]
        priceTotal = row["Sale Price"]
        unitPrice = priceTotal / area
        data.loc[index, 'Unit Price'] = unitPrice





