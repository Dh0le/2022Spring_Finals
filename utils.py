import pandas as pd
import json
import numpy as np
import glob
import os
import doctest

WORKING_DIR = "Data/"
START_YEAR = 2012
END_YEAR = 2019


def get_neighborhood() -> pd.DataFrame:
    """Return a pandas dataframe that contains Neighborhood information"""
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
    """
    Calculate and return the neighborhood as string with given borough and location
    >>> neighborhoods = get_neighborhood()
    >>> find_neighborhood((40.895437, -73.905643), 'BRONX', neighborhoods)
    'Fieldston'
    """
    bor = neighborhoods.loc[neighborhoods.Borough == borough, ['Borough', 'Neighborhood', 'Latitude', 'Longitude']]
    bor['Distance'] = np.square(location[0] - bor['Latitude']) + np.square(location[1] - bor['Longitude'])
    min_index = bor['Distance'].idxmin()
    return bor.loc[min_index, 'Neighborhood']


def attach_NBH(data: pd.DataFrame, crime: bool, neighborhoods: pd.DataFrame):
    """
    attach neighborhood data into corresponding data
    >>> d = {'LATITUDE':[40.697540],'LONGITUDE':[-73.983120]}
    >>> neighborhoods = get_neighborhood()
    >>> df = pd.DataFrame(data)
    >>> attach_NBH(df, False, neighborhoods)
    >>> print(d.loc[0, 'NBH'])
    'Vinegar Hill'
    """
    for index, row in data.iterrows():
        if crime:
            location = (row['Latitude'], row['Longitude'])
            boro = getNameForBorough(row['ARREST_BORO'])
        else:
            location = (row['LATITUDE'], row['LONGITUDE'])
            boro = row['BOROUGH']
        nbh = find_neighborhood(location, boro, neighborhoods)
        data.loc[index, 'NBH'] = nbh


def getNameForBorough(letter: str) -> str:
    """
    Covert Initial into Borough full name
    >>> getNameForBorough('M')
    'MANHATTAN'
    """
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
    """Merge the real estate data file in working directory for given borough"""
    files = os.path.join(f"Data\{boroughName}", f"*.csv")
    print(files)
    files = glob.glob(files)
    print(files)
    df = pd.concat(map(pd.read_csv, files), ignore_index=True)
    df.replace(0, np.nan, inplace=True)
    df.dropna()
    AppendUnitPrice(df)
    df.dropna()
    df.to_csv(f"{WORKING_DIR}{boroughName}-mergeData.csv")


def AppendUnitPrice(data: pd.DataFrame):
    """Calculate unit price for real estate data and attach to dataframe"""
    for index, row in data.iterrows():
        print(index)
        area = row["Land Square Feet"]
        priceTotal = row["Sale Price"]
        unitPrice = priceTotal / area
        data.loc[index, 'Unit Price'] = unitPrice


def price_change_rate_by_year(df: pd.DataFrame, boro: str) -> pd.DataFrame:
    """Calculate the real estate sales price change rate by year in range of given years."""
    res_df = pd.DataFrame(columns=['Borough', 'NBH', 'Overall_Change_Rate'])
    boro_change_rate = get_change_rate(df.loc[df['Sale Date'].str.contains('2012')]['Unit Price'].mean(),
                                       df.loc[df['Sale Date'].str.contains('2019')]['Unit Price'].mean())
    for nbh in df['Neighborhood'].unique():
        this_nbh = df[df['Neighborhood'] == nbh]  # number of collisions or crimes in this neighborhood
        data = [boro, nbh]

        # get the change rate of this year
        change_rate = get_change_rate(this_nbh.loc[this_nbh['Sale Date'].str.contains('2012')]['Unit Price'].mean(),
                                      this_nbh.loc[this_nbh['Sale Date'].str.contains('2019')][
                                          'Unit Price'].mean()) - boro_change_rate
        data.append(change_rate)

        # from the starting year to the end year
        for i in range(START_YEAR, END_YEAR):
            boro_change_rate_this_year = get_change_rate(
                df.loc[df['Sale Date'].str.contains(str(i))]['Unit Price'].mean(),
                df.loc[df['Sale Date'].str.contains(str(i + 1))]['Unit Price'].mean())
            year_change_rate = get_change_rate(
                this_nbh.loc[this_nbh['Sale Date'].str.contains(str(i))]['Unit Price'].mean(),
                this_nbh.loc[this_nbh['Sale Date'].str.contains(str(i + 1))][
                    'Unit Price'].mean()) - boro_change_rate_this_year

            # the column name
            label = str(i) + '-' + str(i + 1)
            if label not in res_df.columns:
                res_df.insert(res_df.shape[1], label, np.nan)
            data.append(year_change_rate)

        # add data to result dataframe
        if res_df.empty:
            res_df.loc[0] = data
        else:
            res_df.loc[res_df.index.max() + 1] = data
    return res_df


def change_rate_by_year(df: pd.DataFrame, boundary: int):
    """Calculate the change rate over collision and crime"""
    res_df = pd.DataFrame(columns=['Borough', 'NBH', 'Overall_Change_Rate'])
    NBH_type = df.columns[2]
    df.iloc[:, 2:] = df.iloc[:, 2:].apply(pd.to_numeric)
    for boro in df['Borough'].unique():
        this_boro = df[df['Borough'] == boro]  # number of collisions or crimes in this borough
        boro_change_rate = get_change_rate(this_boro.loc[this_boro['Year'] == START_YEAR].iloc[:, 2].mean(),
                                           this_boro.loc[this_boro['Year'] == END_YEAR].iloc[:, 2].mean())
        # print(boro_change_rate)
        for nbh in this_boro['NBH'].unique():
            this_nbh = this_boro[this_boro['NBH'] == nbh]  # number of collisions or crimes in this neighborhood

            # we set a boundary to choose data because if the number is too small, the change rate would be too high
            # to have realistic meanings
            if (this_nbh.iloc[:, [2]].max() < boundary).bool():
                break
            data = [boro, nbh]

            # get the change rate of this year
            change_rate = get_change_rate(this_nbh.loc[this_nbh.index[0]][2],
                                          this_nbh.loc[this_nbh.index[-1]][2]) - boro_change_rate
            data.append(change_rate)

            # from the starting year to the end year
            for i in range(START_YEAR, END_YEAR):
                boro_change_rate_year = get_change_rate(this_boro.loc[this_boro['Year'] == i].iloc[:, 2].mean(),
                                                        this_boro.loc[this_boro['Year'] == i + 1].iloc[:, 2].mean())
                # print(boro_change_rate_year)
                year_change_rate = get_change_rate(int(this_nbh.loc[this_nbh['Year'] == i, NBH_type]), int(
                    this_nbh.loc[this_nbh['Year'] == i + 1, NBH_type])) - boro_change_rate_year

                # the column name
                label = str(i) + '-' + str(i + 1)
                if label not in res_df.columns:
                    res_df.insert(res_df.shape[1], label, 0)
                data.append(year_change_rate)

            # add data to result dataframe
            if res_df.empty:
                res_df.loc[0] = data
            else:
                res_df.loc[res_df.index.max() + 1] = data
    return res_df


def find_tar_nbh(df: pd.DataFrame):
    """find target neighborhood in given dataframe"""
    target_nbh = pd.DataFrame(columns=df.columns)
    for boro in df['Borough'].unique():
        this_boro = df[df['Borough'] == boro].copy()

        # sort the change rate
        this_boro.sort_values(by=['Overall_Change_Rate'], inplace=True)
        this_boro = this_boro.reset_index()

        # for each borough, the target neighborhoods are whose crime rate and collision rate increased the most and decreased the most
        if target_nbh.empty:
            target_nbh.loc[0] = this_boro.iloc[0]
            target_nbh.loc[1] = this_boro.iloc[-1]
        else:
            target_nbh.loc[target_nbh.index.max() + 1] = this_boro.iloc[0]
            target_nbh.loc[target_nbh.index.max() + 1] = this_boro.iloc[-1]
    return target_nbh


def get_change_rate(start: int, end: int) -> float:
    """
    return the change rate
    >>> get_change_rate(2, 1)
    1.0
    """
    if start == 0:
        return end
    return (end - start) / start


def numCollisions(collisionData_NBH: pd.DataFrame) -> pd.DataFrame:
    """find number of collisions in each neighborhood and return the dataframe"""
    nbh_collisions = pd.DataFrame(
        columns=['Borough', 'NBH', 'Collisions', 'Year'])  # the number of crimes in each neighborhood from 2012 to 2019
    collision_boros = collisionData_NBH['BOROUGH'].unique()
    for boro in collision_boros:
        collision_this_boro = collisionData_NBH[collisionData_NBH['BOROUGH'] == boro]  # collisions in this borough
        collision_nbhs = collision_this_boro['NBH'].unique()
        for nbh in collision_nbhs:
            collision_this_nbh = collision_this_boro[
                collision_this_boro['NBH'] == nbh]  # collisions in this neighborhood
            # add data from 2012 to 2019
            for i in range(2012, 2020):
                num = get_collision_year(collision_this_nbh, str(i))
                # the data in 2012 starts in July. We should mutiply number of collisions in this year with 2 to estimate the whole year.
                if i == 2012:
                    num *= 2
                if nbh_collisions.empty:
                    nbh_collisions.loc[0] = [boro, nbh, num, i]
                else:
                    nbh_collisions.loc[nbh_collisions.index.max() + 1] = [boro, nbh, num, i]
    return nbh_collisions


def get_collision_year(df: pd.DataFrame, year: str):
    """process the year data of collision and return the number of collisions in this year"""
    mask = (df['CRASH DATE'] >= year + '-01-01') & (df['CRASH DATE'] <= year + '-12-31')
    return len(df.loc[mask])


def numCrimes(crimeData_NBH: pd.DataFrame) -> pd.DataFrame:
    """calculate the number of crime in given dataframe and return the dataframe"""
    nbh_crimes = pd.DataFrame(
        columns=['Borough', 'NBH', 'Crimes', 'Year'])  # the number of crimes in each neighborhood from 2012 to 2019
    crime_boros = crimeData_NBH['ARREST_BORO'].unique()
    for boro in crime_boros:
        crime_this_boro = crimeData_NBH[crimeData_NBH['ARREST_BORO'] == boro]  # crimes in this borough
        crime_nbhs = crime_this_boro['NBH'].unique()
        for nbh in crime_nbhs:
            crime_this_nbh = crime_this_boro[crime_this_boro['NBH'] == nbh]  # crimes in this neighborhood
            # add data from 2012 to 2019
            for i in range(2012, 2020):
                if nbh_crimes.empty:
                    nbh_crimes.loc[0] = [boro, nbh, get_crimes_year(crime_this_nbh, str(i)), i]
                else:
                    nbh_crimes.loc[nbh_crimes.index.max() + 1] = [boro, nbh, get_crimes_year(crime_this_nbh, str(i)), i]
    return nbh_crimes


def get_crimes_year(df: pd.DataFrame, year: str) -> int:
    """process the year data of crime and return the number of crimes in this year"""
    mask = (df['ARREST_DATE'] >= year + '-01-01') & (df['ARREST_DATE'] <= year + '-12-31')
    return len(df.loc[mask])
