import pandas as pd
import json
import numpy as np
import threading
from queue import Queue
import time

def get_neighborhood() -> pd.DataFrame:
    with open('D:/Assignments/IS597/2022Spring_Finals/Data/nyu_2451_34572-geojson.json') as json_data:
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
        
        if neighborhoods.empty:
            neighborhoods.loc[0] = [borough.upper(), neighborhood_name, neighborhood_lat, neighborhood_lon]
        else: 
            neighborhoods.loc[neighborhoods.index.max()] = [borough.upper(), neighborhood_name, neighborhood_lat, neighborhood_lon]
    return neighborhoods

def find_neighborhood(location: tuple, borough: str, neighborhoods: pd.DataFrame) ->str:
    bor = neighborhoods.loc[neighborhoods.Borough == borough, ['Borough', 'Neighborhood', 'Latitude', 'Longitude']]
    bor['Distance'] = np.square(location[0] - bor['Latitude']) + np.square(location[1] - bor['Longitude'])
    min_index = bor['Distance'].idxmin()
    return bor.loc[min_index, 'Neighborhood']


def getNameforBorough(letter:str)->str:
    if letter == 'M':
        borough= 'MANHATTAN'
    elif letter =='B':
        borough= 'BRONX'
    elif letter == 'K':
        borough= 'BROOKLYN'
    elif letter == 'S':
        borough= 'STATEN ISLAND'
    else:
        borough = 'QUEENS'
    return borough


def put_data(q: Queue , df: pd.DataFrame):
    for index, row in crimeData_2012_2016.iterrows():
        q.put((index, row))

def get_data(q: Queue, neighborhoods: pd.DataFrame, df: pd.DataFrame):
    global a
    while True:
        if q.empty():
            break
        index, row = q.get()
        location = (row['Latitude'],row['Longitude'])
        boro = getNameforBorough(row['ARREST_BORO'])
        nbh = find_neighborhood(location, boro, neighborhoods)
        lock.acquire()
        a += 1
        crimeData_2012_2016.loc[index,'NBH'] = nbh
        lock.release()
        if a % 1000 == 0:
            print(a)


    a = 0

if __name__ == "__main__":
    crimeData = pd.read_csv("D:/Assignments/IS597/2022Spring_Finals/Data/NYPD_Arrests_Data__Historic_.csv").dropna()
    neighborhoods = get_neighborhood()
    
    crimeData['ARREST_DATE'] = pd.to_datetime(crimeData['ARREST_DATE'],format="%m/%d/%Y")
    mask = (crimeData['ARREST_DATE'] >= '2012-01-01') & (crimeData['ARREST_DATE'] <= '2016-12-31')
    crimeData_2012_2016 = crimeData.loc[mask].copy()

    q = Queue(maxsize=0)
    lock = threading.Lock()

    th1 = threading.Thread(target=put_data, args=(q, crimeData_2012_2016))
    threads = []

    th1.start()
    for i in range(25):
        th = threading.Thread(target=get_data, args=(q, neighborhoods, crimeData_2012_2016))
        threads.append(th)
        th.start()
        time.sleep(0.2)
    
    th1.join()
    for t in threads:
        t.join()
    
    crimeData_2012_2016.to_csv('D:/Assignments/IS597/2022Spring_Finals/Data/crimeData_2012_2016.csv')
    print(crimeData_2012_2016['NBH'].head())



