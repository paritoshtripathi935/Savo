from geopy.geocoders import GoogleV3
import json
import requests
import pandas as pd
import time

def getGeoJson():
    data = []
    geoJson = json.load(open('Data/geojson/geojson.json'))
    for i in range(0, len(geoJson['features'])):
        lon, lat = geoJson['features'][i]['geometry']['coordinates'][0][0]

        geolocator = GoogleV3(api_key='AIzaSyBV01MronS_3a0cghnJdbhtRE28x0LYGjc')
        locations = geolocator.reverse(str(lat) + ',' + str(lon))
        if locations:
            print(locations.address, "_------------------------------------",i)
            data.append([{"lat":lat, "lon":lon, "address":locations.address}])
         
    df = pd.DataFrame(data)
    df.to_json('Data/google_geoJson.json', orient='records', lines=True)

if __name__ == "__main__":
    getGeoJson()