# genrate a json file for france with lattitude and longitude and it respective address

import json
import requests
import pandas as pd
import time

def getGeoJson():
    data = []
    geoJson = json.load(open('Data/data/Geojson/geoJson.json'))
    for i in range(0, len(geoJson['features'])):
        lon, lat = geoJson['features'][i]['geometry']['coordinates'][0][0]

        url = 'https://nominatim.openstreetmap.org/reverse?format=json&lat=' + str(lat) + '&lon=' + str(lon) + '&zoom=18&addressdetails=1' # we are using the nominatim api to get the address from the lattitude and longitude
        response = requests.get(url)
        try:
            if response.status_code == 200:
                print(response.json()['display_name'])
                data.append([{"lat":lat, "lon":lon, "address":response.json()['display_name']}])
                #time.sleep(1)
        except Exception as e:
            print(e)

    df = pd.DataFrame(data)
    df.to_json('Data/geoJson_new_new.json', orient='records', lines=True)

if __name__ == "__main__":
    getGeoJson()