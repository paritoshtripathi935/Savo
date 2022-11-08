import json
import gzip
import pandas as pd
cars = []
location = []
count = 0
with open('/home/ubuntu/Paritosh-Scrapers/Budget-US/Data/geojson/usa_geojson.json') as f:
    data = json.load(f)

    for k in range(0, len(data['features'])):
        latitude,longitude = data['features'][k]['geometry']['coordinates'][0][0]
        path = "/mnt/efs/fs1/raw/enterprise/usa/raw/2022-11-07-11-13-09/Location/{},{}/2022-11-07-11-13-09.json.gz"
        try:
            with gzip.open(path.format(longitude, latitude), 'rt') as f:
                data_new = json.load(f)
                count = count + 1
                print(count)
                '''
                print(len(data_new[0]['availablecars']))
                cars.append(len(data_new[0]['availablecars']))
                location.append(latitude)
                '''
        except:

            print("error")
            pass
print(count)
# print sum of list sum
# print(sum(cars))
# print(len(location))
"""
132925 total cars in the US
4246 locations
total_runtime = 666 minutes

num = []
car_id_new = []
for i in range(0, len(df)):
    latitude = df[0][i]['lat']
    longitude = df[0][i]['lon']
    try:
        with gzip.open(path.format(latitude, longitude), 'rt') as f:
            data = json.load(f)
            print(len(data[0]['cars']))
            car_id = len(data[0]['cars'])
            num.append(car_id)
            for j in range(0, car_id):
                car_id_new.append(data[0]['cars'][j]['id'])

    except:
        print("error")
        pass

# count unique car id
print(len(set(car_id_new)))
print(sum(num))
print(len(df))
"""
"""
546 uniqye car id
2186 total cars
9059 addresses
69.883 minutes runtime

    path = "/mnt/efs/fs1/raw/budget/us/raw/2022-11-01-07-34-59/Location/{},{}"
    dirs = os.listdir(path)
    count = 0
    for file in dirs:
        if os.path.isdir(os.path.join(path, file)):
            count += 1
    print(count)
"""