# count number of subdirectories in a directory

import os
import json
import gzip
import pandas as pd

#df = pd.read_json('Enterprise-US/Data/geojson/geojson.json', orient='records', lines=True) 
path = "/mnt/efs/fs1/raw/enterprise/usa/raw/2022-11-02-10-22-34/Location/"
dirs = os.listdir(path)
count = 0
for file in dirs:
        if os.path.isdir(os.path.join(path, file)):
            count += 1

print(count)
"""
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