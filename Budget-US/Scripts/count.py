# count number of subdirectories in a directory
import os
import json
import gzip
"""
with open('/home/ubuntu/Budget-US/Data/geojson/usa_geojson.json') as f:
    data = json.load(f)
    path = "/mnt/efs/fs1/raw/budget/us/raw/2022-11-01-07-34-59/Location/{},{}/2022-11-01-07-34-59.json.gz"
    for i in range(1000,1001):
        latitude, longitude = data['features'][i]['geometry']['coordinates'][0][0]
        
        # open gzip file
        with gzip.open(path.format(latitude, longitude), 'rt') as f:
            data = json.load(f)
            
            # get vehicleSummaryList from json
            print(data)
           
"""
count = 0

with open('/home/ubuntu/Budget-US/Data/geojson/usa_geojson.json') as f:
    data = json.load(f)
    for i in range(0,len(data['features'])):
        latitude, longitude = data['features'][i]['geometry']['coordinates'][0][0]
    
        path = "/mnt/efs/fs1/raw/budget/us/raw/2022-11-02-15-50-06/Location/{},{}/".format(latitude, longitude)
        # find all files in the directory
        try:
            files = os.listdir(path)
            # iterate over all the files
            for file in files:
                # Check whether file is in text format or not
                if file.endswith(".json.gz"):
                    # open the file in read mode 
                    with gzip.open(path + file, 'rt') as f:
                        data_new = json.load(f)
                        if len(data_new) > 0:
                            count += 1
                            print("File is not empty")
                    
        except:
            print("No files in this directory")
            pass

print(count)

"""
total number of files in the directory - 8218
total number of locations - 16000
total date combinations - 117
storage path - /mnt/efs/fs1/raw/budget/us/raw/2022-11-02-15-50-06/Location/
"""