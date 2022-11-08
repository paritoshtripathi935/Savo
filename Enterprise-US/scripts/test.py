import json
import gzip

with open('/home/ubuntu/Paritosh-Scrapers/Budget-US/Data/geojson/usa_geojson.json') as f:
    data = json.load(f)

    for k in range(0,12):
        cor = data['features'][k]['geometry']['coordinates']
        lat, lon = cor[0][0]
        print(lon, lat)
        
        path = "/mnt/efs/fs1/raw/enterprise/usa/raw/2022-11-03-13-05-32/Location/{},{}/2022-11-03-13-05-32.json.gz"
        try:
            with gzip.open(path.format(lon, lat), 'rt') as f:
                data_new = json.load(f)
                print(len(data_new[0]['availablecars']))

        except:
            print("error")