import pandas as pd
import numpy as np

# read /mnt/efs/fs1/raw/getaround/fr/test_9_serviceable.json
df = pd.read_json('/mnt/efs/fs1/raw/getaround/fr/test_9_serviceable.json', lines=True)
unique_ids = []
try:
    for i in range(0, len(df)):
        try:
            id = df['results'][i][0]['id']
            print(id)
            unique_ids.append(id)

        except:
            pass        
except:
    pass

# get unique number of ids in the list
unique_ids = list(set(unique_ids))
print(len(unique_ids))
