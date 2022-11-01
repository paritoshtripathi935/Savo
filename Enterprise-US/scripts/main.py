import json
import requests
import os

path = "/mnt/efs/fs1/raw/getaround/uk/raw/"
files = os.listdir(path)
for file in files:
    print(file)
