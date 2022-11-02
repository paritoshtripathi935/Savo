import requests
from bs4 import BeautifulSoup
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests import get
import pandas as pd
from concurrency import Concurrency
import random
import time
import os
import gzip
import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from datetime import timedelta
import sys
from datetime import date
start = time.time()
ip_list = []

request = requests.Session()
retry = Retry(connect=5, backoff_factor=0.1)
adapter = HTTPAdapter(max_retries=retry)
request.mount('http://', adapter)
request.mount('https://', adapter)

Datetime = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
date = datetime.date.today()

class Scraper:
    def __init__(self):
        self.session_headers = json.load(open('Enterprise-US/Data/headers/reservation.json'))
        self.activate_session_headers = json.load(open('Enterprise-US/Data/headers/activate.json'))
        self.car_headers = json.load(open('Enterprise-US/Data/headers/car.json'))

        self.session_api = "https://prd-east.webapi.enterprise.com/enterprise-ewt/current-session/clear-reservation"
        self.activate_session = "https://prd-east.webapi.enterprise.com/enterprise-ewt/enterprise/reservations/initiate"
        self.car_api = "https://prd-east.webapi.enterprise.com/enterprise-ewt/enterprise/reservations/vehicles/availability"
        self.location_api = "https://prd.location.enterprise.com/enterprise-sls/search/location/enterprise/web/spatial/{}/{}?dto=true&rows=1&cor=US&locale=en_US"

        self.storage_path = "/mnt/efs/fs1/raw/enterprise/usa/"
        self.category_data_frames = []


    def make_session(self, lattitude , longitude, start_date, end_date):
        try:
            response = request.get(self.location_api.format(lattitude, longitude))
            if response.status_code == 200:
                id = response.json()['result'][0]['id']
                address = response.json()['result'][0]['address']['street_addresses'][0]

                if id and address is not None:
                    print("Location Found for", lattitude, longitude, id, address)

                    body = {}

                    activate_body = {
                    "pickupLocation":{
                        "id":id,"name":address,"latitude":lattitude,"longitude":longitude,"type":"BRANCH","locationType":"BRANCH","dateTime":start_date,"countryCode":"US"},
                    "returnLocation":{
                        "id":id,"name":address,"latitude":lattitude,"longitude":longitude,"type":"BRANCH","locationType":"BRANCH","dateTime":end_date,"countryCode":"US"},
                        "contract_number":None,"renter_age":25,"country_of_residence_code":"US","enable_north_american_prepay_rates":False,"sameLocation":True,
                        "view_currency_code":"USD","check_if_no_vehicles_available":True,"check_if_oneway_allowed":True
                    }
                    
                    response = request.post(self.session_api, headers=self.session_headers, json=body)
                    
                    if response.status_code == 200:

                        response = request.post(self.activate_session, headers=self.activate_session_headers, json=activate_body)
                        if response.status_code == 200:
                            print("Session Activated")
                            return True
                        else:
                            print("Session Not Activated")
                            return False
                    else:
                        print("Session Not Created")
                        return False

        except Exception as e:
            print(e)
            print(response.json(), lattitude, longitude)
            print('line number: ', sys.exc_info()[-1].tb_lineno)



    def CarDataWriter(self, latitude, longitude):
        try:

            if not os.path.exists(self.storage_path + "raw/"):
                os.makedirs(self.storage_path + "raw/")
            
            if not os.path.exists(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/"):
                os.makedirs(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/")
            
            with gzip.open(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/" + timestamp + ".json.gz", 'wt') as f:
                json.dump(self.category_data_frames, f)
                self.category_data_frames.clear()
                
                print("CarDataWriter: is written in raw folder", latitude, longitude)

        except Exception as e:
            print('Data Write Error of CarDataWriter: ', e)
            print('line number: ', sys.exc_info()[-1].tb_lineno)



    def get_cars(self, lattitude, longitude, start_date, end_date):
        try:
            self.make_session(lattitude, longitude, start_date, end_date)

            response = request.get(self.car_api, headers=self.car_headers)

            if response.status_code == 200:
                print("Cars Found for", lattitude, longitude)
                self.category_data_frames.append(response.json())
                self.CarDataWriter(lattitude, longitude)
                
            else:
                print("Cars Not Found")
                return False

        except Exception as e:
            print(e)
            print('line number: ', sys.exc_info()[-1].tb_lineno)



if __name__ == "__main__":
    try:
        scraper = Scraper()
        print("Scraper initialized")

        data_chunck = []

        try:
            with open('/home/ubuntu/Paritosh-Scrapers/Enterprise-US/Data/geojson/geojson.json') as f:
                data = json.load(f)
                for i in range(0,len(data['features'])):
                    lattitude, longitude = data['features'][i]['geometry']['coordinates'][0][0]
                    start_date = "2022-11-03T12:00"
                    end_date = "2022-11-04T12:00"
                    scraper.get_cars(lattitude, longitude, start_date, end_date)
                    
                    data_chunck.append([lattitude, longitude, start_date, end_date])
                    
                    if len(data_chunck) == 500:
                        concc = Concurrency(worker_thread=10, worker_process=1)
                        concc.run_thread(func=scraper.get_cars, param_list=data_chunck, param_name=['lattitude', 'longitude', 'start_date', 'end_date'])
                        data_chunck = []
                        time.sleep(5)
                        print("Data Chunck -----------------------------------------------------------------------------")
                    
        except Exception as e:
            print(e)
            pass
                
    except Exception as e:
        print(e)



end = time.time()

with open('Enterprise-US/Data/data.txt', 'a+') as f:
    Datetime = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    f.write(str(end - start))
    f.write(Datetime)
    # add new line
    f.write("\n")

