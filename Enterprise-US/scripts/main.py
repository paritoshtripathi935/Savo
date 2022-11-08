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
retry = Retry(connect=5, backoff_factor=0.5)
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
        self.select_car = json.load(open('Enterprise-US/Data/headers/select_car.json'))
        #self.list_Vehicles_headers = json.load(open('Enterprise-US/Data/headers/listVehicles.json'))
        self.fare_headers = json.load(open('Enterprise-US/Data/headers/fare_headers.json'))

        self.session_api = "https://prd-east.webapi.enterprise.com/enterprise-ewt/current-session/clear-reservation"
        self.activate_session = "https://prd-east.webapi.enterprise.com/enterprise-ewt/enterprise/reservations/initiate"
        self.location_api = "https://prd.location.enterprise.com/enterprise-sls/search/location/enterprise/web/spatial/{}/{}?dto=true&rows=1&cor=US&locale=en_US"
        self.car_api = "https://prd-east.webapi.enterprise.com/enterprise-ewt/enterprise/reservations/vehicles/availability"
        self.select_car_api = "https://prd-east.webapi.enterprise.com/enterprise-ewt/reservations/selectCarClass"
        self.fare_conditions_api = "https://prd-east.webapi.enterprise.com/enterprise-ewt/current-session?reservationFlow=true&route=extras"

        self.storage_path = "/mnt/efs/fs1/raw/enterprise/usa/"
        self.car_class_code = []
        self.category_data_frames = []
        self.select_car_list = []
        self.fare_list = []


    def make_session(self, lattitude , longitude, start_date, end_date, ip):
        proxies = {
                'http': f'http://{ip}:3128',
                'https': f'http://{ip}:3128'
            }

        try:
            response = requests.get(self.location_api.format(lattitude, longitude))
            #print(response.status_code)
            time.sleep(1)
            if response.status_code == 200:

                if response.json()['result']:
                    id = response.json()['result'][0]['id']
                    address = response.json()['result'][0]['address']['street_addresses'][0]
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
                    time.sleep(10)
                    if response.status_code == 200:

                        response = request.post(self.activate_session, headers=self.activate_session_headers, json=activate_body)
                        time.sleep(5)
                        if response.status_code == 200:
                            print("Session Activated")
                            return True

                        else:
                            print("Session Not Activated")
                            return False

                    else:
                        print("Session Not Created")
                        return False

                else:
                    print("Location Not Found for", lattitude, longitude)
                    return False
                    #print(response.json()['result'])

            else:
                print("Location API Failed")
                return False

        except Exception as e:
            print(e)
            print('line number: ', sys.exc_info()[-1].tb_lineno)
            pass



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
            
            with gzip.open(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/" + "select-car" + ".json.gz", 'wt') as f:
                json.dump(self.select_car_list, f)
                self.select_car_list.clear()
                
                print("SelectDataWriter: is written in raw folder", latitude, longitude)

            with gzip.open(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/" + "fare" + ".json.gz", 'wt') as f:
                json.dump(self.fare_list, f)
                self.fare_list.clear()
                
                print("FareDataWriter: is written in raw folder", latitude, longitude)

        except Exception as e:
            print('Data Write Error of CarDataWriter: ', e)
            print('line number: ', sys.exc_info()[-1].tb_lineno)


    def get_cars(self, lattitude, longitude, start_date, end_date,ip):
        try:
            if self.make_session(lattitude, longitude, start_date, end_date, ip) == True:   
                time.sleep(1)
                response = request.get(self.car_api, headers=self.car_headers)

                if response.status_code == 200:
                    try:
                        if response.json() != {"code": "#timedout", "message": "link to SESSION_TIMEDOUT", "parameters": [], "type": "HREF", "displayAs": "ALERT", "defaultMessage": "link to SESSION_TIMEDOUT"}:
                            self.category_data_frames.append(response.json())
                            print("Cars Found")

                            for i in range(0,len(response.json())):
                                body = {"car_class_code":response.json()['availablecars'][i]['code']}

                                response = request.post(self.select_car_api, headers=self.select_car, json=body)
                                #print(response.json())
                                time.sleep(1)

                                if response.json() != []:
                                    print("Select Car API Success")
                                    self.select_car_list.append(response.json())

                                    response = request.get(self.fare_conditions_api, headers=self.fare_headers)

                                    if response.status_code == 200:
                                        self.fare_list.append(response.json())
                                        print("Fare Conditions Found")
                                        self.CarDataWriter(lattitude, longitude)
                        
                                    else:
                                        print("Fare Conditions Not Found")
                                    
                        else:
                            print("Timeout Error")
                            pass

                    except Exception as e:
                        print("Cars Not Found", e)
                        pass
                else:
                    print("Car Data Not Found")
                
        except Exception as e:
            print(e)
            print('line number: ', sys.exc_info()[-1].tb_lineno)
    

    def create_ec2_instance(self, number_of_instance):
        print("Creating {} EC2 Proxy Instance".format(number_of_instance))
        url = "http://172.31.39.71:8000/create/"

        payload = {
                    "template_name":"SquidProxyServer",
                    "template_version":"3",
                    "rival":"enterprise-usa",
                    "ownerARN":"arn:aws:iam::047719688069:user/paritosh.tripathi@anakin.company",
                    "region": "ap-south-1",
                    "count":number_of_instance
        }
        response = requests.request("POST", url, json=payload)

        for key, value in response.json().items():
            ip_list.append(value['private'])
        
        print("All EC2 Proxy Instance created")


    def DeleteInstance(self):
        print("Deleting EC2 Proxy instance")
        url = "http://172.31.39.71:8000/terminate/"

        payload = {
                    "template_name":"SquidProxyServer",
                    "template_version":"3",
                    "rival":"enterprise-usa",
                    "ownerARN":"arn:aws:iam::047719688069:user/paritosh.tripathi@anakin.company",
                    "region": "ap-south-1"
            }
        response = requests.request("POST", url, json=payload)

        print("All Instances are deleted")


    def ipRotation(self,n):
        print("IP Rotation")
        self.DeleteInstance()
        ip_list.clear()
        self.create_ec2_instance(n)
        print("IP Rotation Completed")



if __name__ == "__main__":
    try:
        scraper = Scraper()
        print("Scraper initialized")
        
        n = 2
        #scraper.create_ec2_instance(n)
        data_chunck = []

        try:
            with open('/home/ubuntu/Paritosh-Scrapers/Enterprise-US/Data/geojson/geojson.json') as f:
                data = json.load(f)
                for i in range(0,len(data['features'])):
                    lattitude, longitude = data['features'][i]['geometry']['coordinates'][0][0]
                    start_date = "2022-11-08T12:00"
                    end_date = "2022-11-09T12:00"
                    #ip = ip_list[i%n]
                    # pass 
                    ip = "you" # test

                    #scraper.get_cars(longitude, lattitude, start_date, end_date, ip)
                    
                    data_chunck.append([longitude, lattitude, start_date, end_date, ip])
                    count = 0
                    start_time = time.time()
                    if len(data_chunck) == 250:
                        concc = Concurrency(worker_thread=5, worker_process=1)
                        concc.run_thread(func=scraper.get_cars, param_list=data_chunck, param_name=['lattitude', 'longitude', 'start_date', 'end_date', 'ip'])
                        data_chunck = []
                        time.sleep(5)
                        count += 1
                        end_time = time.time()
                        print("Data Chunck", count, "out of", len(data['features'])/250, "completed")
                        
                        with open('Enterprise-US/Data/progress.txt', 'a+') as f:
                            f.write("Data Chunck {} out of {} completed, time taken {}".format(count, 16000/250, end_time-start_time))

                        print("Data Chunck -----------------------------------------------------------------------------")
                    
            #scraper.DeleteInstance()
        
        except Exception as e:
            print(e)
            print('line number: ', sys.exc_info()[-1].tb_lineno)
            pass
        
        #scraper.DeleteInstance()   
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        scraper.DeleteInstance()
        pass



end = time.time()

with open('Enterprise-US/Data/data.txt', 'a+') as f:
    Datetime = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    f.write(str(end - start))
    f.write(Datetime)
    # add new line
    f.write("\n")

