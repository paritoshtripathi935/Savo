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
        self.headers = json.load(open('Getaround-UK/Data/headers/headers.json'))
        self.get_car_api = "https://uk.getaround.com/search.json?address={}&address_source=google&administrative_area=undefined&car_sharing=false&city_display_name={}&country_scope=GB&display_view=list&end_date={}&end_time={}&latitude={}&longitude={}&only_responsive=true&page={}&picked_car_ids=EMPTY&poi_id=EMPTY&start_date={}&start_time={}&user_interacted_with_car_sharing=false&view_mode=list"
        self.get_fare_api = "https://uk.getaround.com/request_availability?car_id={}&end_date={}&end_time={}&start_date={}&start_time={}&web_version=true"                      
        self.servicable_api  = "https://uk.getaround.com/autocomplete/address?delivery_points_of_interest_only=false&enable_google_places=true&enable_imprecise_addresses=true&input={}"     
        self.category_data_frames = []
        self.fare_list = []
        self.servicable_list = []
        self.fare_new_list = []
        self.storage_path = "/mnt/efs/fs1/raw/getaround/uk/"


    def join_get_car_api(self, address, city, end_date, end_time, start_date, start_time, latitude, longitude, page):
        return self.get_car_api.format(address, city, end_date, end_time, latitude, longitude, page, start_date, start_time)
    

    def join_get_fare_api(self, car_id, end_date, end_time, start_date, start_time):
        return self.get_fare_api.format(car_id, end_date, end_time, start_date, start_time)


    def ServiceableArea(self, address, ip):
        try:
            api = self.servicable_api.format(address)

            proxies = {
                'http': f'http://{ip}:3128',
                'https': f'http://{ip}:3128'
            }
            response = request.get(api, proxies=proxies)
            #print('response.status_code: ', response.status_code, "of", address, "at", ip, "of ServiceableArea")
            if response.status_code == 200:
                return response.status_code

        except Exception as e:
            print('error: ', e)

    
    def GetCarData(self, address, city, end_date, end_time, start_date, start_time, latitude, longitude, ip,page):
        try:
            api = self.join_get_car_api(address, city, end_date, end_time, start_date, start_time, latitude, longitude, page)

            proxies = {
                'http': f'http://{ip}:3128',
                'https': f'http://{ip}:3128'
            }

            retry_counter_cardata = 0
            if self.ServiceableArea(address, ip) == 200:
                    time.sleep(1)
                    response = request.get(api, proxies=proxies, headers=self.headers)
                    time.sleep(1)
                    print("ServiceableArea: ", address, response.status_code)

                    
                    while response.status_code != 200:
                        retry_counter_cardata += 1

                        response = request.get(api, proxies=proxies, headers=self.headers)
                        print("Retrying Car Api", address, response.status_code, ip)

                        time.sleep(random.randint(1, 5))
                        if response.status_code == 200:

                            if (response.status_code == 200):
                                parsed = response.json()
                                print("Successfull Response", response.status_code, "for", address, "of", "GetCarData")
                                
                                if parsed['cars'] == []:
                                    print("No Cars Found")
                                    return

                                for car in parsed['cars']:
                                    self.category_data_frames.append(parsed)
                                    #car_id = car['id']
                                    self.CarDataWriter(latitude=latitude, longitude=longitude)

                            break
                         
                        if retry_counter_cardata == 5:
                            break
                    
                    if (response.status_code == 200):
                        parsed = response.json()
                        print("Successfull Response", response.status_code, "for", address, "of", "GetCarData")

                        if parsed['cars'] == []:
                            print("No Cars Found")
                            return

                        for car in parsed['cars']:
                            self.category_data_frames.append(parsed)
                            self.CarDataWriter(latitude=latitude, longitude=longitude)

                    else:
                        print("Failed Response", response.status_code, "for", address, "of", "GetCarData")
                        return
                        
                        
            else:
                print("{}is not serviceable".format(address),"on ip", ip)

        except Exception as e:
        #   print line number and error
            print('error: ', e)
            print('line number: ', sys.exc_info()[-1].tb_lineno)
            pass
    

    def GetFareData(self, car_id, end_date, end_time, start_date, start_time, ip):
        try:
            api = self.join_get_fare_api(car_id, end_date, end_time, start_date, start_time)

            proxies = {
                'http': f'http://{ip}:3128',
                'https': f'http://{ip}:3128'
            }

            response = request.get(api, proxies=proxies)

            if (response.status_code == 200):
                print('Sucessful Response: ', 'response.status_code: ', response.status_code, "of", car_id, "at", ip, "of FareData")
                return response.status_code
            else:
                print('Unsuccessful Response: ', 'response.status_code: ', response.status_code, "of", car_id, "at", ip, "of FareData")
                return response.status_code

        except Exception as e:
            print('error: ', e)

    
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


    def create_ec2_instance(self, number_of_instance):
        print("Creating {} EC2 Proxy Instance".format(number_of_instance))
        url = "http://172.31.39.71:8000/create/"

        payload = {
                    "template_name":"SquidProxyServer",
                    "template_version":"3",
                    "rival":"getaround-uk",
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
                    "rival":"getaround-uk",
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

    def date_generator(self):
        start_date = date.today()
        # start date is tommorow
        start_date = start_date + timedelta(days=1)
        start_date_gap = [ 0, 1, 2, 3, 4, 5, 6, 7, 14, 21, 28, 56, 84]
        end_date_gap =  [1, 2, 3, 4, 5, 6, 7, 14, 28]
        date_list = []
        for x in start_date_gap:
            new_start_date = start_date + timedelta(days=x)
            for y in end_date_gap:
                new_end_date = new_start_date + timedelta(days=y)
                date_list.append([new_start_date, new_end_date])
        return date_list

if __name__ == "__main__":
    try:
        scraper = Scraper()
        
        df = pd.read_json('Getaround-UK/Data/geojson/google_geoJson.json', orient='records', lines=True) 
        n = 5
        scraper.create_ec2_instance(n)

        time.sleep(5)
        print("Waiting for EC2 instances to start")
        data_chunck = []

        try:
            date_list = scraper.date_generator()

            
            for i in range(0, len(df)):
                address = df[0][i]['address']
                city = df[0][i]['address'].split(",")[0].strip()
                end_date = "2022-11-04"
                end_time = "07%3A00"
                start_date = "2022-11-02"
                start_time = "07%3A00"
                latitude = df[0][i]['lat']
                longitude = df[0][i]['lon']
                ip = ip_list[i%n]
                page = 1
                
                #scraper.GetCarData(address, city, end_date, end_time, start_date, start_time, latitude, longitude, ip, page)
                
                data_chunck.append([address, city, end_date, end_time, start_date, start_time, latitude, longitude, ip, page])
                
                
                if len(data_chunck) == 500:
                    concc = Concurrency(worker_thread=5, worker_process=1)
                    concc.run_thread(func=scraper.GetCarData, param_list=data_chunck, param_name=["address", "city", "end_date", "end_time", "start_date", "start_time", "latitude", "longitude", "ip", "page"])
                    data_chunck = []
                    scraper.ipRotation(n)
                    time.sleep(5)
                    print("Rotation Completed-----------------------------------------------------------------------------")
                
            print(len(scraper.category_data_frames))

        except Exception as e:
            print(e)
            pass
        
        scraper.DeleteInstance()

    except KeyboardInterrupt:
        print('KeyboardInterrupt')
        scraper.DeleteInstance()
        print('Data Saved')
        pass    



end = time.time()

with open('Getaround-UK/Data/time.txt', 'a+') as f:
    Datetime = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    f.write(str(end - start))
    f.write(Datetime)
    # add new line
    f.write("\n")

