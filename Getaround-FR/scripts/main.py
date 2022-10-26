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
import datetime
import os
import sys
import gzip

start = time.time()
ip_list = []

request = requests.Session()
retry = Retry(connect=5, backoff_factor=0.1)
adapter = HTTPAdapter(max_retries=retry)
request.mount('http://', adapter)
request.mount('https://', adapter)

Datetime = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

class Scraper:
    def __init__(self):                   
        self.get_car_api = "https://fr.getaround.com/search.json?address={}&address_source=google&administrative_area=Grand%20Est&car_sharing=false&city_display_name={}&country_scope=FR&display_view=list&end_date={}&end_time={}&latitude={}&longitude={}&only_responsive=true&page=1&picked_car_ids=EMPTY&start_date={}&start_time={}&user_interacted_with_car_sharing=false&view_mode=list"
        self.get_fare_api = "https://fr.getaround.com/request_availability?car_id={}&end_date={}&end_time={}&start_date={}&start_time={}&web_version=true"                      
        self.servicable_api  = "https://fr.getaround.com/autocomplete/address?delivery_points_of_interest_only=false&enable_google_places=true&enable_imprecise_addresses=true&input={}"     
        self.category_data_frames = []
        self.fare_list = []
        self.servicable_list = []
        self.storage_path = "/mnt/efs/fs1/raw/getaround/fr/"


    def join_get_car_api(self, address, city, end_date, end_time, start_date, start_time, latitude, longitude):
        return self.get_car_api.format(address, city, end_date, end_time, latitude, longitude, start_date, start_time)
    

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
            print('response.status_code: ', response.status_code, "of", address, "at", ip, "of ServiceableArea")
            if response.status_code == 200:
                self.servicable_list.append(response.json())
            return response.status_code

        except Exception as e:
            print('error: ', e)

    
    def GetCarData(self, address, city, end_date, end_time, start_date, start_time, latitude, longitude, ip):
        try:
            api = self.join_get_car_api(address, city, end_date, end_time, start_date, start_time, latitude, longitude)

            proxies = {
                'http': f'http://{ip}:3128',
                'https': f'http://{ip}:3128'
            }

            retry_counter_cardata = 0
            retry_counter_faredata = 0
            retry_counter_servicablearea = 0
            
            if self.ServiceableArea(address, ip) == 200:
                    response = request.get(api, proxies=proxies)
                    print("ServiceableArea: ", address, response.status_code)

                    while response.status_code != 200:
                        retry_counter_cardata += 1

                        response = request.get(api, proxies=proxies)
                        print("Retrying Car Api", address, response.status_code, ip)

                        time.sleep(random.randint(1, 5))
                        if response.status_code == 200:

                            if (response.status_code == 200):
                                parsed = response.json()
                                print("Successfull Response", response.status_code, "for", address, "of", "GetCarData")
                                self.category_data_frames.append(parsed)

                                if parsed['cars'] == []:
                                    print("No Cars Found")
                                    return

                                for car in parsed['cars']:
                                    car_id = car['id']
                                    self.GetFareData(car_id, end_date, end_time, start_date, start_time, ip)
                            break
                            
                        if retry_counter_cardata == 5:
                            break

                    if (response.status_code == 200):
                        parsed = response.json()
                        print("Successfull Response", response.status_code, "for", address, "of", "GetCarData")
                        self.category_data_frames.append(parsed)
                        if parsed['cars'] == []:
                            return

                        for car in parsed['cars']:
                            car_id = car['id']
                            self.GetFareData(car_id, end_date, end_time, start_date, start_time, ip)
                        
            else:
                print("{}is not serviceable".format(address),"on ip", ip)

            """
            while self.ServiceableArea(address, ip) != 200 and self.ServiceableArea(address, ip) == 429:
                retry_counter_servicablearea += 1
                print("Retrying Service Api: ", retry_counter_servicablearea, "of", ip)

                time.sleep(random.randint(1, 5))
                
                if self.ServiceableArea(address, ip) == 200:
                    response = request.get(api, proxies=proxies)
                    print("ServiceableArea: ", address, response.status_code)

                    while response.status_code != 200:
                        retry_counter_cardata += 1

                        response = request.get(api, proxies=proxies)
                        print("Retrying Car Api", address, response.status_code, ip)

                        time.sleep(random.randint(1, 5))
                        if response.status_code == 200:

                            if (response.status_code == 200):
                                parsed = response.json()
                                print("Successfull Response", response.status_code, "for", address, "of", "GetCarData")
                                self.category_data_frames.append(parsed)

                                if parsed['cars'] == []:
                                    return

                                for car in parsed['cars']:
                                    car_id = car['id']
                                    self.GetFareData(car_id, end_date, end_time, start_date, start_time, ip)
                            break
                            
                        if retry_counter_cardata == 5:
                            break

                    if (response.status_code == 200):
                        parsed = response.json()
                        print("Successfull Response", response.status_code, "for", address, "of", "GetCarData")
                        self.category_data_frames.append(parsed)
                        if parsed['cars'] == []:
                            print("No Cars Found", address, "of", "GetCarData")
                            return

                        for car in parsed['cars']:
                            car_id = car['id']
                            self.GetFareData(car_id, end_date, end_time, start_date, start_time, ip)
                        
                else:
                    print("{}is not serviceable".format(address),"on ip", ip)
                
                if retry_counter_servicablearea == 5:
                    break
            """
        except Exception as e:
            print('error: ', e)
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
                parsed = response.json()
                self.fare_list.append(parsed)   

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
                # clear the data after writing
                self.category_data_frames.clear()
                
                print("CarDataWriter: is written in raw folder", latitude, longitude)

        except Exception as e:
            print('Data Write Error of CarDataWriter: ', e)
    

    def FareDataWriter(self, filename):
        try:
            df = pd.DataFrame(self.fare_list)
            df.to_json(filename, orient='records', lines=True)
        except Exception as e:
            print('Data Write Error of FareDataWriter: ', e)

    
    def ServiceableAreaDataWriter(self, filename):
        try:
            df = pd.DataFrame(self.servicable_list)
            df.to_json(filename, orient='records', lines=True)
        except Exception as e:
            print('Data Write Error of ServiceableAreaDataWriter: ', e)


    def FullCarDataWriter(self, filename):
        try:
            df = pd.DataFrame(self.category_data_frames)
            df.to_json(filename, orient='records', lines=True)
        except Exception as e:
            print('Data Write Error of FullCarDataWriter: ', e)

    
    def create_ec2_instance(self, number_of_instance):
        print("Creating {} EC2 Proxy Instance".format(number_of_instance))
        url = "http://172.31.39.71:8000/create/"

        payload = {
                    "template_name":"SquidProxyServer",
                    "template_version":"3",
                    "rival":"getaround",
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
                    "rival":"getaround",
                    "ownerARN":"arn:aws:iam::047719688069:user/paritosh.tripathi@anakin.company",
                    "region": "ap-south-1"
            }
        response = requests.request("POST", url, json=payload)

        print("All Instances are deleted")


if __name__ == "__main__":
    
    try:
        scraper = Scraper()
        
        df = pd.read_json('Getaround-FR/Data/geojson/google_geoJson.json', orient='records', lines=True) 

        n = 5
        scraper.create_ec2_instance(n)
        time.sleep(5)
        print("Waiting for EC2 instances to start")
 
        try:
            for i in range(0, 1000):
                address = df[0][i]['address']
                city = df[0][i]['address'].split(",")[0].strip()
                end_date = "2022-11-01"
                end_time = "07%3A00"
                start_date = "2022-10-31"
                start_time = "06%3A00"
                latitude = df[0][i]['lat']
                longitude = df[0][i]['lon']
                ip = ip_list[i%n]

                data_chunck = [address, city, end_date, end_time, start_date, start_time, latitude, longitude,ip]
                concc = Concurrency(worker_thread=50)
                concc.run_thread(func=scraper.GetCarData, 
                param_list=[data_chunck], param_name=["address", "city", "end_date", "end_time", "start_date", "start_time", "latitude", "longitude", "ip"])
                scraper.CarDataWriter(latitude, longitude)

            scraper.FullCarDataWriter('{}test_9_car.json'.format(scraper.storage_path))
            scraper.FareDataWriter('{}test_9_fare.json'.format(scraper.storage_path))
            scraper.ServiceableAreaDataWriter('{}test_9_serviceable.json'.format(scraper.storage_path))

        except Exception as e:
            print(e)
            pass

    except KeyboardInterrupt:
        print('KeyboardInterrupt')
        scraper.DeleteInstance()
        scraper.FullCarDataWriter('{}test_9_car.json'.format(scraper.storage_path))
        scraper.FareDataWriter('{}test_9_fare.json'.format(scraper.storage_path))
        scraper.ServiceableAreaDataWriter('{}test_9_serviceable.json'.format(scraper.storage_path))
        print('Data Saved')



end = time.time()

with open('Getaround-FR/Data/time.txt', 'a+') as f:
    Datetime = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    f.write(str(end - start),"Date")
    f.write(Datetime)
    # add new line
    f.write("\n")

