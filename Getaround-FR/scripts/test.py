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

start = time.time()

# code for create ec2instance 
ip_list = []
def create_ec2_instance():
    url = "http://172.31.39.71:8000/create/"
    payload = {
                "template_name":"SquidProxyServer",
                "template_version":"3",
                "rival":"getaround",
                "ownerARN":"arn:aws:iam::047719688069:user/paritosh.tripathi@anakin.company",
                "region": "ap-south-1",
                "count":1
    }
    response = requests.request("POST", url, json=payload)
    print("All ec2 instance created")
    for key, value in response.json().items():
        ip_list.append(value['private'])


def DeleteInstance():
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

request = requests.Session()
retry = Retry(connect=5, backoff_factor=0.1)
adapter = HTTPAdapter(max_retries=retry)
request.mount('http://', adapter)
request.mount('https://', adapter)


class Scraper:
    def __init__(self):                   
        self.get_car_api = "https://fr.getaround.com/search.json?address={}&address_source=google&administrative_area=Grand%20Est&car_sharing=false&city_display_name={}&country_scope=FR&display_view=list&end_date={}&end_time={}&latitude={}&longitude={}&only_responsive=true&page=1&picked_car_ids=EMPTY&start_date={}&start_time={}&user_interacted_with_car_sharing=false&view_mode=list"
        self.get_fare_api = "https://fr.getaround.com/request_availability?car_id={}&end_date={}&end_time={}&start_date={}&start_time={}&web_version=true"                      
        self.servicable_api  = "https://fr.getaround.com/autocomplete/address?delivery_points_of_interest_only=false&enable_google_places=true&enable_imprecise_addresses=true&input={}"     
        self.category_data_frames = [1,2]
        self.fare_list = []
        self.service_location = []
        self.storage_path = "/mnt/efs/fs1/raw/getaround/"

    def join_get_car_api(self, address, city, end_date, end_time, start_date, start_time, latitude, longitude):
        return self.get_car_api.format(address, city, end_date, end_time, latitude, longitude, start_date, start_time)
    
    def join_get_fare_api(self, car_id, end_date, end_time, start_date, start_time):
        return self.get_fare_api.format(car_id, end_date, end_time, start_date, start_time)

    def ServiceableArea(self, address):
        try:
            api = self.servicable_api.format(address)
            """
            proxies = {
                'http': f'http://{ip}:3128',
                'https': f'http://{ip}:3128'
            }
            """
            response = request.get(api)
            print("ServiceableArea: ", response.status_code, "of", address)
            self.service_location.append(response.json())
            return response.status_code

        except Exception as e:
            print('error: ', e)

    
    def GetCarData(self, address, city, end_date, end_time, start_date, start_time, latitude, longitude):
        try:
            api = self.join_get_car_api(address, city, end_date, end_time, start_date, start_time, latitude, longitude)
            """
            proxies = {
                'http': f'http://{ip}:3128',
                'https': f'http://{ip}:3128'
            }
            """
            # check if address is serviceable or not
            while self.ServiceableArea(address) != 200:
                response = request.get(api)
                print("check - 1", response.status_code)

                # check if response is 200 or not if not keep trying until it is 200
                while response.status_code != 200:
                    response = request.get(api)
                    print("check - 2", response.status_code)
                    time.sleep(1)
                    if response.status_code == 200:
                        print("check - loop")
                        # if response is 200 then parse the data

                        if (response.status_code == 200):
                            parsed = response.json()
                            print("check - 3")
                            # if there is no car available then return
                            if parsed['cars'] == []:
                                return

                            # if there is car available then parse the data
                            for car in parsed['cars']:
                                car_id = car['id']
                                self.GetFareData(car_id, end_date, end_time, start_date, start_time)
                                print("check - 4")
                        break
                
                # if response is 200 then parse the data
                if (response.status_code == 200):
                    parsed = response.json()
                    print("check - 3")
                    # if there is no car available then return
                    if parsed['cars'] == []:
                        return

                    # if there is car available then parse the data
                    for car in parsed['cars']:
                        car_id = car['id']
                        self.GetFareData(car_id, end_date, end_time, start_date, start_time)
                        print("check - 4")
                    
            else:
                print("Address is not serviceable")

        except Exception as e:
            print('error: ', e)
    
    def GetFareData(self, car_id, end_date, end_time, start_date, start_time):
        try:
            api = self.join_get_fare_api(car_id, end_date, end_time, start_date, start_time)
            """
            proxies = {
                'http': f'http://{ip}:3128',
                'https': f'http://{ip}:3128'
            }
            """
            response = request.get(api)

            if (response.status_code == 200):
                parsed = response.json()
                self.fare_list.append(parsed)   

                print('Sucessful Response: ', 'response.status_code: ', response.status_code, "of", car_id, "at of FareData")
            else:
                print('Unsuccessful Response: ', 'response.status_code: ', response.status_code, "of", car_id, "at of FareData")

        except Exception as e:
            print('error: ', e)


    def CarDataWriter(self, latitude, longitude):
        # raw/{date}{time}/{address}/{lat}_{lon}/{car_id}.json.gz
        # store the data in above format in raw folder
        try:
            Datetime = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

            # make dir self.storage_path/raw/{date}{time}
            if not os.path.exists(self.storage_path + "raw/"):
                os.makedirs(self.storage_path + "raw/")
            
            # make dir self.storage_path/raw/{date}{time}/Location/{lat}_{lon}
            if not os.path.exists(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/"):
                os.makedirs(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/")
            

            # write the data in json.gz form in self.storage_path/raw/{date}{time}/Location/{lat}_{lon}/filename.json.gz
            with gzip.open(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/" + timestamp + ".json.gz", 'wt') as f:
                json.dump(self.category_data_frames, f)
                # clear the data after writing
                self.category_data_frames.clear()
                
                print("CarDataWriter: is written in raw folder")

        except Exception as e:
            print('Data Write Error of CarDataWriter: ', e)



    def FareDataWriter(self, filename):
        try:
            df = pd.DataFrame(self.fare_list)
            df.to_json(filename, orient='records', lines=True)
        except Exception as e:
            print('Data Write Error of FareDataWriter: ', e)

    def ServiceLocationWriter(self, filename):
        try:
            df = pd.DataFrame(self.service_location)
            df.to_json(filename, orient='records', lines=True)
        except Exception as e:
            print('Data Write Error of ServiceLocationWriter: ', e)

if __name__ == "__main__":
    try:
        scraper = Scraper()
        df = pd.read_json('Data/geojson/google_geoJson.json', orient='records', lines=True) 

        #create_ec2_instance()
        #time.sleep(20)
        try:
            for i in range(0, 1):
                address = df[0][i]['address']
                city = df[0][i]['address'].split(",")[0].strip()
                end_date = "2022-11-01"
                end_time = "07%3A00"
                start_date = "2022-10-31"
                start_time = "06%3A00"
                latitude = df[0][i]['lat']
                longitude = df[0][i]['lon']
                #scraper.ServiceableArea(address)
                #ip = random.choice(ip_list)
                #data_chunck = [address, city, end_date, end_time, start_date, start_time, latitude, longitude]

                #concc = Concurrency(worker_thread=50)
                #concc.run_thread(func=scraper.GetCarData, 
                #param_list=[data_chunck], param_name=["address", "city", "end_date", "end_time", "start_date", "start_time", "latitude", "longitude"])
            
                scraper.CarDataWriter(latitude, longitude)

        except Exception as e:
            print(e)
            pass
        #ip_list.clear()
        #DeleteInstance()
        #time.sleep(10)

        #create_ec2_instance()
        #time.sleep(20)
        """
        try:
            df = pd.read_json('/mnt/efs/fs1/raw/getaround/test_6.json', orient='records', lines=True)
            for i in range(0, len(df)):
                try:
                    car_id = df['cars'][i][0]['id']
                    end_date = "2022-11-01"
                    end_time = "07%3A00"
                    start_date = "2022-10-31"
                    start_time = "06%3A00"
                    #ip = random.choice(ip_list)
                    data_chunck = [car_id, end_date, end_time, start_date, start_time]

                    concc = Concurrency(worker_thread=50)
                    concc.run_thread(func=scraper.GetFareData, 
                    param_list=[data_chunck], param_name=["car_id", "end_date", "end_time", "start_date", "start_time"])
                except Exception as e:
                    print(e)
                    pass
            
            scraper.FareDataWriter('/mnt/efs/fs1/raw/getaround/fare_test_5.json')
        
        except Exception as e:
            print(e)
            pass
        """
        #ip_list.clear()
        #DeleteInstance()
        #time.sleep(25)

    except KeyboardInterrupt:
        print('KeyboardInterrupt')
        #DeleteInstance()
        

end = time.time()

print("Total time taken: ", end - start)