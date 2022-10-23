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

start = time.time()

# code for create ec2instance 
ip_list = []
def create_ec2_instance(number_of_instance):
    print("Creating ec2 instance", number_of_instance)
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
    print("All ec2 instance created")
    for key, value in response.json().items():
        ip_list.append(value['private'])


def DeleteInstance():
    print("Deleting ec2 instance")
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
        self.category_data_frames = []
        self.fare_list = []


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

            if self.ServiceableArea(address, ip) == 200:
                response = request.get(api, proxies=proxies)
                print("ServiceableArea: ", address, response.status_code)

                while response.status_code != 200:
                    response = request.get(api, proxies=proxies)
                    print("Retrying: ", address, response.status_code)
                    time.sleep(1)
                    if response.status_code == 200:

                        if (response.status_code == 200):
                            parsed = response.json()
                            print("Successfull Response", response.status_code, "for", address, "of", "GetCarData")
                            self.category_data_frames.append(parsed)

                            if parsed['cars'] == []:
                                return

                            for car in parsed['cars']:
                                car_id = car['id']
                                self.GetFareData(car_id, end_date, end_time, start_date, start_time)
                                print("Successfull Response", response.status_code, "for car_id", car_id, "of", "GetFareData")

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
                        print("Successfull Response", response.status_code, "for car_id", car_id, "of", "GetFareData")
                    
            else:
                print("Address is not serviceable")

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
            else:
                print('Unsuccessful Response: ', 'response.status_code: ', response.status_code, "of", car_id, "at", ip, "of FareData")

        except Exception as e:
            print('error: ', e)


    def CarDataWriter(self, filename):
        try:
            df = pd.DataFrame(self.category_data_frames)
            df.to_json(filename, orient='records', lines=True)
        
        except Exception as e:
            print('Data Write Error of CarDataWriter: ', e)
    

    def FareDataWriter(self, filename):
        try:
            df = pd.DataFrame(self.fare_list)
            df.to_json(filename, orient='records', lines=True)
        except Exception as e:
            print('Data Write Error of FareDataWriter: ', e)


if __name__ == "__main__":
    try:
        scraper = Scraper()
        df = pd.read_json('Data/geojson/google_geoJson.json', orient='records', lines=True) 
        n = 3  # number of ec2 instances
        create_ec2_instance(n)
        time.sleep(5)
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
                ip = random.choice(ip_list)
                data_chunck = [address, city, end_date, end_time, start_date, start_time, latitude, longitude,ip]

                concc = Concurrency(worker_thread=50)
                concc.run_thread(func=scraper.GetCarData, 
                param_list=[data_chunck], param_name=["address", "city", "end_date", "end_time", "start_date", "start_time", "latitude", "longitude", "ip"])
            
            scraper.CarDataWriter('/mnt/efs/fs1/raw/getaround/test_8.json')
            scraper.FareDataWriter('/mnt/efs/fs1/raw/getaround/test_8_fare.json')

        except Exception as e:
            print(e)
            pass
        ip_list.clear()
        DeleteInstance()
        time.sleep(5)
        """
        create_ec2_instance()
        time.sleep(20)
        try:
            df = pd.read_json('/mnt/efs/fs1/raw/getaround/test_6.json', orient='records', lines=True)
            for i in range(0, len(df)):
                try:
                    car_id = df['cars'][i][0]['id']
                    end_date = "2022-11-01"
                    end_time = "07%3A00"
                    start_date = "2022-10-31"
                    start_time = "06%3A00"
                    ip = random.choice(ip_list)
                    data_chunck = [car_id, end_date, end_time, start_date, start_time, ip]

                    concc = Concurrency(worker_thread=50)
                    concc.run_thread(func=scraper.GetFareData, 
                    param_list=[data_chunck], param_name=["car_id", "end_date", "end_time", "start_date", "start_time", "ip"])
                except Exception as e:
                    print(e)
                    pass
            
            scraper.FareDataWriter('/mnt/efs/fs1/raw/getaround/fare_test_5.json')
        
        except Exception as e:
            print(e)
            pass
        
        ip_list.clear()
        DeleteInstance()
        time.sleep(25)
        """

    except KeyboardInterrupt:
        print('KeyboardInterrupt')
        DeleteInstance()
        

end = time.time()

with open('Data/time.txt', 'a+') as f:
    f.write(str(end - start))
    # add new line
    f.write("\n")

