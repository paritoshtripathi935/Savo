from re import L
import requests
from bs4 import BeautifulSoup
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests import get
import pandas as pd
import random
import time
from concurrency import Concurrency
import os
import gzip
import datetime
from datetime import timedelta
import sys

start = time.time()

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
        self.headers = json.loads(open('Data/headers/headers.json').read())
        self.token_headers = json.loads(open('Data/headers/token_headers.json').read())
        self.service_headers = json.loads(open('Data/headers/service_headers.json').read())
        self.service_api = "https://www.budget.com/webapi/station/proximitySearch"
        self.car_api = "https://www.budget.com/webapi/reservation/vehicles"
        self.token_api = "https://www.budget.com/webapi/init"
        self.car_data = []
        #self.Full_data = []
        self.token = ""
        self.storage_path = "/mnt/efs/fs1/raw/budget/us/"

    def get_token(self):
        '''
        proxies = {
            'http': f'http://{ip}:3128',
            'https': f'http://{ip}:3128'
        }
        '''
        try:
            response = request.get(self.token_api, headers=self.token_headers)
            if response.status_code == 200:
                response_headers = response.headers
                self.token = response_headers['DIGITAL_TOKEN']
            
                print("TOKEN: GENERATED SUCCESSFULLY")
                return response.status_code
            else:
                print("TOKEN: FAILED TO GENERATE")
                return response.status_code

        except Exception as e:
            print("TOKEN: ERROR  " + str(e))
            print("TOKEN: ERROR" + str(sys.exc_info()[-1].tb_lineno))
    
    def GetServiceData(self, longitude, latitude):
        '''
        proxies = {
            'http': f'http://{ip}:3128',
            'https': f'http://{ip}:3128'
        }
        '''

        body = {
                "type": "proximity",
                "geoCoordinate": {
                    "longitude": longitude,
                    "latitude": latitude
                    },
                "rqHeader": {
                    "locale": "en_US",
                    "domain": "us"
                    }
        }

        try:
            # add token to headers
            self.service_headers['DIGITAL_TOKEN'] = self.token
            response = request.post(self.service_api, headers=self.service_headers, json=body)

            if response.status_code == 200:
                print("Successful Response of Service API on", latitude, longitude)
                #self.service_data.append(response.json())
        
                return response.status_code
            else:
                print("Unsuccesful Response of Service API on", latitude, longitude)
                return response.status_code
        
        except Exception as e:
            print("SERVICE: ERROR" + str(e))
            print("SERVICE: ERROR" + str(sys.exc_info()[-1].tb_lineno))

        

    def GetCarData(self, start_date, end_date, start_time, end_time, longitude, latitude):
        '''
        proxies = {
            'http': f'http://{ip}:3128',
            'https': f'http://{ip}:3128'
        }
        '''
        
        body = {
            "rqHeader":{"brand":"","locale":"en_US"},
            "nonUSShop":False,
            "pickInfo":"N5B",
            "pickCountry":"US",
            "pickDate":start_date,
            "pickTime":start_time,"dropInfo":"N5B","dropDate":end_date,"dropTime":end_time,
            "couponNumber":"","couponInstances":"","couponRateCode":"","discountNumber":"","rateType":"",
            "residency":"US","age":26,"wizardNumber":"","lastName":"","userSelectedCurrency":"USD","selDiscountNum":"",
            "promotionalCoupon":"","preferredCarClass":"","membershipId":"","noMembershipAvailable":False,"corporateBookingType":"",
            "enableStrikethrough":"true","picLocTruckIndicator":False,"amazonGCPayLaterPercentageVal":"","amazonGCPayNowPercentageVal":"","corporateEmailID":""
        }
        
        try:

            # start timestamp 2022-11-02T21:02:09.00 format using datetime
            start_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.00")
            self.get_token()
            self.headers['digital_token'] = self.token
            time.sleep(2)

            response = request.post(self.car_api, headers=self.headers, json=body)
            """
            if self.GetServiceData(longitude, latitude) == 200:
                time.sleep(1)
            """
            if response.status_code == 200:
                print("Successful Response of Car API on", latitude, longitude)
                self.car_data.append(response.json())
                #self.Full_data.append(response.json())

                end_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.00")
                self.SaveData(latitude, longitude, start_timestamp, end_timestamp, start_time, end_time)
                return response.status_code
            else:
                print("Unsuccesful Response of Car API on", latitude, longitude)
                return response.status_code
            
        except Exception as e:
            print("CAR: ERROR" + str(e))
            print("CAR: ERROR" + str(sys.exc_info()[-1].tb_lineno))

    def SaveData(self, latitude, longitude, start_date_new, end_date_new, start_time, end_time):
        try:
            if not os.path.exists(self.storage_path + "raw/"):
                os.makedirs(self.storage_path + "raw/")
            
            if not os.path.exists(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/"):
                os.makedirs(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/")

            filename_new = "{}--{}.json.gz".format(start_date_new, end_date_new)
            print(filename_new)
            with gzip.open(self.storage_path + "raw/" + Datetime + "/" + "Location/" + str(latitude) + "," + str(longitude) + "/"+ filename_new, 'wt') as f:
                json.dump(self.car_data, f)
                self.car_data.clear()
                
                print("CarDataWriter: is written in raw folder", latitude, longitude)

        except Exception as e:
            print('Data Write Error of CarDataWriter: ', e)


    def FullDataWriter(self, filename):
        try:
            df = pd.DataFrame(self.Full_data)
            df.to_json(filename, orient='records', lines=True)
        
        except Exception as e:
            print('FullDataWriter: ', e)
    
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


if __name__ == '__main__':
    try:
        scraper = Scraper()
        
        try:
            data_chunck = []
            with open('/home/ubuntu/Budget-US/Data/geojson/usa_geojson.json') as f:
                data = json.load(f)
                start_new = scraper.date_generator()
                for start_date, end_date in start_new:                
                    for i in range(0, len(data['features'])):
                        latitude, longitude = data['features'][i]['geometry']['coordinates'][0][0]
                        start_date_new = start_date.strftime("%m/%d/%Y")
                        end_date_new = end_date.strftime("%m/%d/%Y")
                        start_time = "12:00 AM"
                        end_time = "12:00 AM"

                        #scraper.GetCarData(start_date_new, end_date_new, start_time, end_time, longitude, latitude)

                        
                        data_chunck.append([start_date_new, end_date_new, start_time, end_time, longitude, latitude])
                            
                        if len(data_chunck) == 100:
                            concc = Concurrency(worker_thread=10, worker_process=1)
                            concc.run_thread(func=scraper.GetCarData, 
                            param_list=data_chunck, param_name=['start_date', 'end_date', 'start_time', 'end_time', 'longitude', 'latitude'])
                            data_chunck = []
                            time.sleep(5)
                            print("Data Chunk is done")
                        
                        
            #scraper.FullDataWriter("{}FullData.json".format(scraper.storage_path))

        except Exception as e:
            print('Main: ', e)
            print("Main: " + str(sys.exc_info()[-1].tb_lineno))
            pass

    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        scraper.SaveData("Failed", "Run")
        scraper.FullDataWriter("{}FullData.json".format(scraper.storage_path))
        print("Data Saved")
        sys.exit(0)
  
    
end = time.time()

with open('Data/time.txt', 'a+') as f:
    Datetime = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    f.write(str(end - start))
    f.write(Datetime)
    # add new line
    f.write("\n")

