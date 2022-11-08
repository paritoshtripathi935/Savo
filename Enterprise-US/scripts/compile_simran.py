from cgitb import handler
from datetime import datetime
from config import base_data_path, RIVAL_TYPE, COUNTRY_CODE 
import os
from lib.json_handler import JsonHandler #?

data_path =  os.path.join(base_data_path, "raw", RIVAL_TYPE) 
address_file_path = os.path.join(data_path, "addressMap", "address-10-25-2022-22-22-14.json")
raw_folder = os.path.join(data_path, "raw", "10-30-2022-20-30-03")
op_file = os.path.join(data_path, f"{RIVAL_TYPE.lower()}_compiler.json")

handler = JsonHandler()

address_map = handler.load_json(path=address_file_path, is_zipped=False)
search_path = os.path.join(raw_folder, "Search")

vechicles = set()

def get_fare(v_id, obj):
    vehicle_folder = os.path.join(raw_folder, "Fare", str(v_id)) 
    
    for latlng in os.listdir(vehicle_folder):
        loc_folder = os.path.join(vehicle_folder, latlng)
        address = address_map[latlng]
        result = []
        for se_time in os.listdir(loc_folder):
            time_folder = os.path.join(loc_folder, se_time)
            pickup_ts, dropoff_ts = se_time.split("--")
            file_name = os.listdir(time_folder)[0]
            temp_file_path = os.path.join(loc_folder, se_time, file_name) 
            fare_obj = handler.load_json(path=temp_file_path).get("response")
            epoch = file_name.split("--")[0]
            ts_obj = datetime.fromtimestamp(float(epoch))
            ts = ts_obj.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            # print("FAREEEEEEEEEEE", fare_obj)
            # print("SEARCHHHHHHHH", obj)
            # input()
            #     
            #     
            #     for vechile_file in os.listdir(latlng_folder):
            #         vechile_file_path = os.path.join(latlng_folder, vechile_file)
            #         vechile_data = handler.load_json(vechile_file_path)
            #         ts, start_date, end_date, _ = vechile_file.split("--")
            #         v_id = ""
            #         fare_folder = os.path.join(raw_folder, "Fare", v_id)
            #         fare_files = os.listdir(fare_folder)
            basic_cover = fare_obj.get("basic_cover", {}).get("table")
            #basic_cover = fare_obj.get("basic_cover", {}).get("table")
            lat, lng = latlng.split(",")
            data = {
                "source": RIVAL_TYPE,
                "country_code": COUNTRY_CODE,
                "source_url": "https://www.carnextdoor.com.au",
                "scraped_at": ts,
                "pickup_timestamp": pickup_ts,
                "dropoff_timestamp": dropoff_ts,
                "vehicle_id": str(v_id),
                "vehicle_name": obj.get("vehicle_name"),
                "body_type": obj.get("body_type"),
                "vehicle_make": obj.get("make"),
                "year": obj.get("year"),
                "model_name": obj.get("model"),
                "type": obj.get("body_type"),
                "variant": obj.get("name", ""),
                "vin": None,
                "vehile_url": f"https://www.carnextdoor.com.au/trips/new?vehicle_id={v_id}",
                "license_number": None,
                "description": None,
                "images": [
                    obj.get("name", "photo_url"),
                    obj.get("name", "img"),
                    ],
                "features": [],
                "vehicle_details":{
                        "average_fuel_economy":None,
                        "fuel_consumption":None,
                        "city_fuel_economy":None,
                        "fuel_type":None,
                        "fuel_grade":None,
                        "highway_fuel_economy":None,
                        "num_door": None,
                        "num_seat": obj.get("number_of_seats"),
                        "num_baggage":None
                    },
                "insurance_details": {
                        "plan":None,
                        "provider":None,
                        "insurance_license_number":None,
                        "protection_level":None,
                        "valid_till":None
                    },
                "rating": None,
                "age": None,
                "reviews": {
                    "positive_reviews": obj.get("positive_review_count"), 
                    "negative_reviews": obj.get("negative_review_count"), 
                    "total_reviews": obj.get("total_review_count")
                },
                "favourites_count": None,
                "trip_count": obj.get("total_trips"),
                "order_location":{
                    "latitude": lat, 
                    "longitude": lng, 
                    "name": address
                },
                "pickup_location":{
                    "latitude": obj.get("lat"),
                    "longitude": obj.get("lng"),
                    "address": None,
                    "area": None,
                    "city": None,
                    "region": None,
                    "state": None,
                    "suburb": obj.get("suburb"),
                    "street": obj.get("street"),
                    "name": None
                },
                "lender_note":None,
                "is_high_valued": False,
                "is_available": True if "available" in str(obj.get("availability_text")).lower() else False,
                "is_automatic": True if "automatic" in str(obj.get("transmission", "")).lower() else False,
                "is_frequently_booked": None,
                "owner_name": obj.get("name").split(" ")[0] if obj.get("name") else None,
                "owner_rating":None,
                "owner_images": None,
                "owner_profile_url": None,
                "currency": "AUD",
                "vehicle_price": [{
                    "rate" : "HOUR", 
                    "discount": None, 
                    "amount" : obj.get("costs", {}).get("hourly")
                },
                {
                    "rate" : "DAY", 
                    "discount": None, 
                    "amount" : obj.get("costs", {}).get("daily")
                }],
                "price_per_unit_distance": {
                    "unit":"km", 
                    "amount": obj.get("costs", {}).get("distance")
                },
                "total_price": fare_obj.get( "pre_discount_trip_cost"),
                "payable_price": fare_obj.get( "payment_breakdown").get("table").get("amount_to_charge"),
                "discounts": {
                    "total_discounts": None, 
                    "components" :[
                        {
                            "description": "Long Booking Discount", 
                            "amount" : obj.get("costs", {}).get("long_booking_discount")
                        },
                        {
                            "description": "Promotional Discount", 
                            "amount" : fare_obj.get( "promotion_discount")
                        }
                    ]
                },
                "price_breakup":[
                    {
                        "title": basic_cover.get("name"), 
                        "amount": basic_cover.get("value")
                    },
                     {
                        "title": "Time", 
                        "amount": fare_obj.get( "time")
                    },
                     {
                        "title": "Platform", 
                        "amount": fare_obj.get( "booking")
                    }
                ],
                "other_price_components":[
                    {
                        "title":fare_obj.get("dcl_text"),
                        "amount":fare_obj.get("dcl_cost")
                    },
                    {
                        "title":"Extension Fee",
                        "amount":fare_obj.get("applied_extension_fee")
                    }
                ]
                
            }
            result.append(data)
        handler.save_ndjson(result= result, op_file_path=op_file)

    # fare_path = os.path.join(raw_folder, "Fare") 
    
def main():
    for latlng in os.listdir(search_path): 
        loc_folder = os.path.join(search_path, latlng)
        address = address_map[latlng]
        for vehicle_file in os.listdir(loc_folder):
            vehicle_file_path = os.path.join(loc_folder, vehicle_file)
            vehicle_search_data = handler.load_json(path=vehicle_file_path)
            search_results = vehicle_search_data.get("response", {}).get("search_results", [])
            for result in search_results:
                v_id = result.get("id")
                if v_id not in vechicles and v_id is not None:
                    get_fare(v_id, result)
                vechicles.add(v_id)
        
            

main()