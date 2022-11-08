import pandas as pd
import json
import gzip
import os
import sys
import time

data = {
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "source": {
      "type": "string",
      "description": "Rival name (It should be UpperCamelCase with no space)"
    },
    "source_url": {
      "type": "string",
      "description": "Rival website url with HTTP/HTTPS"
    },
    "scraped_at": {
      "type": "string",
      "format": "date-time",
      "description": "Date and time of scraping in UTC timezone"
    },
    "country_code": {
      "type": "string",
      "enum": [
        "AD",
        "AE",
        "AF",
        "AG",
        "AI",
        "AL",
        "AM",
        "AO",
        "AQ",
        "AR",
        "AS",
        "AT",
        "AU",
        "AW",
        "AX",
        "AZ",
        "BA",
        "BB",
        "BD",
        "BE",
        "BF",
        "BG",
        "BH",
        "BI",
        "BJ",
        "BL",
        "BM",
        "BN",
        "BO",
        "BQ",
        "BR",
        "BS",
        "BT",
        "BV",
        "BW",
        "BY",
        "BZ",
        "CA",
        "CC",
        "CD",
        "CF",
        "CG",
        "CH",
        "CI",
        "CK",
        "CL",
        "CM",
        "CN",
        "CO",
        "CR",
        "CU",
        "CV",
        "CW",
        "CX",
        "CY",
        "CZ",
        "DE",
        "DJ",
        "DK",
        "DM",
        "DO",
        "DZ",
        "EC",
        "EE",
        "EG",
        "EH",
        "ER",
        "ES",
        "ET",
        "FI",
        "FJ",
        "FK",
        "FM",
        "FO",
        "FR",
        "GA",
        "GB",
        "GD",
        "GE",
        "GF",
        "GG",
        "GH",
        "GI",
        "GL",
        "GM",
        "GN",
        "GP",
        "GQ",
        "GR",
        "GS",
        "GT",
        "GU",
        "GW",
        "GY",
        "HK",
        "HM",
        "HN",
        "HR",
        "HT",
        "HU",
        "ID",
        "IE",
        "IL",
        "IM",
        "IN",
        "IO",
        "IQ",
        "IR",
        "IS",
        "IT",
        "JE",
        "JM",
        "JO",
        "JP",
        "KE",
        "KG",
        "KH",
        "KI",
        "KM",
        "KN",
        "KP",
        "KR",
        "KW",
        "KY",
        "KZ",
        "LA",
        "LB",
        "LC",
        "LI",
        "LK",
        "LR",
        "LS",
        "LT",
        "LU",
        "LV",
        "LY",
        "MA",
        "MC",
        "MD",
        "ME",
        "MF",
        "MG",
        "MH",
        "MK",
        "ML",
        "MM",
        "MN",
        "MO",
        "MP",
        "MQ",
        "MR",
        "MS",
        "MT",
        "MU",
        "MV",
        "MW",
        "MX",
        "MY",
        "MZ",
        "NA",
        "NC",
        "NE",
        "NF",
        "NG",
        "NI",
        "NL",
        "NO",
        "NP",
        "NR",
        "NU",
        "NZ",
        "OM",
        "PA",
        "PE",
        "PF",
        "PG",
        "PH",
        "PK",
        "PL",
        "PM",
        "PN",
        "PR",
        "PS",
        "PT",
        "PW",
        "PY",
        "QA",
        "RE",
        "RO",
        "RS",
        "RU",
        "RW",
        "SA",
        "SB",
        "SC",
        "SD",
        "SE",
        "SG",
        "SH",
        "SI",
        "SJ",
        "SK",
        "SL",
        "SM",
        "SN",
        "SO",
        "SR",
        "SS",
        "ST",
        "SV",
        "SX",
        "SY",
        "SZ",
        "TC",
        "TD",
        "TF",
        "TG",
        "TH",
        "TJ",
        "TK",
        "TL",
        "TM",
        "TN",
        "TO",
        "TR",
        "TT",
        "TV",
        "TW",
        "TZ",
        "UA",
        "UG",
        "UM",
        "US",
        "UY",
        "UZ",
        "VA",
        "VC",
        "VE",
        "VG",
        "VI",
        "VN",
        "VU",
        "WF",
        "WS",
        "YE",
        "YT",
        "ZA",
        "ZM",
        "ZW"
      ],
      "description": "The country code of location (ISO3166 Alpha-2 country code)"
    },
    "pickup_timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "Pickup date and time of vehicle in UTC timezone"
    },
    "dropoff_timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "Dropoff date and time of vehicle in UTC timezone"
    },
    "vehicle_id": {
      "type": "string",
      "description": "Unique vehicle id of source"
    },
    "vehicle_name": {
      "type": ["string", "null"],
      "description": "User defined name of vehicle"
    },
    "body_type": {
      "type": ["string", "null"],
      "description": "Body type of a vehicle (SUV/4x4)"
    },
    "vehicle_make": {
      "type": ["string", "null"],
      "description": "Manufacturing company of vehicle"
    },
    "year": {
      "type": "string",
      "description": "Year of manufacturing"
    },
    "model_name": {
      "type": "string",
      "description": "Model name of vehicle"
    },
    "color": {
      "type": ["string", "null"],
      "description": "Color of vehicle"
    },
    "type": {
      "type": ["string", "null"],
      "description": "Type of vehicle (CAR,etc)"
    },
    "variant": {
      "type": ["string", "null"],
      "description": "Exact model number of vehicle"
    },
    "vin": {
      "type": ["string", "null"],
      "description": "Vehicle Identification Number"
    },
    "vehile_url": {
      "type": ["string", "null"],
      "description": "Vehicle profile url"
    },
    "license_number": {
      "type": ["string", "null"],
      "description": "License plate number of vehicle"
    },
    "description": {
      "type": ["string", "null"],
      "description": "A brief description of vehicle"
    },
    "images": {
      "type": ["array", "null"],
      "description": "Images of vehicle",
      "items": {
        "type": "string"
      }
    },
    "features": {
      "type": ["array", "null"],
      "description": "List of vehicle features",
      "items": {
        "type": ["string", "null"]
      }
    },
    "vehicle_details": {
      "type": "object",
      "description": "Basic details of vehicle",
      "properties": {
        "average_fuel_economy": {
          "type": ["string", "null"],
          "description": "A combined average of highway and city MPG"
        },
        "fuel_consumption": {
          "type": ["string", "null"],
          "description": "The amount of fuel consumed in driving a given distance"
        },
        "city_fuel_economy": {
          "type": ["string", "null"],
          "description": "The score a car will get on average in city conditions"
        },
        "fuel_type": {
          "type": ["string", "null"],
          "description": "Type of fuel"
        },
        "fuel_grade": {
          "type": ["string", "null"],
          "description": "Grade of fuel"
        },
        "highway_fuel_economy": {
          "type": ["string", "null"],
          "description": "The average a car will get while driving on an open stretch"
        },
        "num_door": {
          "type": ["string", "null"],
          "description": "Door count of the car"
        },
        "num_seat": {
          "type": ["string", "null"],
          "description": "Seat/Passengers count in the car"
        },
        "num_baggage": {
          "type": ["string", "null"],
          "description": "Baggage count that we can store in the car"
        }
      },
      "required": [
        "average_fuel_economy",
        "fuel_consumption",
        "city_fuel_economy",
        "fuel_type",
        "fuel_grade",
        "highway_fuel_economy",
        "num_door",
        "num_seat",
        "num_baggage"
      ]
    },
    "insurance_details": {
      "description": "Insurance details of vehicle",
      "type": ["object", "null"],
      "properties": {
        "plan": {
          "type": ["string", "null"],
          "description": "Insurance plan details"
        },
        "provider": {
          "type": ["string", "null"],
          "description": "Insurance plan provider details"
        },
        "insurance_license_number": {
          "type": ["string", "null"],
          "description": "Insurance license number"
        },
        "protection_level": {
          "type": ["string", "null"],
          "description": "Insurance protection level"
        },
        "valid_till": {
          "type": ["string", "null"],
          "format": "date-time",
          "description": "Date insurance is valid upto in UTC timezone"
        }
      },
      "required": [
        "plan",
        "provider",
        "insurance_license_number",
        "protection_level",
        "valid_till"
      ]
    },
    "rating": {
      "type": ["string", "null"],
      "description": "Vehicle ratings"
    },
    "age": {
      "type": ["string", "null"],
      "description": "Age of customer"
    },
    "reviews": {
      "type": "object",
      "description": "Reviews for vehicle",
      "properties": {
        "positive_reviews": {
          "type": ["string", "null"],
          "description": "Positive reviews for vehicle"
        },
        "negative_reviews": {
          "type": ["string", "null"],
          "description": "Negative reviews for vehicle"
        },
        "total_reviews": {
          "type": ["string", "null"],
          "description": "Total reviews for vehicle"
        },
      },
      "required": ["positive_reviews", "negative_reviews", "total_reviews"]
    },
    "favourites_count": {
      "type": ["string", "null"],
      "description": "Vehicle favourite count"
    },
    "trip_count": {
      "type": ["string", "null"],
      "description": "Trip count"
    },
    "order_location": {
      "type": "object",
      "description": "Request location",
      "properties": {
        "latitude": {
          "type": ["string", "null"],
          "description": "Request location latitude"
        },
        "longitude": {
          "type": ["string", "null"],
          "description": "Request location longitude"
        },
        "name": {
          "type": ["string", "null"],
          "description": "Request location name"
        }
      },
      "required": ["latitude", "longitude", "name"]
    },
    "pickup_location": {
      "type": "object",
      "description": "Vehicle pickup location",
      "properties": {
        "latitude": {
          "type": ["string", "null"],
          "description": "Vehicle pickup location latitude"
        },
        "longitude": {
          "type": ["string", "null"],
          "description": "Vehicle pickup location longitude"
        },
        "name": {
          "type": ["string", "null"],
          "description": "Vehicle pickup location name"
        },
        "address": {
          "type": ["string", "null"],
          "description": "Vehicle pickup address"
        },
        "area": {
          "type": ["string", "null"],
          "description": "Vehicle pickup area"
        },
        "city": {
          "type": ["string", "null"],
          "description": "Vehicle pickup city"
        },
        "region": {
          "type": ["string", "null"],
          "description": "Vehicle pickup region"
        },
        "state": {
          "type": ["string", "null"],
          "description": "Vehicle pickup state"
        },
        "street": {
          "type": ["string", "null"],
          "description": "Vehicle pickup street"
        },
        "suburb": {
          "type": ["string", "null"],
          "description": "Vehicle pickup suburb"
        }
      },
      "required": [
        "latitude",
        "longitude",
        "address",
        "area",
        "city",
        "region",
        "state",
        "suburb",
        "street",
        "name"
      ]
    },
    "lender_note": {
      "type": ["string", "null"],
      "description": "Any note for lenders"
    },
    "is_high_valued": {
      "type": ["boolean", "null"],
      "description": "Is vehicle highly valued"
    },
    "is_available": {
      "type": ["boolean", "null"],
      "description": "Is vehicle available"
    },
    "is_automatic": {
      "type": ["boolean", "null"],
      "description": "Is vehicle trasnmission type is automatic"
    },
    "is_frequently_booked": {
      "type": ["boolean", "null"],
      "description": "Is vehicle frequently booked (or in top)"
    },
    "owner_name": {
      "type": ["string", "null"],
      "description": "Owner's name"
    },
    "owner_rating": {
      "type": ["string", "null"],
      "description": "Owner's rating"
    },
    "owner_images": {
      "type": ["array", "null"],
      "description": "Images of owner",
      "items": {
        "type": "string"
      }
    },
    "owner_profile_url": {
      "type": ["string", "null"],
      "description": "Owner's profile url"
    },
    "currency": {
      "type": "string",
      "description": "Currency symbol"
    },
    "vehicle_price": {
      "type": ["array", "null"],
      "description": "Amount and discount offered",
      "items": [
        {
          "type": ["object", "null"],
          "properties": {
            "rate": {
              "type": ["string", "null"],
              "enum": ["DAY", "HOUR", "WEEK"],
              "description": "Rate at which price will be charged on vehicle"
            },
            "amount": {
              "type": ["string", "null"],
              "description": "Amount that will be charged on vehicle"
            },
            "discount": {
              "type": ["string", "null"],
              "description": "Amount of Discount on vehicle"
            }
          },
          "required": ["rate", "discount", "amount"]
        }
      ]
    },
    "price_per_unit_distance": {
      "type": ["object", "null"],
      "properties": {
        "unit": {
          "type": ["string", "null"],
          "description": "Unit of distance travelled"
        },
        "amount": {
          "type": ["string", "null"],
          "description": "Price of per distance unit travelled"
        }
      },
      "required": ["unit", "amount"]
    },
    "total_price": {
      "type": ["string", "null"],
      "description": "Price of trip before discount"
    },
    "payable_price": {
      "type": ["string", "null"],
      "description": "Price of trip after discount"
    },
    "discounts": {
      "type": ["object", "null"],
      "description": "All information about discounts",
      "properties": {
        "total_discount": {
          "type": "string",
          "description": "Net applied discount"
        },
        "components": {
          "type": ["array", "null"],
          "description": "Available discount components",
          "items": [
            {
              "type": "object",
              "properties": {
                "description": {
                  "type": ["string", "null"],
                  "description": "Description of discount available"
                },
                "amount": {
                  "type": ["string", "null"],
                  "description": "Amount of discount available"
                }
              },
              "required": ["description", "amount"]
            }
          ]
        }
      },
      "required": ["total_discounts", "components"]
    },
    "price_breakup": {
      "type": ["array", "null"],
      "description": "Components of price",
      "items": [
        {
          "type": ["object", "null"],
          "properties": {
            "title": {
              "type": ["string", "null"],
              "description": "component title"
            },
            "amount": {
              "type": ["string", "null"],
              "description": "component amount"
            }
          },
          "required": ["title", "amount"]
        }
      ]
    },
    "other_price_components": {
      "type": ["array", "null"],
      "description": "Components to add in a trip",
      "items": [
        {
          "type": ["object", "null"],
          "properties": {
            "title": {
              "type": ["string", "null"],
              "description": "component title"
            },
            "amount": {
              "type": ["string", "null"],
              "description": "component amount"
            }
          },
          "required": ["title", "amount"]
        }
      ]
    },
    "labels": {
      "type": ["array", "null"],
      "description": "Tags on vehicle",
      "items": [
        {
          "type": ["object", "null"],
          "properties": {
            "description": {
              "type": ["string", "null"],
              "description": "Tag description"
            },
            "value": {
              "type": ["string", "null", "bool"],
              "description": "Tag value"
            }
          },
          "required": ["description", "value"]
        }
      ]
    },
    "unmapped_fields": {
      "type": ["string", "null"],
      "description": "unmapped fields"
    }
  },
  "required": [
    "source",
    "source_url",
    "scraped_at",
    "country_code",
    "vehicle_id",
    "pickup_timestamp",
    "dropoff_timestamp",
    "body_type",
    "vehicle_name",
    "vehicle_make",
    "year",
    "model_name",
    "color",
    "type",
    "variant",
    "vin",
    "vehile_url",
    "license_number",
    "description",
    "age",
    "images",
    "features",
    "vehicle_details",
    "insurance_details",
    "rating",
    "reviews",
    "favourites_count",
    "trip_count",
    "order_location",
    "pickup_location",
    "lender_note",
    "is_high_valued",
    "is_frequently_booked",
    "is_automatic",
    "owner_name",
    "owner_profile_url",
    "owner_rating",
    "owner_images",
    "currency",
    "price_per_unit_distance",
    "vehicle_price",
    "total_price",
    "payable_price",
    "discounts",
    "price_breakup",
    "other_price_components",
    "is_available",
    "labels"
  ]
}


# make a class compile to compile the schema
# /mnt/efs/fs1/raw/enterprise/usa/raw/2022-11-03-13-05-32/Location/{long},{lat}/2022-11-03-13-05-32.json.gz
class Compile:
    def __init__(self, schema):
        self.schema = schema
        self.base_path = "/mnt/efs/fs1/raw/enterprise/usa/raw/2022-11-03-13-05-32/Location/{},{}/2022-11-03-13-05-32.json.gz"
        self.geojson_path = '/home/ubuntu/Paritosh-Scrapers/Enterprise-US/Data/geojson/geojson.json'

    def joinBasePath(self, base_path, lat, long):
        return base_path.format(long, lat)

    def openFiles(self, path):
        try:
            with gzip.open(path, 'rt') as f:
                data_new = json.load(f)
                return data_new

        except Exception as e:
            print("This Lat Long is not avalable")
            pass
    
    def getData(self):
        with open(self.geojson_path) as f:
            data = json.load(f)

            for k in range(0, 1):
                latitude,longitude = data['features'][k]['geometry']['coordinates'][0][0]
                path = self.joinBasePath(self.base_path, latitude, longitude)
                carData = self.openFiles(path)

                df = pd.DataFrame(carData)
                print(df.head())
                print(df['availablecars'][0])

if __name__ == "__main__":
    compile = Compile(data)
    compile.getData()

#df = pd.DataFrame(data["properties"])
#print(df.columns)