import pymongo

if __name__ == "__main__":
    print("Welcome to pymongo")
    client = pymongo.MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
    print(client)

    db = client['Paritosh']
    collection = db['Paritosh_collection']
    dictionary = {'name': 'Harry', 'marks': 50}
    collection.insert_one(dictionary)
