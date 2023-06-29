from pymongo import MongoClient

client = MongoClient("mongodb+srv://.mongodb.net/", maxPoolSize=100)
db = client['dustDrive_prod']
print("MongoDB Connected")

import requests

repair_shop_col = db['repair_shop']
count = 0

datas = repair_shop_col.find()

url = 'http://api.vworld.kr/req/address?'
params = 'service=address&request=getcoord&version=2.0&crs=epsg:4326&refine=true&simple=false&format=json&type='
road_type = 'ROAD'
address = '&address='
keys = '&key='
primary_key = '619A8966-35B3-335C-90C5-B2AB88E4A5BD'


def request_geo(road):
    page = requests.get(url + params + road_type + address + road + keys + primary_key)
    json_data = page.json()
    print(json_data)
    return json_data


for data in datas:

    repair_shop_col.update_one({"_id": data['_id']}, {"$unset": {"location": ""}}, upsert=False)

    json_data = request_geo(data['address'])

    try:
        x = json_data['response']['result']['point']['x']
        y = json_data['response']['result']['point']['y']

        data['location'] = {
            'x': float(x), 'y': float(y)
        }

        repair_shop_col.update_one({"_id": data['_id']}, {"$set": data}, upsert=False)
        print('data pushed ::: ' + str(data['address']))
    except KeyError:
        print('KeyError ::: ' + str(data['address']))

print('finished')
