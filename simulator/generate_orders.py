import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Azure Event Hub credentials
BOOTSTRAP_SERVERS = 'here' #Event Hub Namespace - Overview - Host name : ""
EVENT_HUB_CONNECTION_STRING = 'here' #Event Hub Namespace - Settings - Shared access ploicies - RootManageSharedAccessKey - Primary connection string:
EVENT_HUB_NAME = 'here' #Event Hub Namespace - Overview - Event Hubs - Name :

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password=EVENT_HUB_CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categories = ['Electronics', 'Books', 'Clothing', 'Home Decor', 'Toys']
locations = [
    {"city": "Montgomery", "state": "AL", "lat": 32.3777, "lon": -86.3006},
    {"city": "Juneau", "state": "AK", "lat": 58.3016, "lon": -134.4197},
    {"city": "Phoenix", "state": "AZ", "lat": 33.4484, "lon": -112.0740},
    {"city": "Little Rock", "state": "AR", "lat": 34.7465, "lon": -92.2896},
    {"city": "Sacramento", "state": "CA", "lat": 38.5816, "lon": -121.4944},
    {"city": "Denver", "state": "CO", "lat": 39.7392, "lon": -104.9903},
    {"city": "Hartford", "state": "CT", "lat": 41.7658, "lon": -72.6734},
    {"city": "Dover", "state": "DE", "lat": 39.1582, "lon": -75.5244},
    {"city": "Tallahassee", "state": "FL", "lat": 30.4383, "lon": -84.2807},
    {"city": "Atlanta", "state": "GA", "lat": 33.7490, "lon": -84.3880},
    {"city": "Honolulu", "state": "HI", "lat": 21.3069, "lon": -157.8583},
    {"city": "Boise", "state": "ID", "lat": 43.6150, "lon": -116.2023},
    {"city": "Springfield", "state": "IL", "lat": 39.7980, "lon": -89.6440},
    {"city": "Indianapolis", "state": "IN", "lat": 39.7684, "lon": -86.1581},
    {"city": "Des Moines", "state": "IA", "lat": 41.5868, "lon": -93.6250},
    {"city": "Topeka", "state": "KS", "lat": 39.0473, "lon": -95.6752},
    {"city": "Frankfort", "state": "KY", "lat": 38.2009, "lon": -84.8733},
    {"city": "Baton Rouge", "state": "LA", "lat": 30.4515, "lon": -91.1871},
    {"city": "Augusta", "state": "ME", "lat": 44.3106, "lon": -69.7795},
    {"city": "Annapolis", "state": "MD", "lat": 38.9784, "lon": -76.4922},
    {"city": "Boston", "state": "MA", "lat": 42.3601, "lon": -71.0589},
    {"city": "Lansing", "state": "MI", "lat": 42.7325, "lon": -84.5555},
    {"city": "Saint Paul", "state": "MN", "lat": 44.9537, "lon": -93.0900},
    {"city": "Jackson", "state": "MS", "lat": 32.2988, "lon": -90.1848},
    {"city": "Jefferson City", "state": "MO", "lat": 38.5767, "lon": -92.1735},
    {"city": "Helena", "state": "MT", "lat": 46.5891, "lon": -112.0391},
    {"city": "Lincoln", "state": "NE", "lat": 40.8136, "lon": -96.7026},
    {"city": "Carson City", "state": "NV", "lat": 39.1638, "lon": -119.7674},
    {"city": "Concord", "state": "NH", "lat": 43.2081, "lon": -71.5376},
    {"city": "Trenton", "state": "NJ", "lat": 40.2206, "lon": -74.7699},
    {"city": "Santa Fe", "state": "NM", "lat": 35.6870, "lon": -105.9378},
    {"city": "Albany", "state": "NY", "lat": 42.6526, "lon": -73.7562},
    {"city": "Raleigh", "state": "NC", "lat": 35.7796, "lon": -78.6382},
    {"city": "Bismarck", "state": "ND", "lat": 46.8083, "lon": -100.7837},
    {"city": "Columbus", "state": "OH", "lat": 39.9612, "lon": -82.9988},
    {"city": "Oklahoma City", "state": "OK", "lat": 35.4676, "lon": -97.5164},
    {"city": "Salem", "state": "OR", "lat": 44.9429, "lon": -123.0351},
    {"city": "Harrisburg", "state": "PA", "lat": 40.2732, "lon": -76.8867},
    {"city": "Providence", "state": "RI", "lat": 41.8240, "lon": -71.4128},
    {"city": "Columbia", "state": "SC", "lat": 34.0007, "lon": -81.0348},
    {"city": "Pierre", "state": "SD", "lat": 44.3683, "lon": -100.3510},
    {"city": "Nashville", "state": "TN", "lat": 36.1627, "lon": -86.7816},
    {"city": "Austin", "state": "TX", "lat": 30.2672, "lon": -97.7431},
    {"city": "Salt Lake City", "state": "UT", "lat": 40.7608, "lon": -111.8910},
    {"city": "Montpelier", "state": "VT", "lat": 44.2601, "lon": -72.5754},
    {"city": "Richmond", "state": "VA", "lat": 37.5407, "lon": -77.4360},
    {"city": "Olympia", "state": "WA", "lat": 47.0379, "lon": -122.9007},
    {"city": "Charleston", "state": "WV", "lat": 38.3498, "lon": -81.6326},
    {"city": "Madison", "state": "WI", "lat": 43.0731, "lon": -89.4012},
    {"city": "Cheyenne", "state": "WY", "lat": 41.1400, "lon": -104.8202}
]

def generate_order():
    location = random.choice(locations)
    category = random.choice(categories)
    price = round(random.uniform(10, 2000), 2)
    quantity = random.randint(1, 5)
    
    return {
        "order_id": fake.uuid4(),
        "timestamp": datetime.utcnow().isoformat(),
        "customer_id": fake.uuid4(),
        "product_id": fake.uuid4(),
        "category": category,
        "price": price,
        "quantity": quantity,
        "total_amount": round(price * quantity, 2),
        "city": location["city"],
        "state": location["state"],
        "country": "USA",
        "latitude": location["lat"],
        "longitude": location["lon"],
        "delivery_status": random.choice(["Processing", "Shipped", "Delivered", "Cancelled"])
    }

if __name__ == "__main__":
    print("Streaming fake U.S. e-commerce orders to Azure Event Hub...")
    while True:
        event = generate_order()
        producer.send(EVENT_HUB_NAME, value=event)
        print("Sent:", event)
        time.sleep(1) 