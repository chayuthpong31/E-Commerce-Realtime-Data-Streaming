from faker import Faker
import random
from dotenv import load_dotenv
import os
from kafka import KafkaProducer
import json
import time

load_dotenv()

# init faker
fake = Faker()

def get_product():

    # Define Products
    products = [
    {"product_name": "UltraView 4K Monitor", "product_category": "Electronics", "product_price": 349.99},
    {"product_name": "ErgoFlow Office Chair", "product_category": "Furniture", "product_price": 210.50},
    {"product_name": "BrewMaster Espresso Machine", "product_category": "Appliances", "product_price": 129.00},
    {"product_name": "TrailBlazer Running Shoes", "product_category": "Footwear", "product_price": 85.00},
    {"product_name": "Zenith Wireless Headphones", "product_category": "Electronics", "product_price": 199.99},
    {"product_name": "Organic Green Tea (50 bags)", "product_category": "Groceries", "product_price": 12.45},
    {"product_name": "PowerLift Dumbbell Set", "product_category": "Fitness", "product_price": 55.00},
    {"product_name": "GlowRadiance Night Cream", "product_category": "Skincare", "product_price": 42.00},
    {"product_name": "EcoWrite Recycled Notebook", "product_category": "Stationery", "product_price": 5.99},
    {"product_name": "Titanium Multi-Tool", "product_category": "Tools", "product_price": 29.50}
    ]

    seed = random.randrange(len(products))
    fake.seed_instance(seed)

    return {
        'product_id' : fake.uuid4(),
        **products[seed],
        'quantity' : random.randint(1, 10)
    }
    

def get_customer():
    # Define locations
    locations = [
    {"city": "New York", "country": "USA", "state": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"city": "Los Angeles", "country": "USA", "state": "California", "latitude": 34.0522, "longitude": -118.2437},
    {"city": "Chicago", "country": "USA", "state": "Illinois", "latitude": 41.8781, "longitude": -87.6298},
    {"city": "Houston", "country": "USA", "state": "Texas", "latitude": 29.7604, "longitude": -95.3698},
    {"city": "Miami", "country": "USA", "state": "Florida", "latitude": 25.7617, "longitude": -80.1918},
    {"city": "Tokyo", "country": "Japan", "state": "Tokyo", "latitude": 35.6895, "longitude": 139.6917},
    {"city": "London", "country": "United Kingdom", "state": "England", "latitude": 51.5074, "longitude": -0.1278},
    {"city": "Paris", "country": "France", "state": "Île-de-France", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Sydney", "country": "Australia", "state": "New South Wales", "latitude": -33.8688, "longitude": 151.2093},
    {"city": "Bangkok", "country": "Thailand", "state": "Bangkok", "latitude": 13.7563, "longitude": 100.5018}
    ]

    seed = random.randrange(len(locations))
    fake.seed_instance(seed)

    return {
        "customer_id" : fake.uuid4(),
        "customer_name" : fake.name(),
       **(locations[seed])
    }

def generate_data():
    return {
        "order_id" : fake.uuid4() ,
        **get_customer(), # Dictionary Unpack
        **get_product(),
        "delivery_status" : random.choice(['Processing', 'Shipped', 'Delivered', 'Cancelled']),
        "order_timestamp" : fake.date_time_between(start_date='-1d', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
    }

# Configuration Constants
# Load Environment Variables
NAME_SPACE = os.getenv('NAME_SPACE')
# Connection String from Azure Portal
CONNECTION_STR = os.getenv('CONNECTION_STR')
# Format: <namespace>.servicebus.windows.net:9093
BOOTSTRAP_SERVERS = f'{NAME_SPACE}.servicebus.windows.net:9093'
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString', # Must be literally "$ConnectionString"
    sasl_plain_password=CONNECTION_STR,      # The actual connection string
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # Convert dict to JSON bytes
)

def stream_data():
    print("--------------------------- Start Streaming Data -------------------------")
    try:
        while True:
            data = generate_data()

            # Sent data asynchronously
            producer.send(EVENT_HUB_NAME, value=data)
            print(f"Sent: {data['order_id']} | {data['customer_name']} | {data['product_name']}")

            # Control the flow rate
            time.sleep(3)

    except Exception as e:
        producer.close()

    finally:
        producer.flush() 
        producer.close()
        print("Producer Closed.")

if __name__ == "__main__":
    stream_data()
    
