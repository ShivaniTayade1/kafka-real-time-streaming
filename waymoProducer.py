import time
import json
import random
import uuid
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ride_id = str(uuid.uuid4())
user = random.choice(['Amber', 'Sakshi', 'Karishma', 'Shivani'])
pickup = random.choice(['Market St', 'Lombard St', 'Nob Hill', 'Union Square'])
destination = random.choice(['SFO Airport', 'Golden Gate Park', 'UC Law', 'Buena Vista Cafe'])

# Initial ride request
stages = [
    ('booked', 10),
    ('en_route_to_pickup', 8),
    ('arriving', 3),
    ('passenger_onboard', 0),
    ('in_transit', 5),
    ('approaching_destination', 1),
    ('arrived', 0)
]

print(f"Simulating Waymo ride {ride_id} from {pickup} to {destination}\n")

for status, eta in stages:
    ride_update = {
        'ride_id': ride_id,
        'user': user,
        'pickup': pickup,
        'destination': destination,
        'status': status,
        'eta_minutes': eta,
        'proximity_meters': random.randint(20, 500) if status != 'arrived' else 0,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }
    producer.send('waymo-rides', ride_update)
    print(f"🚗 Sent update: {ride_update}")
    time.sleep(random.uniform(1.5, 3.0))  # simulate real time
