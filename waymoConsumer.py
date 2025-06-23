from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'waymo-rides',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='waymo-tracker',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📡 Listening for Waymo ride updates...\n")

seen_rides = set()  # store unique ride IDs

for message in consumer:
    update = message.value
    ride_id = update['ride_id']
    
    # 🆕 Print ride summary when we see it for the first time
    if ride_id not in seen_rides:
        print(f"🚘 Simulating Waymo ride {ride_id[:8]} from {update['pickup']} to {update['destination']} (User: {update['user']})\n")
        seen_rides.add(ride_id)
    
    # 🖨️ Print the current update
    print(f"[{update['timestamp']}] Ride {ride_id[:8]} | Status: {update['status'].upper()} | ETA: {update['eta_minutes']} min | Proximity: {update['proximity_meters']}m")
    
    if update['status'] == 'arrived':
        print(f"✅ Passenger {update['user']} arrived at {update['destination']}.\n")
