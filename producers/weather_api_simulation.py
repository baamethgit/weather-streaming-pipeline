import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "meteo-simulee"

def generate_fake_weather():
    return {
        "temperature": round(random.uniform(18.0, 35.0), 2),
        "humidity": random.randint(40, 90),
        "wind_speed": round(random.uniform(1.0, 10.0), 2),
        "wind_direction": random.choice(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]),
        "timestamp": time.time()
    }

while True:
    data = generate_fake_weather()
    producer.send(TOPIC, data)
    print(f"[Simulé] Données envoyées : {data}")
    time.sleep(10)  
