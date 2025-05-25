import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

TOPIC = "capteur-iot"
KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def simulate_sensor():
    return {
        "temperature": round(random.uniform(15.0, 30.0), 2),
        "humidity": round(random.uniform(40.0, 80.0), 2),
        "pressure": round(random.uniform(0.0, 10.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

while True:
    data = simulate_sensor()
    producer.send(TOPIC, data)
    print(f"[Capteur] Envoyé : {data}")
    time.sleep(60)  # toutes les 10 secondes
