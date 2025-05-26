import requests
import json
from kafka import KafkaProducer
import time
import schedule
import datetime


API_KEY = "baad5febfdfcbc8b2f1283e57a6f49d2"
API_KEY_TST = "baad5febfdfcbc8b2f1283e57a6f49d2"
LAT, LON = 14.7908, -16.9382  # Thies
KAFKA_TOPIC = "weather-api"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC_ALERTS = "weather-alerts"


producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def fetch_and_send_weather():
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={LAT}&lon={LON}&exclude=minutely,hourly,daily,alerts&appid={API_KEY_TST}&units=metric"
    try:
        response = requests.get(url)
        data = response.json()

        current = data.get("current", {})
        payload = {
            "timestamp": current.get("dt"),
            "datetime": datetime.datetime.fromtimestamp(current.get("dt")).isoformat(),
            "temperature": current.get("temp"),
            "humidity": current.get("humidity"),
            "wind_speed": current.get("wind_speed"),
            "wind_deg": current.get("wind_deg"),
            "pressure": current.get("pressure"),
            "feels_like": current.get("feels_like"),
            "uvi": current.get("uvi"), #Indice UV
            "weather_main": current.get("weather.main"), # Description textuelle des conditions
            "weather_description": current.get("weather.description"), 
        }

        producer.send(KAFKA_TOPIC, value=payload)
        print(100*"#")
        print(100*"#")
        print(f"[{payload['datetime']}] Sent to Kafka: {payload}")
        print(100*"#")
        for alert in data.get("alerts", []):
            alert_payload = {
                "datetime": datetime.datetime.fromtimestamp().isoformat(),
                "event": alert.get("event"),
                "start": alert.get("start"),
                "end": alert.get("end"),
                "sender": alert.get("sender_name"),
                "description": alert.get("description"),
                "tags": alert.get("tags", []),
                "location": "LOCATION_TAG"  # genre "paris"
            }
            print(100*"#")
            producer.send(KAFKA_TOPIC_ALERTS, value=alert_payload)
            print(f"=============[ALERT] Sent to Kafka: {alert_payload}==========")



    except Exception as e:
        print(f"Error fetching/sending data: {e}")

schedule.every(10).minutes.do(fetch_and_send_weather)

print("Weather ingestion started")
fetch_and_send_weather()  # premier appel immédiat

while True:
    schedule.run_pending()
    time.sleep(1)