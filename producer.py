from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "sensor_id": random.randint(1, 5),
        "pm25": round(random.uniform(5, 60), 2),
        "no2": round(random.uniform(10, 80), 2),
        "timestamp": time.time()
    }

    producer.send("iot-sensors", data)
    print("Sent:", data)

    time.sleep(1)

