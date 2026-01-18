import os
import json
import random
import time
from google.cloud import pubsub_v1

# 🔑 Path to your service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\User\Pictures\Zafri USM\CPC357\cpc357-iot-assignment-2-dc758cf09f74.json"

# GCP settings
project_id = "cpc357-iot-assignment-2"
topic_id = "sensor-data"

# Force same region as Cloud Function (asia-southeast1)
publisher = pubsub_v1.PublisherClient(
    client_options={"api_endpoint": "asia-southeast1-pubsub.googleapis.com:443"}
)

# Full topic path
topic_path = f"projects/cpc357-iot-assignment-2/topics/sensor-data"

print("Publishing sensor data to:", topic_path)

while True:
    data = {
        "temperature": round(random.uniform(20, 35), 2),
        "humidity": round(random.uniform(40, 80), 2),
        "device_id": "DHT11_SIM_01"
    }

    message = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, message)
    future.result()  # wait for publish to complete

    print("Published:", data)
    time.sleep(5)
