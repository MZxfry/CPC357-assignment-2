import base64
import json
from google.cloud import firestore

db = firestore.Client()

def process_sensor_data(cloud_event):
    try:
        # cloud_event.data may arrive as bytes in 2nd-gen functions
        if isinstance(cloud_event.data, (bytes, bytearray)):
            payload = json.loads(cloud_event.data.decode("utf-8"))
        else:
            payload = cloud_event.data

        # Pub/Sub message is inside payload["message"]["data"]
        encoded = payload["message"]["data"]
        decoded = base64.b64decode(encoded).decode("utf-8")
        sensor_data = json.loads(decoded)

        db.collection("sensor_readings").add({
            "temperature": sensor_data["temperature"],
            "humidity": sensor_data["humidity"],
            "device_id": sensor_data["device_id"]
        })

        print("Data stored successfully:", sensor_data)
        return "OK", 200

    except Exception as e:
        print("Error processing message:", e)
        return "Error", 500
