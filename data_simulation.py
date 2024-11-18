import requests
import time
import random
from datetime import datetime

api_url = "http://192.168.0.79:8000/stream-data"

def generate_streaming_data():
    timestamp = datetime.utcnow().isoformat()
    data = random.uniform(0, 100)
    return {"timestamp": timestamp, "temperature": data}

def send_streaming_data():
    while True:
        data_payload = generate_streaming_data()
        try:
            response = requests.post(api_url, json=data_payload)
            if response.status_code == 200:
                print(f"Data sent successfully: {data_payload}")
            else:
                print(f"Failed to send data: {response.status_code} - {response.text}")
        except requests.ConnectionError:
            print("Failed to connect to the API server.")
            break
        
        # Simulate 1 second delay for data sending
        time.sleep(1)

if __name__ == "__main__":
    send_streaming_data()
