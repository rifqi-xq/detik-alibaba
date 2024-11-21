import time
import requests
from confluent_kafka import Producer
import setting

# Load Kafka configuration
conf = setting.kafka_setting

# Initialize the Kafka producer
producer = Producer(
    {
        "bootstrap.servers": conf["bootstrap_servers"],
        "sasl.mechanisms": "PLAIN",
        "ssl.ca.location": conf["ca_location"],
        "security.protocol": "SASL_SSL",
        "ssl.endpoint.identification.algorithm": "none",
        "sasl.username": conf["sasl_plain_username"],
        "sasl.password": conf["sasl_plain_password"],
    }
)

api_url = "http://127.0.0.1:8000/stream-data"
topic_name = conf["topic_name"]

def fetch_data_from_api():
    """
    Fetch data from the FastAPI endpoint.
    """
    try:
        response = requests.get(api_url)  # Adjust method if endpoint requires POST or other HTTP methods
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None


def send_to_kafka(data: dict):
    """
    Send data to Kafka.
    """
    try:
        producer.produce(topic_name, key=None, value=str(data))
        producer.flush()
        print(f"Sent to Kafka: {data}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")


if __name__ == "__main__":
    print("Starting Kafka Producer...")
    while True:
        data = fetch_data_from_api()
        if data:
            send_to_kafka(data)
        time.sleep(1)  # Poll API every 5 seconds
