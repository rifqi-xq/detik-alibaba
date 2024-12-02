from confluent_kafka import Producer, KafkaException
import json
import logging
import time
import setting
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Kafka producer configuration
kafka_conf = setting.kafka_setting
oss_conf = setting.oss_setting
producer = Producer(
    {
        "bootstrap.servers": kafka_conf["bootstrap_servers"],
    }
)

# Initiate FastAPI
app = FastAPI()

# Define the request schema
class StreamDataRequest(BaseModel):
    data: dict

# API endpoint
@app.post("/stream-data")
async def receive_stream_data(request: StreamDataRequest):
    try:
        # Access the `data` field from the request body
        data = request.data
        topic = detect_topic(data)
        produce_data(data, topic)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to send data to Kafka: {e}"
        )
    return {"status": "success", "data_sent": request}


# Ensure all messages are flushed from Kafka producer on shutdown
@app.on_event("shutdown")
def shutdown_event():
    producer.flush()


def detect_topic(data: dict):
    topic_apps = kafka_conf["topic_name_01"]
    topic_desktop = kafka_conf["topic_name_02"]
    
    if "device_brand" in data.keys():
        return topic_apps
    elif "thumbnailUrl" in data.keys():
        return topic_desktop
    else:
        return "unknown"

# Kafka producer
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_data(data: dict, topic: str):
    try:
        timestamp = int(time.time())  # Add timestamp to data
        data["timestamp"] = timestamp

        message = json.dumps(data).encode("utf8")
        # print(f"message: {message}")
        producer.produce(
            topic,
            message,
            callback=delivery_report,
        )
        producer.poll(0)
        logging.info(f"Data sent to topic {topic}")
    except KafkaException as e:
        logging.error(f"Kafka error: {e}")
    except Exception as e:
        logging.error(f"Error producing data: {e}")

