from confluent_kafka import Producer, KafkaException
import json
import logging
import time
import setting

from fastapi import FastAPI, HTTPException

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
        "ssl.endpoint.identification.algorithm": "none",
        "sasl.mechanisms": "PLAIN",
        "ssl.ca.location": kafka_conf["ca_location"],
        "security.protocol": "SASL_SSL",
        "sasl.username": kafka_conf["sasl_plain_username"],
        "sasl.password": kafka_conf["sasl_plain_password"],
        "group.id": kafka_conf["group_name"],
        "auto.offset.reset": "latest",
        "fetch.message.max.bytes": "524288",  # 512 KB
    }
)

# Initiate 
app = FastAPI()



################################################################

# API endpoint
@app.post("/stream-data")
async def receive_stream_data(data):
    try:
        produce_data(data.dict())
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to send data to Kafka: {e}"
        )
    return {"status": "success", "data_sent": data.dict()}

# Ensure all messages are flushed from Kafka producer on shutdown
@app.on_event("shutdown")
def shutdown_event():
    producer.flush()

################################################################

# Kafka producer
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_data(data:dict):
    try:
        timestamp = int(time.time())  # Add timestamp to data
        data["timestamp"] = timestamp
        
        message = json.dumps(data).encode("utf8")
        producer.produce(
            kafka_conf["topic"],
            message,
            callback=delivery_report,
        )
        producer.poll(0)
        logging.info(f"Data sent to topic {kafka_conf["topic"]}: {data}")
    except KafkaException as e:
        logging.error(f"Kafka error: {e}")
    except Exception as e:
        logging.error(f"Error producing data: {e}")


# if __name__ == "__main__":
#     produce_data(example_data)
