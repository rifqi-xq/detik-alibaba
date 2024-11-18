from confluent_kafka import Producer
from kafka_producer_withapi import send_to_kafka
import setting

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

conf = setting.kafka_setting
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

app = FastAPI()

# Define the data model for incoming streaming data
class StreamData(BaseModel):
    timestamp: str
    temperature: float


# API endpoint
@app.post("/stream-data")
async def receive_stream_data(data: StreamData):
    try:
        send_to_kafka(data.dict())
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to send data to Kafka: {e}"
        )

    return {"status": "success", "data_sent": data.dict()}


# Ensure all messages are flushed from Kafka producer on shutdown
@app.on_event("shutdown")
def shutdown_event():
    producer.flush()
