from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from producer.setting import kafka_setting, oss_setting
from kafkaparser.service import Service

app = FastAPI()

# Kafka Configuration
kafka_conf = kafka_setting
oss_conf = oss_setting

kafka_config = {
    "bootstrap.servers": kafka_conf["bootstrap_servers"],
    "topic_name_01": kafka_conf["topic_name_01"],
    "topic_name_02": kafka_conf["topic_name_02"],
}

oss_config = {
    "oss_access_key_id": oss_conf["oss_access_key_id"],
    "oss_access_key_secret": oss_conf["oss_access_key_secret"],
    "oss_endpoint": oss_conf["oss_endpoint"],
    "oss_bucket_name": oss_conf["oss_bucket_name"],
}

consumer_topics = [kafka_conf["topic_name_01"], kafka_conf["topic_name_02"]]

service = Service(
    kafka_config=kafka_config,
    consumer_topics=consumer_topics,
    oss_config=oss_config,
    base_path="data/logs",
    worker_count=3,
    batch_interval=60,
)

# Define the request schema
class StreamDataRequest(BaseModel):
    data: dict

@app.post("/stream-data")
async def stream_data(request: StreamDataRequest):
    try:
        service.produce_data(request.data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to produce data: {e}")
    return {"status": "success", "data_sent": request.data}

@app.on_event("startup")
def on_startup():
    service.start()

@app.on_event("shutdown")
def on_shutdown():
    service.stop()
