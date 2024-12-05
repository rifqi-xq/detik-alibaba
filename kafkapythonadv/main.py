from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafkaparser.service import Service
from setting import kafka_setting, oss_setting

app = FastAPI()

consumer_topics = [kafka_setting["topic_name_01"], kafka_setting["topic_name_02"]]

# Initialize the Service
service = Service(
    kafka_config=kafka_setting,
    consumer_topics=consumer_topics,
    oss_config=oss_setting,
    base_path="data/logs",
    worker_count=3,
    batch_interval=5,
)

# Define a request schema for the endpoint
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
def startup_event():
    service.start()

@app.on_event("shutdown")
def shutdown_event():
    service.stop()
