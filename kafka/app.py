from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from typing import Optional
import random

app = FastAPI()

# Data Model (Optional if you dynamically generate data in the GET request)
class StreamData(BaseModel):
    timestamp: str
    temperature: float


# Simulated database or live data generator
def generate_stream_data() -> dict:
    """
    Generate a random data record to simulate streaming data.
    """
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(20.0, 30.0), 2)
        }

# API endpoint to serve streaming data
@app.get("/stream-data")
async def get_stream_data():
    """
    Provide a single record of streaming data.
    """
    data = generate_stream_data()
    return data


# Optional: Health-check endpoint
@app.get("/health")
async def health_check():
    """
    Health-check endpoint to confirm the app is running.
    """
    return {"status": "ok"}
