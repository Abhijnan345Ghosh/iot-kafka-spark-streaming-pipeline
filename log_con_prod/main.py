from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from .kafka_producer import send_to_kafka

app = FastAPI()

class TelemetryData(BaseModel):
    trip_id: str
    car_id: str
    latitude: float
    longitude: float
    timestamp: str
    speed_kmph: float
    fuel_level: float
    engine_temp_c: float
    trip_start_time: str
    trip_start_latitude: float
    trip_start_longitude: float

@app.post("/iot-data")
async def receive_data(payload: TelemetryData):
    try:
        send_to_kafka(payload.dict())
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka send failed: {str(e)}")
