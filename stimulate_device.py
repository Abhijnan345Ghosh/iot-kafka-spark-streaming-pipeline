import requests
import random
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import uuid

API_ENDPOINT = "http://localhost:8000/iot-data"
NUM_CARS = 2
BASE_LAT, BASE_LNG = 22.5726, 88.3639

def simulate_car(car_num):
    car_id = f"Car_{car_num}"
    trip_id = str(uuid.uuid4())
    trip_start_time = datetime.now().isoformat()
    start = (BASE_LAT + random.uniform(-0.1, 0.1), BASE_LNG + random.uniform(-0.1, 0.1))
    end = (start[0] + random.uniform(0.2, 1.0), start[1] + random.uniform(0.2, 1.0))
    num_points = 100
    lat_step = (end[0] - start[0]) / num_points
    lng_step = (end[1] - start[1]) / num_points

    for i in range(num_points):
        lat = start[0] + lat_step * i + random.uniform(-0.01, 0.01)
        lng = start[1] + lng_step * i + random.uniform(-0.01, 0.01)
        timestamp = datetime.now().isoformat()
        speed = round(random.uniform(40, 120), 2)
        fuel = round(random.uniform(10, 100), 2)
        temp = round(random.uniform(70, 110), 2)

        payload = {
            "trip_id": trip_id,
            "car_id": car_id,
            "latitude": lat,
            "longitude": lng,
            "timestamp": timestamp,
            "speed_kmph": speed,
            "fuel_level": fuel,
            "engine_temp_c": temp,
            "trip_start_time": trip_start_time,
            "trip_start_latitude": start[0],
            "trip_start_longitude": start[1]
        }

        try:
            res = requests.post(API_ENDPOINT, json=payload)
            print(f"[{car_id}] ✅ API Response: {res.status_code}")
        except Exception as e:
            print(f"[{car_id}] ❌ Failed to POST: {e}")

        time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=NUM_CARS) as executor:
        for i in range(NUM_CARS):
            executor.submit(simulate_car, i)
