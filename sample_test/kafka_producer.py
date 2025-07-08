from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import uuid
## to generate  telemetry data of cars moving from one location to another 


# -------- Callbacks for Logging --------

from logger_success import success_logger
from logger_failure import failure_logger

# Global counters
total_sent = 0
total_success = 0
total_failed = 0

# -- Callbacks --

def on_send_success(record_metadata,message):
    try:
        print('on success print')
        #print(f'Record metadata: {record_metadata}')
        #print(f'Message: {message}')
        #msg = f'Successfully produced: "{message}"'
        print("""Successfully produced "{}" to topic {} and partition {} at offset {}""".format(message,record_metadata.topic,record_metadata.partition,record_metadata.offset))
        msg = (f'message = {message} topic="{record_metadata.topic}", partition={record_metadata.partition}, offset={record_metadata.offset}')
        print(msg)
        success_logger.info(msg)
        global total_success
        total_success =total_success +1
    except Exception as e:
        print(f"Exception in on_send_success: {e}")

def on_send_error(excp, message):
    print('on failure print')
    msg = f'Failed to send message: "{message}", error: {excp}'
    print(msg)
    failure_logger.error(msg)
    global total_failed
    total_failed = total_failed+1

# -------- Topic Management --------

def create_topic(admin_client, topic_name, num_partitions, replication_factor):
    try:
        topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Error creating topic: {e}")

def topic_exists(admin_client, topic_name):
    try:
        topics = admin_client.list_topics()
        return topic_name in topics
    except Exception as e:
        print(f"Error checking topic existence: {e}")
        return False

# -------- Data Generator --------

def generate_fake_movement_data(start, end, producer, topic_name, car_id, num_points=100):
    lat_step = (end[0] - start[0]) / num_points
    lng_step = (end[1] - start[1]) / num_points
      # Trip metadata generated ONCE per trip
    trip_id = str(uuid.uuid4()) # each trip start will have a unique id
    trip_start_time = datetime.now().isoformat()
    trip_start_lat = start[0]
    trip_start_lng = start[1]

    for i in range(num_points):
        lat = start[0] + lat_step * i + random.uniform(-0.01, 0.01)
        lng = start[1] + lng_step * i + random.uniform(-0.01, 0.01)
        timestamp = datetime.now().isoformat()
        speed = round(random.uniform(40, 120), 2)
        fuel = round(random.uniform(10, 100), 2)
        temp = round(random.uniform(70, 110), 2)

        data_point = {
            "trip_id": trip_id,
            "car_id": car_id,
            "latitude": lat,
            "longitude": lng,
            "timestamp": timestamp,
            "speed_kmph": speed,
            "fuel_level": fuel,
            "engine_temp_c": temp,

            # Add trip-level metadata these wil be the key columns
            "trip_start_time": trip_start_time,
            "trip_start_latitude": trip_start_lat,
            "trip_start_longitude": trip_start_lng
        }

        message_json = json.dumps(data_point)
        key = car_id

        try:
            print(f"[{car_id}] Sending message...")
            global total_sent
            total_sent =total_sent +1
            ## send the data to broker 
            future = producer.send(topic_name, key=key, value=data_point)
            ## if success need to run this 
            future.add_callback(on_send_success, message= message_json)
            ## if error  need to run this 
            future.add_errback(on_send_error,message=message_json)
            print(f"[{car_id}] waiting for call back mesage ...")
        except Exception as e:
            print(f"[{car_id}] Immediate send failure: {e}")

        time.sleep(random.uniform(0.5, 2.0))  # Simulate realistic interval

    producer.flush()
    print(f"[{car_id}] Message stream completed.")

# -------- Main --------

if __name__ == "__main__":
    bootstrap_servers = "localhost:29092"
    topic_name = "multi_car_telemetry1"
    num_partitions = 5
    replication_factor = 1
    num_cars = 2  # Simulate 5 cars

    try:
        # Create topic if needed
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id='admin_client')
        if not topic_exists(admin_client, topic_name):
            create_topic(admin_client, topic_name, num_partitions, replication_factor)
        admin_client.close()

        # Kafka producer configuration
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8'),
            acks='all', ## acknowledge from broker  that the message is received by it 
            retries=1,  # Kafka will retry up to 5 times
            
            enable_idempotence=True,  # Prevent duplicate delivery
            linger_ms=10, 
            batch_size=16384,
            request_timeout_ms=5000,  # 5 seconds timeout
            delivery_timeout_ms=10000, # total max time to deliver message
            compression_type='gzip',
            security_protocol="PLAINTEXT"
        )

        # Start location range (Kolkata base)
        base_lat, base_lng = 22.5726, 88.3639

        def simulate_car(car_num):
            car_id = f"Car_{car_num}"
            start = (base_lat + random.uniform(-0.1, 0.1), base_lng + random.uniform(-0.1, 0.1))
            end = (start[0] + random.uniform(0.2, 1.0), start[1] + random.uniform(0.2, 1.0))
            generate_fake_movement_data(start, end, producer, topic_name, car_id)

        # Start car threads
        with ThreadPoolExecutor(max_workers=num_cars) as executor:
            for i in range(num_cars):
                executor.submit(simulate_car, i)

        producer.flush()
        producer.close()
        print("All car simulations completed.")
        print(f"Total Sent: {total_sent}, Success: {total_success}, Failed: {total_failed}")
        for handler in success_logger.handlers:
            handler.flush()
            handler.close()

        for handler in failure_logger.handlers:
            handler.flush()
            handler.close()
    except Exception as e:
        print(f"Main program error: {e}")
