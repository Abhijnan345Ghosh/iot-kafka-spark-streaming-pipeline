# api/kafka_producer.py
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
from logger_success import success_logger
from logger_failure import failure_logger

TOPIC_NAME = "multi_car_telemetry2"
BOOTSTRAP_SERVERS = "localhost:29092"

# Ensure topic exists
try:
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id='api-producer')
    topics = admin_client.list_topics()
    if TOPIC_NAME not in topics:
        topic = NewTopic(name=TOPIC_NAME, num_partitions=5, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"Topic '{TOPIC_NAME}' created.")
    else:
        print(f"Topic '{TOPIC_NAME}' already exists.")
    admin_client.close()
except Exception as e:
    print(f"Error ensuring topic exists: {e}")

# Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8'),
    acks='all',
    retries=1,
    enable_idempotence=True,
    linger_ms=10,
    batch_size=16384,
    request_timeout_ms=5000,
    delivery_timeout_ms=10000,
    compression_type='gzip',
    security_protocol="PLAINTEXT"
)

def on_send_success(record_metadata, message):
    try:
        msg = f"Successfully produced message to {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}"
        success_logger.info(msg)
    except Exception as e:
        print(f"Success callback exception: {e}")

def on_send_error(excp, message):
    msg = f"Failed to send message: {message}, error: {excp}"
    failure_logger.error(msg)

def send_to_kafka(payload: dict):
    try:
        key = payload["car_id"]
        message = json.dumps(payload)
        future = producer.send(TOPIC_NAME, key=key, value=payload)
        future.add_callback(on_send_success, message=message)
        future.add_errback(on_send_error, message=message)
    except Exception as e:
        failure_logger.error(f"Immediate Kafka failure: {e}")
