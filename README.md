
# 🚀 Kafka Streaming Pipeline with PySpark & PostgreSQL

This project implements a real-time, production-grade data streaming pipeline that simulates car telemetry data using a Kafka producer, performs schema validation and quality control using Spark Structured Streaming, and stores results into PostgreSQL.

---

## 📚 Use Case

Millions of telemetry or log events are generated every second in real-world systems like ride-sharing, IoT, logistics, and vehicle monitoring. This project demonstrates a high-throughput ingestion and processing pipeline that ensures data correctness via schema and QC checks.

---

## 🏗️ Architecture

```text
 Kafka Producer (Python) --> Kafka Topic (car_telemetry) --> Spark Structured Streaming
                                                             |
                                                 ┌───────────┴───────────┐
                                                 |                       |
                                         Clean Records               Corrupt Records
                                       → PostgreSQL                  → PostgreSQL
                                       (device_data)                 (corrupt_records)
```

---

## 📂 Project Structure

```text
kafka-streaming-pipeline/
│
├── docker-compose.yaml         # Launches Kafka, Zookeeper, Postgres
├── Dockerfile.spark            # Spark Docker image
├── db_schema.sql               # SQL file to create Postgres tables
│
├── kafka_producer.py           # Kafka producer to simulate telemetry data
├── logger_success.py           # Logs successful producer messages
├── logger_failure.py           # Logs failed producer messages
│
├── producer_success.log        # Log of success messages
├── producer_failure.log        # Log of failure messages
│
└── jobs/
    ├── consumer_grp.py         # PySpark consumer for schema & QC validation
    ├── spark_log.log           # Runtime logs
```

---

## 🔧 Tech Stack

| Component         | Technology        |
|------------------|-------------------|
| Message Broker    | Kafka + Zookeeper |
| Stream Processor  | PySpark (Structured Streaming) |
| Storage           | PostgreSQL        |
| Containerization  | Docker + Compose  |
| Language          | Python 3.9+       |

---

## 📦 PostgreSQL Table Design

### ✅ Clean Data Table: `device_data`

| Column               | Type        | Description                       |
|----------------------|-------------|-----------------------------------|
| trip_id              | STRING      | Unique trip identifier            |
| car_id               | STRING      | Car identifier                    |
| latitude             | DOUBLE      | Latitude at event time            |
| longitude            | DOUBLE      | Longitude at event time           |
| event_timestamp      | TIMESTAMP   | Original event timestamp          |
| speed_kmph           | DOUBLE      | Car speed                         |
| fuel_level           | DOUBLE      | Fuel level                        |
| engine_temp_c        | DOUBLE      | Engine temperature                |
| trip_start_time      | TIMESTAMP   | Trip start time                   |
| trip_start_latitude  | DOUBLE      | Latitude at trip start            |
| trip_start_longitude | DOUBLE      | Longitude at trip start           |
| trip_start_date      | DATE        | Partition key                     |
| message_key          | STRING      | Kafka message key                 |
| kafka_partition      | INT         | Kafka partition                   |
| kafka_offset         | BIGINT      | Kafka offset                      |
| load_timestamp       | TIMESTAMP   | Data ingestion time               |

---

### ❌ Corrupt Records Table: `corrupt_records`

| Column          | Type        | Description                       |
|------------------|-------------|-----------------------------------|
| message_key     | STRING      | Kafka message key                 |
| kafka_partition | INT         | Kafka partition                   |
| kafka_offset    | BIGINT      | Kafka offset                      |
| value_str       | STRING      | Raw JSON value                    |
| error_reason    | STRING      | Reason for failure                |
| load_timestamp  | TIMESTAMP   | Ingestion timestamp               |

---

### 📋 Error Log Table: `consumer_error_log`

| Column         | Type        | Description                       |
|----------------|-------------|-----------------------------------|
| error_message  | STRING      | Full error traceback              |
| log_time       | TIMESTAMP   | Error occurrence time             |
| target_table   | STRING      | Table attempted to write          |
| batch_id       | STRING      | Spark batch ID                    |

---

## 🛠️ Setup Instructions

### 🔁 1. Clone the Repo
```bash
git clone https://github.com/yourusername/kafka-streaming-pipeline.git
cd kafka-streaming-pipeline
```

### 🐳 2. Start Kafka + Postgres + Spark
```bash
docker-compose up --build
```

### 🗃️ 3. Create PostgreSQL Tables
```bash
psql -h localhost -U pst_docker -d kfk_sp_db -f db_schema.sql
```

### 🚗 4. Run Kafka Producer
```bash
python kafka_producer.py
```

### 🔥 5. Run Spark Kafka Consumer
```bash
cd jobs
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.5 consumer_grp.py
```

---

## ✅ Features Completed

- [x] Kafka + Spark + PostgreSQL integration
- [x] Schema validation for incoming JSON
- [x] Raw & corrupt record separation
- [x] Column-wise QC logic in second layer (null checks, range validation)
- [x] Dockerized environment
- [ ] Streamlit Dashboard (coming soon)
- [ ] FastAPI producer for REST-based mock data (future)

---

## 📈 Example Use Cases

- IoT sensor pipelines  
- Real-time vehicle monitoring  
- Fraud detection pre-processing  
- Data lake staging before warehouse  

---

## 👨‍💻 Author

Developed by [Your Name].  
Want help integrating dashboards or alerts? Reach out or open a PR.

---

## 📄 License

This project is licensed under the MIT License.

'''
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.5 --py-files con_grp_upd.py con_gr_upd.py > sc.log
'''