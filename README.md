
# 🚀 Kafka Streaming Pipeline with PySpark & PostgreSQL

This project demonstrates a **production-grade, real-time data streaming pipeline** that simulates car telemetry data, validates it using PySpark Structured Streaming, and stores clean and corrupt records in PostgreSQL. A robust column-level quality control layer ensures only trustworthy data is ingested.

---

## 📚 Use Case

This project emulates a real-world IoT telemetry pipeline, common in ride-sharing, logistics, or automotive systems. Events like fuel level, speed, GPS, and engine temperature are streamed and validated in real-time — simulating millions of rows per day.

---

## 🏗️ Architecture Overview

```text
                       +---------------------+
                       |  External API Queue |
                       +----------+----------+
                                  |
                         Retry if failure
                                  |
                         ┌────────▼─────────┐
                         |   Kafka Producer  |
                         └────────┬─────────┘
                                  |
                        Sends JSON to Kafka Topic (raw_telemetry)
                                  |
                                  ▼
                         ┌────────────────────┐
                         │  Spark Structured  │
                         │    Streaming Job   │
                         └────────┬───────────┘
                                  |
                       +----------+----------+
                       |                     |
               ┌───────▼───────┐     ┌───────▼──────┐
               │ Schema Valid? │     │ Invalid?     │
               └───────┬───────┘     └───────┬──────┘
                       |                    |
                       ▼                    ▼
             +-----------------+     +--------------------+
             | Post-Schema QC  |     | corrupt_records DB |
             | on valid rows   |     +--------------------+
             +--------+--------+
                      |
                      ▼
        ┌─────────────────────────────┐
        │ Column-level QC (cell wise) │
        └───────┬────────────┬────────┬─────────────┐
                |            |        |             |
                ▼            ▼        ▼             ▼
     +----------------+   +------------------+   +---------------------+
     | device_data DB |   | qc_failed_table  |   | qc_audit_log_table  |
     | qc_status=OK   |   | qc_status=FAIL   |   | cell-level QC log   |
     +----------------+   +------------------+   +---------------------+
````

---

## 📂 Project Structure

```text
kafka-streaming-pipeline/
│
├── docker-compose.yaml         # Launches Kafka, Zookeeper, Postgres
├── Dockerfile.spark            # Spark image with dependencies
├── db_schema.sql               # SQL file to create PostgreSQL tables
│
├── kafka_producer.py           # Simulates car telemetry and sends to Kafka
├── logger_success.py           # Logs successful producer sends
├── logger_failure.py           # Logs failures + retry queue logic
│
├── producer_success.log        # Log of success messages
├── producer_failure.log        # Log of retry/failure messages
│
└── jobs/
    ├── consumer_grp.py         # PySpark consumer with schema & QC logic
    ├── qc_checks.py            # Contains column-level validation functions
    ├── spark_log.log           # Runtime logs
```

---

## 🔧 Tech Stack

| Component        | Technology                     |
| ---------------- | ------------------------------ |
| Message Broker   | Kafka + Zookeeper              |
| Stream Processor | PySpark (Structured Streaming) |
| Storage          | PostgreSQL                     |
| Retry Mechanism  | Custom API Queue               |
| Containerization | Docker + Compose               |
| Language         | Python 3.9+                    |

---

## 🗃️ PostgreSQL Table Design

### ✅ Clean Records Table: `device_data`

| Column                 | Type      | Description              |
| ---------------------- | --------- | ------------------------ |
| trip\_id               | STRING    | Unique trip identifier   |
| car\_id                | STRING    | Car identifier           |
| latitude               | DOUBLE    | Latitude at event time   |
| longitude              | DOUBLE    | Longitude at event time  |
| event\_timestamp       | TIMESTAMP | Original event timestamp |
| speed\_kmph            | DOUBLE    | Car speed                |
| fuel\_level            | DOUBLE    | Fuel level               |
| engine\_temp\_c        | DOUBLE    | Engine temperature       |
| trip\_start\_time      | TIMESTAMP | Trip start time          |
| trip\_start\_latitude  | DOUBLE    | Latitude at trip start   |
| trip\_start\_longitude | DOUBLE    | Longitude at trip start  |
| trip\_start\_date      | DATE      | Partition key            |
| message\_key           | STRING    | Kafka message key        |
| kafka\_partition       | INT       | Kafka partition          |
| kafka\_offset          | BIGINT    | Kafka offset             |
| qc\_status             | STRING    | 'success'                |
| load\_timestamp        | TIMESTAMP | Ingestion time           |

---

### ❌ Corrupt Records Table: `corrupt_records`

| Column           | Type      | Description               |
| ---------------- | --------- | ------------------------- |
| message\_key     | STRING    | Kafka message key         |
| kafka\_partition | INT       | Kafka partition           |
| kafka\_offset    | BIGINT    | Kafka offset              |
| value\_str       | STRING    | Raw JSON value            |
| error\_reason    | STRING    | Reason for schema failure |
| load\_timestamp  | TIMESTAMP | Ingestion time            |

---

### ❌ Failed QC Table: `qc_failed_table`

| Column               | Type          | Description                             |
| -------------------- | ------------- | --------------------------------------- |
| All input columns... | As per schema |                                         |
| qc\_status           | STRING        | 'fail'                                  |
| qc\_summary          | STRING        | Brief reason (e.g., "fuel\_level null") |
| load\_timestamp      | TIMESTAMP     | Ingestion time                          |

---

### 📋 QC Audit Table: `qc_audit_log_table`

| Column       | Type      | Description                   |
| ------------ | --------- | ----------------------------- |
| trip\_id     | STRING    | Identifier for the trip       |
| column\_name | STRING    | Column that was checked       |
| passed       | BOOLEAN   | Whether this column passed QC |
| fail\_reason | STRING    | If failed, reason why         |
| checked\_at  | TIMESTAMP | Timestamp of QC check         |

---

## 🛠️ Setup Instructions

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/yourusername/kafka-streaming-pipeline.git
cd kafka-streaming-pipeline
```

---

### 2️⃣ Launch Services (Kafka, Zookeeper, Postgres, Spark)

```bash
docker-compose up --build
```

---

### 3️⃣ Create PostgreSQL Tables

```bash
psql -h localhost -U pst_docker -d kfk_sp_db -f db_schema.sql
```

---

### 4️⃣ Run Kafka Producer (Simulates Telemetry Events)

```bash
python kafka_producer.py
```

---

### 5️⃣ Run Spark Streaming Consumer

```bash
cd jobs
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.5 consumer_grp.py
```

---

## ✅ Features Completed

* [x] Kafka + Spark + PostgreSQL integration
* [x] Real-time JSON ingestion with schema validation
* [x] Separation of valid vs corrupt records
* [x] Column-level QC checks (nulls, thresholds, bounds)
* [x] Clean/failed data routed to different PostgreSQL tables
* [x] Cell-wise audit table for every QC decision
* [x] Retry queue logic in producer for Kafka delivery failures
* [x] Dockerized full pipeline for easy deployment
* [ ] Streamlit dashboard for vehicle-level QC analytics (coming soon)


---

## 📈 Example Applications

* Real-time IoT monitoring
* Vehicle/Logistics telemetry analytics
* Data pre-QC before lake ingestion
* Fraud detection preprocessing
* Auditable data pipelines with fine-grained QC

---


