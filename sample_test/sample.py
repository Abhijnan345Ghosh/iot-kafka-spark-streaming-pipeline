from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config("spark.jars", ",".join([
        "/opt/bitnami/spark/jars/commons-pool2-2.11.1.jar",
        "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar",
        "/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar",
        "/opt/bitnami/spark/jars/kafka-clients-3.5.1.jar",
        "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
    ])) \
    .getOrCreate()
# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "multi_car_telemetry") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse key, value, and timestamp
parsed_df = df.selectExpr(
    "CAST(key AS STRING) as record_key",
    "CAST(value AS STRING) as record_value",
    "timestamp as event_ts"
)

# Write batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/kfk_sp_db") \
            .option("dbtable", "raw_kafka_data") \
            .option("user", "pst_docker") \
            .option("password", "abhi9876") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"❌ Error writing batch {batch_id} to PostgreSQL:", str(e))

# Start query
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
