from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date, lit
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Expected schema for your telemetry JSON data
expected_schema = StructType() \
    .add("trip_id", StringType()) \
    .add("car_id", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", TimestampType()) \
    .add("speed_kmph", DoubleType()) \
    .add("fuel_level", DoubleType()) \
    .add("engine_temp_c", DoubleType()) \
    .add("trip_start_time", TimestampType()) \
    .add("trip_start_latitude", DoubleType()) \
    .add("trip_start_longitude", DoubleType())

def compare_with_expected_schema(df):
    expected_fields = {f.name: f.dataType for f in expected_schema}
    actual_fields = {f.name: f.dataType for f in df.schema}
    discrepancies = []

    all_fields = set(expected_fields.keys()) | set(actual_fields.keys())
    for field in all_fields:
        if field not in actual_fields:
            discrepancies.append(f"Missing column: {field}")
        elif field not in expected_fields:
            discrepancies.append(f"Unexpected column: {field}")
        elif expected_fields[field] != actual_fields[field]:
            discrepancies.append(f"Type mismatch for column '{field}': expected {expected_fields[field]}, got {actual_fields[field]}")

    return discrepancies

def process_kafka_stream(
    group_id, topic_name, checkpoint_dir,
    db_url, db_clean_table, db_corrupt_table, db_log_table,
    db_user, db_password):

    spark = (
        SparkSession.builder
        .appName(f"Kafka Consumer Group: {group_id}")
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config("spark.sql.shuffle.partitions", 4)
        .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.5')\
        .getOrCreate()
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON from Kafka value
    parsed_df = kafka_df.withColumn("value_str", col("value").cast("string")) \
        .withColumn("parsed", from_json("value_str", expected_schema))

    # Extract columns from parsed JSON
    enriched_df = parsed_df.select(
        col("parsed.trip_id").alias("trip_id"),
        col("parsed.car_id").alias("car_id"),
        col("parsed.latitude").alias("latitude"),
        col("parsed.longitude").alias("longitude"),
        col("parsed.timestamp").alias("timestamp"),
        col("parsed.speed_kmph").alias("speed_kmph"),
        col("parsed.fuel_level").alias("fuel_level"),
        col("parsed.engine_temp_c").alias("engine_temp_c"),
        col("parsed.trip_start_time").alias("trip_start_time"),
        col("parsed.trip_start_latitude").alias("trip_start_latitude"),
        col("parsed.trip_start_longitude").alias("trip_start_longitude"),
        to_date(col("parsed.trip_start_time")).alias("trip_start_date"),
        col("key").cast("string").alias("message_key"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        current_timestamp().alias("load_timestamp"),
        col("value_str")  # keep raw JSON for corrupt data logging
    )

    def write_to_postgres(df, target_table):
        df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", target_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .save()

    def log_error(error_message, batch_id, target_table):
        # Create a DataFrame explicitly with schema to avoid inference issues
        error_data = [(error_message, batch_id)]
        error_schema = "error_message STRING, batch_id LONG"
        error_df = spark.createDataFrame(error_data, schema=error_schema) \
            .withColumn("log_time", current_timestamp()) \
            .withColumn("target_table", lit(target_table))

        error_df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_log_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .save()

    def write_batch_with_schema_check(batch_df, batch_id):
        try:
            check_df = batch_df.select(
                            col("trip_id"),
                            col("car_id"),
                            col("latitude"),
                            col("longitude"),
                            col("timestamp"),
                            col("speed_kmph"),
                            col("fuel_level"),
                            col("engine_temp_c"),
                            col("trip_start_time"),
                            col("trip_start_latitude"),
                            col("trip_start_longitude")
                            
                        )
            check_df.show()
            discrepancies = compare_with_expected_schema(check_df)

            if discrepancies:
                # For corrupt records, keep raw JSON + error reason, match corrupt_records table columns exactly
                corrupt_df = batch_df.select(
                    col("message_key"),
                    col("kafka_partition"),
                    col("kafka_offset"),
                    col("value_str").alias("value_str"),
                    lit("; ".join(discrepancies)).alias("error_reason"),
                    col("load_timestamp"),
                    lit(topic_name).alias("topic_name")  # optional if you want to track topic
                )
                write_to_postgres(corrupt_df, db_corrupt_table)
            else:
                # Clean data: remove value_str before writing
                clean_df = batch_df.drop("value_str")
                write_to_postgres(clean_df, db_clean_table)

        except Exception as e:
            log_error(str(e), batch_id, "schema_check")

    query = enriched_df.writeStream \
        .foreachBatch(write_batch_with_schema_check) \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/clean") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    process_kafka_stream(
        group_id="kafka_consumer_group_1",
        topic_name="multi_car_telemetry2",
        checkpoint_dir="/opt/bitnami/spark/checkpoints2/group1",
        db_url="jdbc:postgresql://postgres:5432/kfk_sp_db",
        db_clean_table="device_data",
        db_corrupt_table="corrupt_records",
        db_log_table="consumer_error_log",
        db_user="pst_docker",
        db_password="abhi9876"
    )
