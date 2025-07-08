from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date, lit
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from schema_validation import compare_with_expected_schema  # custom validation module

def process_kafka_stream(group_id, topic_name, checkpoint_dir, db_url, db_clean_table, db_corrupt_table, db_log_table, db_user, db_password):
    spark = (
        SparkSession.builder
        .appName(f"Kafka Consumer Group: {group_id}")
        .master("local[*]")
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )

    # Import expected schema from your validation module
    from schema_validation import expected_schema

    try:
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:9092")
            .option("subscribe", topic_name)
            .option("startingOffsets", "earliest")
            .load()
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e

    parsed_df = (
        kafka_df.withColumn("value_str", col("value").cast("string"))
                .withColumn("parsed", from_json("value_str", expected_schema))
    )

    enriched_df = (
        parsed_df
        .withColumn("trip_id", col("parsed.trip_id"))
        .withColumn("car_id", col("parsed.car_id"))
        .withColumn("latitude", col("parsed.latitude"))
        .withColumn("longitude", col("parsed.longitude"))
        .withColumn("event_timestamp", col("parsed.timestamp"))
        .withColumn("speed_kmph", col("parsed.speed_kmph"))
        .withColumn("fuel_level", col("parsed.fuel_level"))
        .withColumn("engine_temp_c", col("parsed.engine_temp_c"))
        .withColumn("trip_start_time", col("parsed.trip_start_time"))
        .withColumn("trip_start_latitude", col("parsed.trip_start_latitude"))
        .withColumn("trip_start_longitude", col("parsed.trip_start_longitude"))
        .withColumn("trip_start_date", to_date(col("parsed.trip_start_time")))
        .withColumn("load_timestamp", current_timestamp())
        .withColumn("message_key", col("key").cast("string"))
        .withColumn("kafka_partition", col("partition"))
        .withColumn("kafka_offset", col("offset"))
        # Include raw Kafka columns needed for error handling
        .withColumn("value_str", col("value_str"))
        .withColumn("key", col("key"))
        .withColumn("partition", col("partition"))
        .withColumn("offset", col("offset"))
        .select(
            "trip_id", "car_id", "latitude", "longitude", "event_timestamp",
            "speed_kmph", "fuel_level", "engine_temp_c",
            "trip_start_time", "trip_start_latitude", "trip_start_longitude", "trip_start_date",
            "message_key", "kafka_partition", "kafka_offset", "load_timestamp",
            "value_str", "key", "partition", "offset"
        )
    )

    def write_to_postgres(df, target_table, batch_id):
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
        error_df = spark.createDataFrame(
            [(error_message, current_timestamp(), target_table, batch_id)],
            ["error_message", "log_time", "target_table", "batch_id"]
        )
        error_df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_log_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .save()

    def write_batch_with_schema_check(df, batch_id):
        try:
            print('starting schema validation')
            discrepancies = compare_with_expected_schema(df)

            if discrepancies:
                corrupt_df = df.withColumn("value_str", col("value_str")) \
                            .withColumn("error_reason", lit("; ".join(discrepancies))) \
                            .withColumn("load_timestamp", current_timestamp()) \
                            .withColumn("message_key", col("message_key")) \
                            .withColumn("kafka_partition", col("kafka_partition")) \
                            .withColumn("kafka_offset", col("kafka_offset")) \
                            .select("raw_json", "message_key", "kafka_partition", "kafka_offset", "error_reason", "load_timestamp")

                write_to_postgres(corrupt_df, db_corrupt_table, batch_id)
            else:
                write_to_postgres(df.drop("value_str", "key", "partition", "offset"), db_clean_table, batch_id)

        except Exception as e:
            log_error(str(e), batch_id, "schema_check")
        

    query = (
        enriched_df.writeStream
        .foreachBatch(write_batch_with_schema_check)
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_dir}/clean")
        .start()
    )

    query.awaitTermination()


# ---- Main Entrypoint ----
if __name__ == "__main__":
    group_id = "kafka_consumer_group_1"
    topic_name = "multi_car_telemetry"
    checkpoint_dir = "/opt/bitnami/spark/checkpoints/group1"
    db_url = "jdbc:postgresql://postgres:5432/kfk_sp_db"
    db_user = "pst_docker"
    db_password = "abhi9876"

    db_clean_table = "device_data"
    db_corrupt_table = "corrupt_records"
    db_log_table = "consumer_error_log"

    process_kafka_stream(
        group_id, topic_name, checkpoint_dir,
        db_url, db_clean_table, db_corrupt_table, db_log_table,
        db_user, db_password
    )
