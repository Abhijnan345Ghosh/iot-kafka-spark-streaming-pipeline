from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("MyApp") \
    .config("subscribe", "multi_car_telemetry") \
    .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.5')\
    .getOrCreate()

# Try creating Kafka read stream (won't actually run, just triggers jar download)
df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "multi_car_telemetry") \
    .load()
df.show(3)

spark.stop()
