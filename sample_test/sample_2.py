from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BatchToPostgres") \
    .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.5')\
    .getOrCreate()

# Create a simple DataFrame
data = [(5, "Alice"), (4, "Bob"), (6, "Charlie")]
df = spark.createDataFrame(data, ["id", "name"])
df.show()
# Write DataFrame to PostgreSQL
df.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://postgres:5432/kfk_sp_db") \
  .option("dbtable", "sample_data") \
  .option("user", "pst_docker") \
  .option("password", "abhi9876") \
  .option("driver", "org.postgresql.Driver") \
  .mode("append") \
  .save()

spark.stop()
