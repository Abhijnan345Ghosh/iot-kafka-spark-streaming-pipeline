from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = [(1, "a"), (2, "b")]
df = spark.createDataFrame(data, ["id", "value"])

df.show()

spark.stop()
