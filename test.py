from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.range(0, 10)
df.show()

spark.stop()