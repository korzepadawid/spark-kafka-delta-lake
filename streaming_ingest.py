from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, LongType, StringType

from delta import *


builder = (
    SparkSession.builder.appName("streaming-consuner")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

my_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"]
spark = configure_spark_with_delta_pip(
    builder, extra_packages=my_packages
).getOrCreate()

schema = StructType(
    [
        StructField("user_id", LongType(), True),
        StructField("movie_id", LongType(), True),
        StructField("event", StringType(), True),
        StructField("tz", LongType(), True),
    ]
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "spark-topic-1")
    .load()
)

events = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

stream = (
    events.writeStream.format("delta")
    .option("checkpointLocation", "/tmp/checkpoint")
    .start("/tmp/delta-table/bronze")
)

stream.awaitTermination()
spark.stop()