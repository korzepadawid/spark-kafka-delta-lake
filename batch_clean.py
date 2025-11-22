from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("batch_clean_data").getOrCreate()

schema = StructType(
    [
        StructField("userId", LongType()),
        StructField("event", StringType()),
        StructField("movieId", LongType()),
        StructField("ts", TimestampType()),
    ]
)

df = spark.read.option("multiLine", "true").schema(schema).json("data/events.json")

df.show()
df.printSchema()

spark.stop()
