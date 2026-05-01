from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("AirQualityStreaming") \
    .getOrCreate()

schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("pm25", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("timestamp", DoubleType())
])

# READ FROM KAFKA
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "iot-sensors") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ALERT RULES
alerts = json_df.filter(
    (col("pm25") > 25) | (col("no2") > 40)
).withColumn(
    "alert",
    when(col("pm25") > 25, "PM2.5 HIGH").otherwise("NO2 HIGH")
)

# OUTPUT TO CONSOLE (TEST FIRST)
query = alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
