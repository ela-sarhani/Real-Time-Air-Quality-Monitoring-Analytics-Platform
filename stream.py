from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("AirQualityStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("pm25", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("timestamp", TimestampType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "iot-sensors") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

alerts = json_df.filter(
    (col("pm25") > 25) | (col("no2") > 40)
).withColumn(
    "alert",
    when(col("pm25") > 25, "PM2.5 HIGH")
    .when(col("no2") > 40, "NO2 HIGH")
    .otherwise("UNKNOWN")
)

query = alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()