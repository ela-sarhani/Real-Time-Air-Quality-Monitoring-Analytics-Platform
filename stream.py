from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------------------------
# Spark Session
# ---------------------------
spark = SparkSession.builder \
    .appName("KafkaToPostgresAndHDFS") \
    .getOrCreate()

# ---------------------------
# Schema
# ---------------------------
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("pm25", FloatType()),
    StructField("no2", FloatType()),
    StructField("timestamp", StringType())
])

# ---------------------------
# Kafka Stream
# ---------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "air-quality") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json")

parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")

# Convert timestamp
parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# ---------------------------
# Output Function
# ---------------------------
def write_to_postgres_and_hdfs(batch_df, batch_id):

    batch_df.cache()

    # -----------------------
    # PostgreSQL sink
    # -----------------------
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airquality") \
        .option("dbtable", "sensor_data") \
        .option("user", "spark") \
        .option("password", "spark") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    # -----------------------
    # HDFS sink (Parquet)
    # -----------------------
    hdfs_path = "hdfs://namenode:9000/data/airquality"

    batch_df.write \
        .mode("append") \
        .format("parquet") \
        .save(hdfs_path)

    batch_df.unpersist()

# ---------------------------
# Streaming Query
# ---------------------------
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres_and_hdfs) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/airquality_pipeline") \
    .trigger(processingTime="5 seconds") \
    .outputMode("append") \
    .start()

query.awaitTermination()
