from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from pyspark.sql.functions import to_timestamp

from pyspark.sql.functions import window, avg

# connecteur Kafka
spark = SparkSession.builder \
    .appName("KafkaSparkStructuredConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# lire le topic Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-api") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Définir le schéma des données envoyées

schema = StructType() \
    .add("timestamp", LongType()) \
    .add("datetime", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("wind_speed", DoubleType()) \
    .add("wind_deg", DoubleType()) \
    .add("pressure", DoubleType())


df_json = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.timestamp"),
        to_timestamp(col("data.timestamp")).alias("event_time"),
        "data.temperature",
        "data.humidity",
        "data.wind_speed",
        "data.wind_deg",
        "data.pressure"
    )


df_json.writeStream \
    .format("parquet") \
    .option("path", "./resultats/weather_raw/") \
    .option("checkpointLocation", "./checkpoints/weather_raw/") \
    .outputMode("append") \
    .start()

from pyspark.sql.functions import window, avg

df_hourly_avg = df_json \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 hour")) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed")
    )

df_hourly_avg.writeStream \
    .format("parquet") \
    .option("path", "./resultats/weather_hourly/") \
    .option("checkpointLocation", "./checkpoints/weather_hourly/") \
    .outputMode("append") \
    .start()

df_daily_avg = df_json \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "1 day")) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    )

df_daily_avg.writeStream \
    .format("parquet") \
    .option("path", "./resultats/weather_daily/") \
    .option("checkpointLocation", "./checkpoints/weather_daily/") \
    .outputMode("append") \
    .start()


# 5. Affichage console des données brutes
query = df_json.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='15 seconds') \
    .option("truncate", False) \
    .start()

query.awaitTermination()
