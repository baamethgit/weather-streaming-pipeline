from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit, avg, min, max, count,
    window, date_format, hour, dayofyear, year
)
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# Configuration Spark
spark = SparkSession.builder \
    .appName("WeatherAggregationPipeline") \
    .config("spark.jars.packages", 
           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
           "org.postgresql:postgresql:42.5.0") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Schémas (identiques aux vôtres)
api_schema = StructType() \
    .add("timestamp", LongType()) \
    .add("datetime", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("wind_speed", DoubleType()) \
    .add("wind_deg", DoubleType()) \
    .add("pressure", DoubleType()) \
    .add("feels_like", DoubleType()) \
    .add("uvi", DoubleType()) \
    .add("weather_main", StringType()) \
    .add("weather_description", StringType())

local_schema = StructType() \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("pressure", DoubleType()) \
    .add("timestamp", StringType())

# Lecture des flux
df_api_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-api") \
    .option("startingOffsets", "latest") \
    .load()

df_local_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "capteur-iot") \
    .option("startingOffsets", "latest") \
    .load()

# Parsing et nettoyage (identique à votre code)
df_api = df_api_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), api_schema).alias("data")) \
    .select(
        col("data.timestamp"),
        to_timestamp(col("data.datetime")).alias("event_time"),
        col("data.temperature"),
        col("data.humidity"),
        col("data.wind_speed"),
        col("data.wind_deg"),
        col("data.pressure"),
        col("data.feels_like"),
        col("data.uvi"),
        col("data.weather_main"),
        col("data.weather_description"),
        lit("api").alias("source")
    ) \
    .filter(col("temperature").isNotNull()) \
    .filter(col("humidity").between(0, 100)) \
    .dropDuplicates(["timestamp", "source"])

df_local = df_local_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), local_schema).alias("data")) \
    .select(
        to_timestamp(col("data.timestamp")).alias("event_time"),
        col("data.temperature"),
        col("data.humidity"),
        col("data.pressure"),
        lit(None).cast(DoubleType()).alias("wind_speed"),
        lit(None).cast(DoubleType()).alias("wind_deg"),
        lit(None).cast(DoubleType()).alias("feels_like"),
        lit(None).cast(DoubleType()).alias("uvi"),
        lit(None).cast(StringType()).alias("weather_main"),
        lit(None).cast(StringType()).alias("weather_description"),
        lit("local").alias("source")
    ) \
    .filter(col("temperature").isNotNull()) \
    .filter(col("humidity").between(0, 100))

# Union des deux sources
df_unified = df_api.unionByName(df_local)

# ==============================================
# AGRÉGATIONS TEMPORELLES
# ==============================================

# 1. Données brutes toutes les 10 minutes
df_raw_10min = df_unified \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("source")
    ) \
    .agg(
        avg("temperature").alias("temperature"),
        avg("humidity").alias("humidity"),
        avg("wind_speed").alias("wind_speed"),
        avg("wind_deg").alias("wind_deg"),
        avg("pressure").alias("pressure"),
        avg("feels_like").alias("feels_like"),
        avg("uvi").alias("uvi"),
        count("*").alias("record_count")
    ) \
    .select(
        col("window.start").alias("timestamp"),
        col("source"),
        col("temperature"),
        col("humidity"),
        col("wind_speed"),
        col("wind_deg"),
        col("pressure"),
        col("feels_like"),
        col("uvi"),
        col("record_count"),
        lit("10min").alias("aggregation_level")
    )

# 2. Moyennes horaires
df_hourly = df_unified \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("source")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("pressure").alias("avg_pressure"),
        avg("feels_like").alias("avg_feels_like"),
        count("*").alias("record_count")
    ) \
    .select(
        col("window.start").alias("timestamp"),
        hour(col("window.start")).alias("hour"),
        col("source"),
        col("avg_temperature"),
        col("min_temperature"),
        col("max_temperature"),
        col("avg_humidity"),
        col("avg_wind_speed"),
        col("avg_pressure"),
        col("avg_feels_like"),
        col("record_count"),
        lit("hourly").alias("aggregation_level")
    )

# 3. Moyennes journalières
df_daily = df_unified \
    .withWatermark("event_time", "1 hour") \
    .groupBy(
        window(col("event_time"), "1 day"),
        col("source")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("pressure").alias("avg_pressure"),
        count("*").alias("record_count")
    ) \
    .select(
        col("window.start").alias("timestamp"),
        date_format(col("window.start"), "yyyy-MM-dd").alias("date"),
        dayofyear(col("window.start")).alias("day_of_year"),
        col("source"),
        col("avg_temperature"),
        col("min_temperature"),
        col("max_temperature"),
        col("avg_humidity"),
        col("avg_wind_speed"),
        col("avg_pressure"),
        col("record_count"),
        lit("daily").alias("aggregation_level")
    )

# ==============================================
# FONCTIONS DE SAUVEGARDE
# ==============================================

def save_to_postgres(df, table_name, mode="append"):
    """Sauvegarde vers PostgreSQL"""
    return df.writeStream \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/weather_db") \
        .option("dbtable", table_name) \
        .option("user", "your_user") \
        .option("password", "your_password") \
        .option("driver", "org.postgresql.Driver") \
        .outputMode(mode) \
        .option("checkpointLocation", f"/tmp/checkpoints/{table_name}") \
        .trigger(processingTime='2 minutes') \
        .start()

def save_to_s3_parquet(df, s3_path, partition_cols=None):
    """Sauvegarde vers S3 en format Parquet"""
    writer = df.writeStream \
        .format("parquet") \
        .option("path", s3_path) \
        .option("checkpointLocation", f"/tmp/checkpoints/{s3_path.split('/')[-1]}") \
        .outputMode("append") \
        .trigger(processingTime='2 minutes')
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    return writer.start()

# ==============================================
# DÉMARRAGE DES STREAMS DE SAUVEGARDE
# ==============================================

# Option 1: PostgreSQL
query_raw = save_to_postgres(df_raw_10min, "weather_raw_10min")
query_hourly = save_to_postgres(df_hourly, "weather_hourly")
query_daily = save_to_postgres(df_daily, "weather_daily")

# Option 2: S3 (Recommandé)
# query_raw = save_to_s3_parquet(
#     df_raw_10min, 
#     "s3a://your-bucket/weather-data/raw/",
#     ["source", "aggregation_level"]
# )

# query_hourly = save_to_s3_parquet(
#     df_hourly, 
#     "s3a://your-bucket/weather-data/hourly/",
#     ["source", "hour"]
# )

# query_daily = save_to_s3_parquet(
#     df_daily, 
#     "s3a://your-bucket/weather-data/daily/",
#     ["source", "date"]
# )

# Debug en console (optionnel)
debug_query = df_hourly.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime='1 minute') \
    .start()

print("=== Pipeline d'agrégation démarré ===")
print("Données sauvegardées :")
print("- Brutes (10min) -> weather-data/raw/")
print("- Horaires -> weather-data/hourly/")  
print("- Journalières -> weather-data/daily/")

# Attendre la fin des streams
spark.streams.awaitAnyTermination()