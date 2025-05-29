from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit, avg, min, max, count,
    window, date_format, hour, dayofyear, year
)
from pyspark.sql.functions import when, coalesce,first

from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from pyspark.sql.functions import to_date

# Configuration Spark
spark = SparkSession.builder \
    .appName("WeatherAggregationPipeline") \
    .config("spark.jars.packages", 
           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
           "org.postgresql:postgresql:42.5.0") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
# Réduit le niveau de log global à ERROR
spark.sparkContext.setLogLevel("ERROR")


# logging.getLogger("py4j").setLevel(logging.ERROR)

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
    .option("startingOffsets", "earliest") \
    .load()
df_local_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "capteur-iot") \
    .option("startingOffsets", "earliest") \
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
        lit(None).cast(LongType()).alias("timestamp"),    # ← AJOUTEZ CETTE LIGNE
        lit(None).cast(DoubleType()).alias("wind_speed"),
        lit(None).cast(DoubleType()).alias("wind_deg"),
        lit(None).cast(DoubleType()).alias("feels_like"),
        lit(None).cast(StringType()).alias("weather_main"),
        lit(None).cast(StringType()).alias("weather_description"),
        lit("local").alias("source")
    ) \
    .filter(col("temperature").isNotNull()) \
    .filter(col("humidity").between(0, 100))

df_api_std = df_api.select(
    col("event_time"),
    col("temperature"),
    col("humidity"), 
    col("pressure"),
    col("wind_speed"),
    col("wind_deg"),
    col("feels_like"),
    col("weather_main"),           # ← AJOUTÉ
    col("weather_description"),    # ← AJOUTÉ
    lit("api").alias("source")
)

df_local_std = df_local.select(
    col("event_time"),
    col("temperature"),
    col("humidity"),
    col("pressure"),
    lit(None).cast("double").alias("wind_speed"),    # Local n'a pas ces données
    lit(None).cast("double").alias("wind_deg"),      # Local n'a pas ces données  
    lit(None).cast("double").alias("feels_like"),    # Local n'a pas ces données
    lit(None).cast("string").alias("weather_main"),           # ← AJOUTÉ
    lit(None).cast("string").alias("weather_description"),    # ← AJOUTÉ
    lit("local").alias("source")
)

# 2. Union des deux sources
df_unified = df_api_std.union(df_local_std)

# 3. Agrégation avec logique de priorité simple et correcte
df_raw_10min = df_unified \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "10 minutes")) \
    .agg(
        # Température : local en priorité, sinon API
        coalesce(
            avg(when(col("source") == "local", col("temperature"))),
            avg(when(col("source") == "api", col("temperature")))
        ).alias("temperature"),
        
        # Humidité : local en priorité, sinon API
        coalesce(
            avg(when(col("source") == "local", col("humidity"))),
            avg(when(col("source") == "api", col("humidity")))
        ).alias("humidity"),
        
        # Pression : local en priorité, sinon API
        coalesce(
            avg(when(col("source") == "local", col("pressure"))),
            avg(when(col("source") == "api", col("pressure")))
        ).alias("pressure"),
        
        # Wind et feels_like : toujours depuis API
        avg(when(col("source") == "api", col("wind_speed"))).alias("wind_speed"),
        avg(when(col("source") == "api", col("wind_deg"))).alias("wind_deg"),
        avg(when(col("source") == "api", col("feels_like"))).alias("feels_like"),
        
        # Weather : toujours depuis API                                        # ← AJOUTÉ
        first(when(col("source") == "api", col("weather_main"))).alias("weather_main"),           # ← AJOUTÉ
        first(when(col("source") == "api", col("weather_description"))).alias("weather_description")  # ← AJOUTÉ
    ) \
    .select(
        col("window.start").alias("timestamp"),
        col("temperature"),
        col("humidity"), 
        col("pressure"),
        col("wind_speed"),
        col("wind_deg"),
        col("feels_like"),
        col("weather_main"),
        col("weather_description")
    )


# =========================================
# moyennes journaliere,par heure
# ===============================

df_hourly = df_raw_10min \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "1 hour")) \
    .agg(
        # Température : moyenne, min, max
        avg("temperature").alias("temperature_avg"),
        min("temperature").alias("temperature_min"),
        max("temperature").alias("temperature_max"),
        
        # Autres moyennes
        avg("humidity").alias("humidity_avg"),
        avg("pressure").alias("pressure_avg"),
        avg("wind_speed").alias("wind_speed_avg"),
        avg("feels_like").alias("feels_like_avg"),
        
        # Weather_main : le plus fréquent
        first("weather_main", ignorenulls=True).alias("weather_main")
    ) \
    .select(
        col("window.start").alias("timestamp"),
        col("temperature_avg"),
        col("temperature_min"),
        col("temperature_max"),
        col("humidity_avg"),
        col("pressure_avg"),
        col("wind_speed_avg"),
        col("feels_like_avg"),
        col("weather_main")
    )

# =============================================
# MOYENNES JOURNALIÈRES
# =============================================

df_daily = df_raw_10min \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(window(col("timestamp"), "1 day")) \
    .agg(
        # Température : moyenne, min, max
        avg("temperature").alias("temperature_avg"),
        min("temperature").alias("temperature_min"),
        max("temperature").alias("temperature_max"),
        
        # Autres moyennes
        avg("humidity").alias("humidity_avg"),
        avg("pressure").alias("pressure_avg"),
        avg("wind_speed").alias("wind_speed_avg"),
        avg("feels_like").alias("feels_like_avg"),
        
        # Weather_main : le plus fréquent
        first("weather_main", ignorenulls=True).alias("weather_main")
    ) \
    .select(
        col("window.start").cast("date").alias("timestamp"),  # DATE seulement
        col("temperature_avg"),
        col("temperature_min"),
        col("temperature_max"),
        col("humidity_avg"),
        col("pressure_avg"),
        col("wind_speed_avg"),
        col("feels_like_avg"),
        col("weather_main")
    )

def save_to_postgres(df, table_name, mode="append"):
    """Sauvegarde vers PostgreSQL avec foreachBatch"""
    def write_to_postgres(batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/weather_db") \
            .option("dbtable", table_name) \
            .option("user", "postgres") \
            .option("password", "postgreSQL") \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()
        print(f"Batch {batch_id} sauvegardé dans {table_name}")
    
    return df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", f"./checkpoints/{table_name}") \
        .trigger(processingTime='2 minutes') \
        .start()

# def save_to_s3_parquet(df, s3_path, partition_cols=None):
#     """Sauvegarde vers S3 en format Parquet"""
#     writer = df.writeStream \
#         .format("parquet") \
#         .option("path", s3_path) \
#         .option("checkpointLocation", f"/tmp/checkpoints/{s3_path.split('/')[-1]}") \
#         .outputMode("append") \
#         .trigger(processingTime='2 minutes')
    
#     if partition_cols:
#         writer = writer.partitionBy(*partition_cols)
    
#     return writer.start()


# ==============================================
# STREAMS DE SAUVEGARDE
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
debug_query = df_raw_10min.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime='1 minute') \
    .start()

debug_hourly = df_hourly.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime='5 minutes') \
    .start()

debug_daily = df_daily.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime='1 hour') \
    .start()

# Attendre la fin des streams
spark.streams.awaitAnyTermination()


