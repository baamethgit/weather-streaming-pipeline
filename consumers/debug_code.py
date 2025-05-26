from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, lit, coalesce, avg, window
from pyspark.sql.types import StructType, StringType, DoubleType, LongType


# 1. Démarrer SparkSession avec le connecteur Kafka et plus de logging
spark = SparkSession.builder \
    .appName("WeatherDataFusionJob_Debug") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# schémas 

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

# Lecture du flux API météo
df_api_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-api") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Lecture du flux capteurs IoT
df_local_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "capteur-iot") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

print("===== Configuration des flux Kafka terminée =====")


# Parser les données JSON pour chaque source
df_api = df_api_raw.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), api_schema).alias("data"), col("timestamp"))

df_api = df_api \
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
    )

df_local = df_local_raw.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), local_schema).alias("data"), col("timestamp"))

df_local = df_local \
    .select(
        to_timestamp(col("data.timestamp")).alias("event_time"),
        col("data.temperature"),
        col("data.humidity"),
        col("data.pressure"),
        lit(None).cast(DoubleType()).alias("wind_speed"),    # Champs non disponibles dans la source locale
        lit(None).cast(DoubleType()).alias("wind_deg"),      # mais ajoutés pour uniformiser la structure
        lit(None).cast(DoubleType()).alias("feels_like"),
        lit(None).cast(DoubleType()).alias("uvi"),
        lit(None).cast(StringType()).alias("weather_main"),
        lit(None).cast(StringType()).alias("weather_description"),
        lit("local").alias("source")
    )

# petis pretraitement

df_api_cleaned = df_api \
    .filter(col("temperature").isNotNull())\
    .filter(col("humidity").between(0, 100))

df_local_cleaned = df_local \
    .filter(col("temperature").isNotNull())\
    .filter(col("humidity").between(0, 100))

#affichage console pour tesT

api_parsed_debug = df_api_cleaned.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime='30 seconds') \
    .start()

local_parsed_debug = df_local_cleaned.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime='30 seconds') \
    .start()

# # 6. Extraction des données par tranches de 1 minute (au lieu de 10) pour le débogage
# df_api_1min = df_api_cleaned \
#     .withWatermark("event_time", "10 minutes") \
#     .groupBy(window(col("event_time"), "1 minute")) \
#     .agg(
#         avg("temperature").alias("temperature"),
#         avg("humidity").alias("humidity"),
#         avg("wind_speed").alias("wind_speed"),
#         avg("wind_deg").alias("wind_deg"),
#         avg("pressure").alias("pressure"),
#         avg("feels_like").alias("feels_like"),
#         avg("uvi").alias("uvi"),
#         lit("api").alias("source")
#     ) \
#     .select(
#         col("window.start").alias("interval_start"),
#         col("window.end").alias("interval_end"),
#         col("temperature"),
#         col("humidity"),
#         col("wind_speed"),
#         col("wind_deg"),
#         col("pressure"),
#         col("feels_like"),
#         col("uvi"),
#         col("source")
#     )

# df_local_1min = df_local_cleaned \
#     .withWatermark("event_time", "10 minutes") \
#     .groupBy(window(col("event_time"), "1 minute"))\
#     .agg(
#         avg("temperature").alias("temperature"),
#         avg("humidity").alias("humidity"),
#         avg("pressure").alias("pressure"),
#         lit("local").alias("source")
#     ) \
#     .select(
#         col("window.start").alias("interval_start"),
#         col("window.end").alias("interval_end"),
#         col("temperature"),
#         col("humidity"),
#         lit(None).cast(DoubleType()).alias("wind_speed"),
#         lit(None).cast(DoubleType()).alias("wind_deg"),
#         col("pressure"),
#         lit(None).cast(DoubleType()).alias("feels_like"),
#         lit(None).cast(DoubleType()).alias("uvi"),
#         col("source")
#     )

# # Ajouter un débogage des moyennes sur 1 minute
# api_1min_debug = df_api_1min.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .trigger(processingTime='10 seconds') \
#     .start()

# local_1min_debug = df_local_1min.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .trigger(processingTime='10 seconds') \
#     .start()

# print("===== Débogage des moyennes sur 1 minute configuré =====")

# # 7. Union des deux sources pour la fusion
# df_all_1min = df_api_1min.unionByName(df_local_1min)

# # 8. Fusion des données avec priorisation du local quand disponible
# df_fused = df_all_1min \
#     .withWatermark("interval_start", "15 minutes") \
#     .groupBy(col("interval_start"), col("interval_end")) \
#     .pivot("source", ["local", "api"]) \
#     .agg(
#         avg("temperature").alias("temp"),
#         avg("humidity").alias("humidity"),
#         avg("pressure").alias("pressure"),
#         avg("wind_speed").alias("wind_speed"),
#         avg("wind_deg").alias("wind_deg"),
#         avg("feels_like").alias("feels_like"),
#         avg("uvi").alias("uvi")
#     ) \
#     .select(
#         col("interval_start"),
#         col("interval_end"),
#         # Priorité aux données locales avec basculement automatique pour les données communes
#         coalesce(col("local_temp"), col("api_temp")).alias("temperature"),
#         coalesce(col("local_humidity"), col("api_humidity")).alias("humidity"),
#         coalesce(col("local_pressure"), col("api_pressure")).alias("pressure"),
#         # Données uniquement disponibles via l'API
#         col("api_wind_speed").alias("wind_speed"),
#         col("api_wind_deg").alias("wind_deg"),
#         col("api_feels_like").alias("feels_like"),
#         col("api_uvi").alias("uvi"),
#         # Indication des sources utilisées pour les données principales
#         when(col("local_temp").isNotNull(), "local")
#             .otherwise("api").alias("temp_source"),
#         when(col("local_humidity").isNotNull(), "local")
#             .otherwise("api").alias("humidity_source"),
#         when(col("local_pressure").isNotNull(), "local")
#             .otherwise("api").alias("pressure_source")
#     )

# # 14. Affichage console pour monitoring (obligatoire pour le débogage)
# query_monitor = df_fused.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime='10 seconds') \
#     .option("truncate", False) \
#     .start()

# print("===== Monitoring des données fusionnées démarré =====")

"""
# COMMENTÉ POUR LE DÉBOGAGE: 9. Calcul des moyennes horaires à partir des données fusionnées
df_hourly_avg = df_fused \
    .withWatermark("interval_start", "1 hour") \
    .groupBy(window(col("interval_start"), "1 hour")) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("pressure").alias("avg_pressure"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("wind_deg").alias("avg_wind_deg"),
        avg("feels_like").alias("avg_feels_like"),
        avg("uvi").alias("avg_uvi")
    ) \
    .select(
        col("window.start").alias("hour_start"),
        col("window.end").alias("hour_end"),
        col("avg_temp"),
        col("avg_humidity"),
        col("avg_pressure"),
        col("avg_wind_speed"),
        col("avg_wind_deg"),
        col("avg_feels_like"),
        col("avg_uvi")
    )

# COMMENTÉ POUR LE DÉBOGAGE: 10. Calcul des moyennes journalières
df_daily_avg = df_fused \
    .withWatermark("interval_start", "1 day") \
    .groupBy(window(col("interval_start"), "1 day")) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("pressure").alias("avg_pressure"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("wind_deg").alias("avg_wind_deg"),
        avg("feels_like").alias("avg_feels_like"),
        avg("uvi").alias("avg_uvi")
    ) \
    .select(
        col("window.start").alias("day_start"),
        col("window.end").alias("day_end"),
        col("avg_temp"),
        col("avg_humidity"),
        col("avg_pressure"),
        col("avg_wind_speed"),
        col("avg_wind_deg"),
        col("avg_feels_like"),
        col("avg_uvi")
    )

# COMMENTÉ POUR LE DÉBOGAGE: 11. Écriture des données fusionnées (1 minute) au format CSV
query_fused = df_fused.writeStream \
    .format("csv") \
    .option("path", "./resultats/weather_fused_1min/") \
    .option("checkpointLocation", "./checkpoints/weather_fused_1min/") \
    .option("header", "true") \
    .outputMode("append") \
    .start()

# COMMENTÉ POUR LE DÉBOGAGE: 12. Écriture des moyennes horaires au format CSV
query_hourly = df_hourly_avg.writeStream \
    .format("csv") \
    .option("path", "./resultats/weather_hourly/") \
    .option("checkpointLocation", "./checkpoints/weather_hourly/") \
    .option("header", "true") \
    .outputMode("append") \
    .start()

# COMMENTÉ POUR LE DÉBOGAGE: 13. Écriture des moyennes journalières au format CSV
query_daily = df_daily_avg.writeStream \
    .format("csv") \
    .option("path", "./resultats/weather_daily/") \
    .option("checkpointLocation", "./checkpoints/weather_daily/") \
    .option("header", "true") \
    .outputMode("append") \
    .start()
"""

print("===== Attente des données... =====")
spark.streams.awaitAnyTermination()