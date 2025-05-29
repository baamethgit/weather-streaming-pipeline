from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from pyspark.sql.functions import to_timestamp

from pyspark.sql.functions import window, avg

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, lit, coalesce, avg, window
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# 1. Démarrer SparkSession avec le connecteur Kafka
# spark = SparkSession.builder \
#     .appName("WeatherDataFusionJob") \
#     .config("spark.jars.packages", 
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
#             "org.apache.hadoop:hadoop-aws:3.3.4,"
#             "com.amazonaws:aws-java-sdk-bundle:1.12.367") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
#             "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
#     .config("spark.hadoop.fs.s3a.profile.name", "comptePerso") \
#     .config("spark.hadoop.fs.s3a.credentials.file", "C:/Users/DELL/.aws/credentials") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("WeatherAggregationPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Définir les schémas corrects pour les deux sources
# Schéma complet pour l'API météo
api_schema = StructType() \
    .add("timestamp", StringType()) \
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

# Schéma pour les capteurs IoT locaux (structure simplifiée)
local_schema = StructType() \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("pressure", DoubleType()) \
    .add("timestamp", StringType())

# 3. Lire depuis les deux topics Kafka
# Lecture du flux API météo
df_api_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-api") \
    .option("startingOffsets", "latest") \
    .load()

# Lecture du flux capteurs IoT
df_local_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "capteur-iot") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parser les données JSON pour chaque source
df_api = df_api_raw.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), api_schema).alias("data"), col("timestamp")) \
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
    .select(from_json(col("value"), local_schema).alias("data"), col("timestamp")) \
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

# 5. Prétraitement : standardisation et nettoyage des données
# Nettoyage des données API
df_api_cleaned = df_api \
    .filter(col("temperature").isNotNull()) \
    .filter(col("humidity").between(0, 100))

# Nettoyage des données locales
df_local_cleaned = df_local \
    .filter(col("temperature").isNotNull()) \
    .filter(col("humidity").between(0, 100))

# 6. Extraction des données par tranches de 10 minutes
df_api_10min = df_api_cleaned \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(window(col("event_time"), "10 minutes")) \
    .agg(
        avg("temperature").alias("temperature"),
        avg("humidity").alias("humidity"),
        avg("wind_speed").alias("wind_speed"),
        avg("wind_deg").alias("wind_deg"),
        avg("pressure").alias("pressure"),
        avg("feels_like").alias("feels_like"),
        avg("uvi").alias("uvi"),
        lit("api").alias("source")
    ) \
    .select(
        col("window.start").alias("interval_start"),
        col("window.end").alias("interval_end"),
        col("temperature"),
        col("humidity"),
        col("wind_speed"),
        col("wind_deg"),
        col("pressure"),
        col("feels_like"),
        col("uvi"),
        col("source")
    )

df_local_10min = df_local_cleaned \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(window(col("event_time"), "10 minutes")) \
    .agg(
        avg("temperature").alias("temperature"),
        avg("humidity").alias("humidity"),
        avg("pressure").alias("pressure"),
        lit("local").alias("source")
    ) \
    .select(
        col("window.start").alias("interval_start"),
        col("window.end").alias("interval_end"),
        col("temperature"),
        col("humidity"),
        lit(None).cast(DoubleType()).alias("wind_speed"),
        lit(None).cast(DoubleType()).alias("wind_deg"),
        col("pressure"),
        lit(None).cast(DoubleType()).alias("feels_like"),
        lit(None).cast(DoubleType()).alias("uvi"),
        col("source")
    )

# 7. Union des deux sources pour la fusion
df_all_10min = df_api_10min.unionByName(df_local_10min)

# 8. Fusion des données avec priorisation du local quand disponible
df_fused_raw = df_all_10min \
    .withWatermark("interval_start", "15 minutes") \
    .groupBy(col("interval_start"), col("interval_end")) \
    .pivot("source", ["local", "api"]) \
    .agg(
        avg("temperature").alias("temp"),
        avg("humidity").alias("humidity"),
        avg("pressure").alias("pressure"),
        avg("wind_speed").alias("wind_speed"),
        avg("wind_deg").alias("wind_deg"),
        avg("feels_like").alias("feels_like"),
        avg("uvi").alias("uvi")
    )

# 9. Sélection finale - SEULEMENT les colonnes business (pas de source)
df_fused = df_fused_raw.select(
    col("interval_start"),
    col("interval_end"),
    # Fusion intelligente mais résultat propre
    coalesce(col("local_temp"), col("api_temp")).alias("temperature"),
    coalesce(col("local_humidity"), col("api_humidity")).alias("humidity"),
    coalesce(col("local_pressure"), col("api_pressure")).alias("pressure"),
    col("api_wind_speed").alias("wind_speed"),
    col("api_wind_deg").alias("wind_deg"),
    col("api_feels_like").alias("feels_like"),
    col("api_uvi").alias("uvi")
    # PAS de colonne source dans le résultat final
)

# 10. Calcul des moyennes horaires à partir des données fusionnées
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

# 11. Calcul des moyennes journalières
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

# 12. Sauvegarde S3 format Parquet (optimal pour dashboard)
query_fused_s3 = df_fused.writeStream \
    .format("parquet") \
    .option("path", "s3a://weatherdata-bucket-00132345/weather-data/fused-10min/") \
    .option("checkpointLocation", "s3a://weatherdata-bucket-00132345/checkpoints/weather-fused-10min/") \
    .outputMode("append") \
    .trigger(processingTime='10 minutes') \
    .start()

query_hourly_s3 = df_hourly_avg.writeStream \
    .format("parquet") \
    .option("path", "s3a://weatherdata-bucket-00132345/weather-data/hourly/") \
    .option("checkpointLocation", "s3a://weatherdata-bucket-00132345/checkpoints/weather-hourly/") \
    .outputMode("append") \
    .trigger(processingTime='1 hour') \
    .start()

query_daily_s3 = df_daily_avg.writeStream \
    .format("parquet") \
    .option("path", "s3a://weatherdata-bucket-00132345/weather-data/daily/") \
    .option("checkpointLocation", "s3a://weatherdata-bucket-00132345/checkpoints/weather-daily/") \
    .outputMode("append") \
    .trigger(processingTime='6 hours') \
    .start()

# 13. Affichage console pour monitoring (optionnel)
query_monitor = df_fused.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='5 minutes') \
    .option("truncate", False) \
    .start()

# Attendre la fin des requêtes
spark.streams.awaitAnyTermination()