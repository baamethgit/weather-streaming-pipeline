# ============ DÉBUT DU FICHIER - Configuration Windows ============
import sys
import os
import shutil

# Configuration critique Windows AVANT tout import Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit, avg, min, max, count,
    window, date_format, hour, dayofyear, year
)
from pyspark.sql.functions import when, coalesce, first
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from pyspark.sql.functions import to_date
from predictions_model.predictions import generate_predictions

# ============ LOGGING SETUP ============
import logging

def setup_logging():
    """Configuration du système de logging sans emojis"""
    logger = logging.getLogger('weather_pipeline')
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        logger.handlers.clear()
    
    # Créer les répertoires
    os.makedirs("./logs", exist_ok=True)
    
    # Handler pour fichier avec encoding UTF-8
    file_handler = logging.FileHandler('./logs/weather_pipeline.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Handler pour console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Format sans emojis
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# ============ SPARK SESSION ============
os.makedirs("./checkpoints", exist_ok=True)

# Configuration Spark avec chemins Python corrects
spark = SparkSession.builder \
    .appName("WeatherAggregationPipeline") \
    .config("spark.jars.packages", 
           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
           "org.postgresql:postgresql:42.5.0") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.pyspark.python", sys.executable) \
    .config("spark.pyspark.driver.python", sys.executable) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
spark.sparkContext.setLogLevel("ERROR")



# ============ SCHÉMAS (identiques) ============
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

# ============ LECTURE DES FLUX ============
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

# ============ PARSING ET NETTOYAGE ============
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
        lit(None).cast(LongType()).alias("timestamp"),
        lit(None).cast(DoubleType()).alias("wind_speed"),
        lit(None).cast(DoubleType()).alias("wind_deg"),
        lit(None).cast(DoubleType()).alias("feels_like"),
        lit(None).cast(StringType()).alias("weather_main"),
        lit(None).cast(StringType()).alias("weather_description"),
        lit("local").alias("source")
    ) \
    .filter(col("temperature").isNotNull()) \
    .filter(col("humidity").between(0, 100))

# ============ STANDARDISATION ============
df_api_std = df_api.select(
    col("event_time"),
    col("temperature"),
    col("humidity"), 
    col("pressure"),
    col("wind_speed"),
    col("wind_deg"),
    col("feels_like"),
    col("weather_main"),
    col("weather_description"),
    lit("api").alias("source")
)

df_local_std = df_local.select(
    col("event_time"),
    col("temperature"),
    col("humidity"),
    col("pressure"),
    lit(None).cast("double").alias("wind_speed"),
    lit(None).cast("double").alias("wind_deg"),
    lit(None).cast("double").alias("feels_like"),
    lit(None).cast("string").alias("weather_main"),
    lit(None).cast("string").alias("weather_description"),
    lit("local").alias("source")
)

# ============ UNION ET AGRÉGATIONS ============
df_unified = df_api_std.union(df_local_std)

# Agrégation 10 minutes (votre code existant)
df_raw_10min = df_unified \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "10 minutes")) \
    .agg(
        coalesce(
            avg(when(col("source") == "local", col("temperature"))),
            avg(when(col("source") == "api", col("temperature")))
        ).alias("temperature"),
        coalesce(
            avg(when(col("source") == "local", col("humidity"))),
            avg(when(col("source") == "api", col("humidity")))
        ).alias("humidity"),
        coalesce(
            avg(when(col("source") == "local", col("pressure"))),
            avg(when(col("source") == "api", col("pressure")))
        ).alias("pressure"),
        avg(when(col("source") == "api", col("wind_speed"))).alias("wind_speed"),
        avg(when(col("source") == "api", col("wind_deg"))).alias("wind_deg"),
        avg(when(col("source") == "api", col("feels_like"))).alias("feels_like"),
        first(when(col("source") == "api", col("weather_main"))).alias("weather_main"),
        first(when(col("source") == "api", col("weather_description"))).alias("weather_description")
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

# Agrégations horaires et journalières (votre code existant)
df_hourly = df_raw_10min \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "1 hour")) \
    .agg(
        avg("temperature").alias("temperature_avg"),
        min("temperature").alias("temperature_min"),
        max("temperature").alias("temperature_max"),
        avg("humidity").alias("humidity_avg"),
        avg("pressure").alias("pressure_avg"),
        avg("wind_speed").alias("wind_speed_avg"),
        avg("feels_like").alias("feels_like_avg"),
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

df_daily = df_raw_10min \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(window(col("timestamp"), "1 day")) \
    .agg(
        avg("temperature").alias("temperature_avg"),
        min("temperature").alias("temperature_min"),
        max("temperature").alias("temperature_max"),
        avg("humidity").alias("humidity_avg"),
        avg("pressure").alias("pressure_avg"),
        avg("wind_speed").alias("wind_speed_avg"),
        avg("feels_like").alias("feels_like_avg"),
        first("weather_main", ignorenulls=True).alias("weather_main")
    ) \
    .select(
        col("window.start").cast("date").alias("timestamp"),
        col("temperature_avg"),
        col("temperature_min"),
        col("temperature_max"),
        col("humidity_avg"),
        col("pressure_avg"),
        col("wind_speed_avg"),
        col("feels_like_avg"),
        col("weather_main")
    )

# # ============ INTÉGRATION DU SYSTÈME D'ALERTES (CORRIGÉ) ============
# monitoring_system = setup_monitoring_for_pipeline(
#     spark, logger, df_api, df_local, df_unified
# )

# alert_manager = monitoring_system["alert_manager"]

# Alerte de démarrage
# alert_manager.create_alert(
#     AlertType.SYSTEM_HEALTH,
#     AlertSeverity.LOW,
#     "Pipeline météo démarré avec succès",
#     "system",
#     {"version": "1.0", "spark_version": spark.version}
# )

logger.info("Pipeline météo démarré avec surveillance complète")

# ============ FONCTION SAUVEGARDE SIMPLE ============
def save_to_postgres(df, table_name, mode="append"):
    """Sauvegarde vers PostgreSQL avec foreachBatch"""
    def write_to_postgres(batch_df, batch_id):
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/weather_db") \
                .option("dbtable", table_name) \
                .option("user", "postgres") \
                .option("password", "postgreSQL") \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()
            logger.info(f"Batch {batch_id} sauvegardé dans {table_name}")
        except Exception as e:
            logger.error(f"Erreur sauvegarde {table_name}: {str(e)}")
    
    return df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", f"./checkpoints/{table_name}") \
        .trigger(processingTime='2 minutes') \
        .start()

# ============ STREAMS DE SAUVEGARDE ============
query_raw = save_to_postgres(df_raw_10min, "weather_raw_10min")
query_hourly = save_to_postgres(df_hourly, "weather_hourly")
query_daily = save_to_postgres(df_daily, "weather_daily")

# Debug en console
debug_query = df_raw_10min.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime='1 minute') \
    .start()

# Prédictions
prediction_stream = df_hourly.writeStream \
    .foreachBatch(generate_predictions) \
    .option("checkpointLocation", "./checkpoints/predictions") \
    .trigger(processingTime='1 hour') \
    .start()

# ============ EXÉCUTION PRINCIPALE ============
try:
    logger.info("Démarrage des streams, appuyez sur Ctrl+C pour arrêter...")
    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:
    logger.info("Arrêt demandé par l'utilisateur")
    alert_manager.create_alert(
        AlertType.SYSTEM_HEALTH,
        AlertSeverity.LOW,
        "Pipeline arrêté par l'utilisateur",
        "system"
    )

except Exception as e:
    logger.error(f"Erreur critique du pipeline: {str(e)}")
    alert_manager.create_alert(
        AlertType.PROCESSING_ERROR,
        AlertSeverity.CRITICAL,
        f"Erreur critique pipeline: {str(e)}",
        "system",
        {"error": str(e)}
    )
    raise

finally:
    logger.info("Nettoyage et fermeture du pipeline")
    spark.stop()