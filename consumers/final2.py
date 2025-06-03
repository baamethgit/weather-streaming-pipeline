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
    """Configuration du système de logging amélioré"""
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
    
    # Format détaillé pour surveillance
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

# ============ SYSTÈME DE SURVEILLANCE SIMPLIFIÉ ============
import json
from datetime import datetime, timedelta
from enum import Enum
import psycopg2
import threading
import time

class AlertType(Enum):
    DATA_QUALITY = "data_quality"
    SYSTEM_HEALTH = "system_health" 
    PROCESSING_ERROR = "processing_error"

class AlertSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertManager:
    def __init__(self, logger):
        self.logger = logger
        self.last_data_received = {}
        self._create_alerts_table()
        
    def _clean_error_message(self, error_str):
        """Nettoyer les messages d'erreur - garder seulement l'essentiel"""
        error_str = str(error_str)
        
        # Messages spécifiques courants
        if "SparkContext was shut down" in error_str:
            return "Arrêt inattendu de Spark"
        if "Connection refused" in error_str:
            return "Connexion refusée"
        if "timeout" in error_str.lower():
            return "Timeout de connexion"
        if "does not exist" in error_str:
            return "Ressource introuvable"
        
        # Prendre seulement la première ligne d'erreur
        first_line = error_str.split('\n')[0].strip()
        
        # Limiter à 100 caractères
        if len(first_line) > 100:
            return first_line[:97] + "..."
        
        return first_line
    def create_alert(self, alert_type: AlertType, severity: AlertSeverity, 
                    message: str, component: str, metadata: dict = None):
        """Créer une alerte - LOG + DB + CONSOLE"""
        
        # 1. Logger l'alerte avec niveau approprié
        log_levels = {
            AlertSeverity.LOW: logging.INFO,
            AlertSeverity.MEDIUM: logging.WARNING, 
            AlertSeverity.HIGH: logging.ERROR,
            AlertSeverity.CRITICAL: logging.CRITICAL
        }
        
        alert_msg = f" [{alert_type.value.upper()}] {message} | {component}"
        if metadata:
            alert_msg += f" | {json.dumps(metadata)}"
            
        self.logger.log(log_levels[severity], alert_msg)
        
        # 2. Sauvegarde en DB pour le dashboard
        try:
            self._save_alert_to_db(alert_type.value, severity.value, message, component, metadata)
        except Exception as e:
            self.logger.error(f"Erreur sauvegarde alerte DB: {e}")
        
        # 3. Notification immédiate pour alertes critiques
        if severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]:
            print(f"\nALERTE {severity.value.upper()}: {message} [{component}]")
    
    def _save_alert_to_db(self, alert_type, severity, message, component, metadata):
        """Sauvegarder l'alerte en DB"""
        try:
            conn = psycopg2.connect(
                host="localhost", port=5432, dbname="weather_db",
                user="postgres", password="postgreSQL"
            )
            
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO weather_alerts 
                    (alert_type, severity, message, component, metadata, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (alert_type, severity, message, component, 
                     json.dumps(metadata) if metadata else None, datetime.now()))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Erreur sauvegarde alerte: {e}")
    
    def _create_alerts_table(self):
        """Créer la table des alertes si elle n'existe pas"""
        try:
            conn = psycopg2.connect(
                host="localhost", port=5432, dbname="weather_db",
                user="postgres", password="postgreSQL"
            )
            
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather_alerts (
                        id SERIAL PRIMARY KEY,
                        alert_type VARCHAR(50),
                        severity VARCHAR(20),
                        message TEXT,
                        component VARCHAR(50),
                        metadata JSONB,
                        created_at TIMESTAMP,
                        resolved BOOLEAN DEFAULT FALSE
                    )
                """)
            
            conn.commit()
            conn.close()
            logger.info("Table weather_alerts créée/vérifiée")
            
        except Exception as e:
            logger.error(f"Erreur création table alerts: {e}")
    
    def check_data_delay(self, source: str, threshold_minutes: int = 15):
        """Vérifier si les données sont en retard"""
        now = datetime.now()
        last_time = self.last_data_received.get(source)
        
        if last_time:
            delay_minutes = (now - last_time).total_seconds() / 60
            
            if delay_minutes > threshold_minutes:
                severity = AlertSeverity.CRITICAL if delay_minutes > 30 else AlertSeverity.HIGH
                
                self.create_alert(
                    AlertType.DATA_QUALITY,
                    severity,
                    f"Données {source} en retard de {delay_minutes:.1f} minutes",
                    source,
                    {"delay_minutes": delay_minutes}
                )
                return False
        
        return True
    
    def validate_data_batch(self, batch_df, source: str, batch_id: int):
        """Validation rapide d'un batch"""
        try:
            count = batch_df.count()
            
            if count == 0:
                self.create_alert(
                    AlertType.DATA_QUALITY,
                    AlertSeverity.MEDIUM,
                    f"Batch {batch_id} vide reçu de {source}",
                    source
                )
                return
            
            # Marquer réception des données
            self.last_data_received[source] = datetime.now()
            
            # Validation simple température
            if "temperature" in batch_df.columns:
                temp_rows = batch_df.select("temperature").filter(
                    (batch_df.temperature < -40) | (batch_df.temperature > 50)
                ).count()
                
                if temp_rows > 0:
                    self.create_alert(
                        AlertType.DATA_QUALITY,
                        AlertSeverity.HIGH,
                        f"{temp_rows} températures aberrantes dans batch {batch_id} ({source})",
                        source,
                        {"aberrant_count": temp_rows, "total_count": count, "batch_id": batch_id}
                    )
            
            # Log succès
            logger.info(f" Batch {batch_id} ({source}) validé: {count} lignes")
            
        except Exception as e:
            self.create_alert(
                AlertType.PROCESSING_ERROR,
                AlertSeverity.CRITICAL,
                f"Erreur validation batch {batch_id} ({source}): {self._clean_error_message(e)}",
                source,
                {"error": str(e), "batch_id": batch_id}
            )

# ============ INITIALISATION DU SYSTÈME D'ALERTES ============
alert_manager = AlertManager(logger)

# Alerte de démarrage
alert_manager.create_alert(
    AlertType.SYSTEM_HEALTH,
    AlertSeverity.LOW,
    "Pipeline météo démarré avec succès",
    "system",
    {"version": "1.0", "spark_version": spark.version}
)

logger.info("Pipeline météo démarré avec surveillance complète")

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

# Agrégation 10 minutes
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

# Agrégations horaires et journalières
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

# ============ FONCTION SAUVEGARDE AVEC SURVEILLANCE ============
def save_to_postgres_monitored(df, table_name, source_name, mode="append"):
    """Sauvegarde vers PostgreSQL avec surveillance intégrée"""
    def write_to_postgres(batch_df, batch_id):
        try:
            #  VALIDATION AVANT SAUVEGARDE
            alert_manager.validate_data_batch(batch_df, source_name, batch_id)
            
            # Sauvegarde PostgreSQL
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
            #  ALERTE EN CAS D'ERREUR
            alert_manager.create_alert(
                AlertType.PROCESSING_ERROR,
                AlertSeverity.CRITICAL,
                f"Erreur sauvegarde batch {batch_id} dans {table_name}: {str(e)}",
                table_name,
                {"batch_id": batch_id, "error": str(e)}
            )
            logger.error(f"Erreur sauvegarde {table_name}: {str(e)}")
    
    return df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", f"./checkpoints/{table_name}") \
        .trigger(processingTime='2 minutes') \
        .start()

# ============ MONITORING PÉRIODIQUE ============
def start_periodic_monitoring():
    """Surveillance périodique en arrière-plan"""
    def monitoring_loop():
        while True:
            try:
                time.sleep(300)  # Toutes les 5 minutes
                
                # Vérifier retards de données
                alert_manager.check_data_delay("weather_raw_10min", 15)
                alert_manager.check_data_delay("weather_hourly", 75)
                
                # Vérifier les streams Spark
                active_streams = spark.streams.active
                if len(active_streams) == 0:
                    alert_manager.create_alert(
                        AlertType.SYSTEM_HEALTH,
                        AlertSeverity.CRITICAL,
                        "Aucun stream Spark actif détecté",
                        "spark"
                    )
                
                logger.info(f" Check périodique effectué - {len(active_streams)} streams actifs")
                
            except Exception as e:
                alert_manager.create_alert(
                    AlertType.SYSTEM_HEALTH,
                    AlertSeverity.HIGH,
                    f"Erreur monitoring périodique: {str(e)}",
                    "monitoring"
                )
                time.sleep(60)  # Attendre plus longtemps en cas d'erreur
    
    # Lancer en thread daemon
    monitoring_thread = threading.Thread(target=monitoring_loop, daemon=True)
    monitoring_thread.start()
    logger.info("Monitoring périodique démarré")

# ============ STREAMS DE SAUVEGARDE AVEC SURVEILLANCE ============
query_raw = save_to_postgres_monitored(df_raw_10min, "weather_raw_10min", "raw_aggregation")
query_hourly = save_to_postgres_monitored(df_hourly, "weather_hourly", "hourly_aggregation")
query_daily = save_to_postgres_monitored(df_daily, "weather_daily", "daily_aggregation")

# Debug en console
debug_query = df_raw_10min.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime='1 minute') \
    .start()

# Prédictions avec surveillance
def generate_predictions_monitored(batch_df, batch_id):
    """Prédictions avec alertes en cas d'erreur"""
    try:
        generate_predictions(batch_df, batch_id)
        logger.info(f"Prédictions générées pour batch {batch_id}")
    except Exception as e:
        alert_manager.create_alert(
            AlertType.PROCESSING_ERROR,
            AlertSeverity.HIGH,
            f"Erreur génération prédictions batch {batch_id}: {str(e)}",
            "predictions",
            {"batch_id": batch_id, "error": str(e)}
        )

prediction_stream = df_hourly.writeStream \
    .foreachBatch(generate_predictions_monitored) \
    .option("checkpointLocation", "./checkpoints/predictions") \
    .trigger(processingTime='1 hour') \
    .start()

# ============ DÉMARRAGE DU MONITORING ============
start_periodic_monitoring()

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
    logger.error(f" Erreur critique du pipeline: {str(e)}")
    alert_manager.create_alert(
        AlertType.SYSTEM_HEALTH,
        AlertSeverity.CRITICAL,
        f"Erreur critique pipeline: {str(e)}",
        "system",
        {"error": str(e)}
    )
    raise

finally:
    logger.info(" Nettoyage et fermeture du pipeline")
    alert_manager.create_alert(
        AlertType.SYSTEM_HEALTH,
        AlertSeverity.LOW,
        "Pipeline fermé proprement",
        "system"
    )
    spark.stop()