# # monitoring/weather_alerts_logging.py
# import logging
# import json
# from datetime import datetime, timedelta
# from enum import Enum
# import psycopg2

# class AlertType(Enum):
#     DATA_QUALITY = "data_quality"
#     SYSTEM_HEALTH = "system_health" 
#     PROCESSING_ERROR = "processing_error"

# class AlertSeverity(Enum):
#     LOW = "low"
#     MEDIUM = "medium"
#     HIGH = "high"
#     CRITICAL = "critical"

# class AlertManager:
#     def __init__(self, logger):
#         self.logger = logger
#         self.last_data_received = {}
        
#     def create_alert(self, alert_type: AlertType, severity: AlertSeverity, 
#                     message: str, component: str, metadata: dict = None):
#         """Créer une alerte - LOG + DB"""
        
#         # 1. Logger l'alerte avec niveau approprié
#         log_levels = {
#             AlertSeverity.LOW: logging.INFO,
#             AlertSeverity.MEDIUM: logging.WARNING, 
#             AlertSeverity.HIGH: logging.ERROR,
#             AlertSeverity.CRITICAL: logging.CRITICAL
#         }
        
#         alert_msg = f"🚨 [{alert_type.value.upper()}] {message} | {component}"
#         if metadata:
#             alert_msg += f" | {json.dumps(metadata)}"
            
#         self.logger.log(log_levels[severity], alert_msg)
        
#         # 2. Sauvegarde en DB pour le dashboard
#         try:
#             self._save_alert_to_db(alert_type.value, severity.value, message, component, metadata)
#         except Exception as e:
#             self.logger.error(f"Erreur sauvegarde alerte DB: {e}")
        
#         # 3. Notification immédiate pour alertes critiques
#         if severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]:
#             print(f"\n🔥 ALERTE {severity.value.upper()}: {message} [{component}]")
#             # Ici tu peux ajouter Slack/email plus tard
    
#     def _save_alert_to_db(self, alert_type, severity, message, component, metadata):
#         """Sauvegarder l'alerte en DB"""
#         try:
#             conn = psycopg2.connect(
#                 host="localhost", port=5432, dbname="weather_db",
#                 user="postgres", password="postgreSQL"
#             )
            
#             with conn.cursor() as cur:
#                 cur.execute("""
#                     INSERT INTO weather_alerts 
#                     (alert_type, severity, message, component, metadata, created_at)
#                     VALUES (%s, %s, %s, %s, %s, %s)
#                 """, (alert_type, severity, message, component, 
#                      json.dumps(metadata) if metadata else None, datetime.now()))
            
#             conn.commit()
#             conn.close()
            
#         except Exception as e:
#             # Si table n'existe pas, la créer
#             self._create_alerts_table()
    
#     def _create_alerts_table(self):
#         """Créer la table des alertes si elle n'existe pas"""
#         try:
#             conn = psycopg2.connect(
#                 host="localhost", port=5432, dbname="weather_db",
#                 user="postgres", password="postgreSQL"
#             )
            
#             with conn.cursor() as cur:
#                 cur.execute("""
#                     CREATE TABLE IF NOT EXISTS weather_alerts (
#                         id SERIAL PRIMARY KEY,
#                         alert_type VARCHAR(50),
#                         severity VARCHAR(20),
#                         message TEXT,
#                         component VARCHAR(50),
#                         metadata JSONB,
#                         created_at TIMESTAMP,
#                         resolved BOOLEAN DEFAULT FALSE
#                     )
#                 """)
            
#             conn.commit()
#             conn.close()
#             print("✅ Table weather_alerts créée")
            
#         except Exception as e:
#             print(f"Erreur création table alerts: {e}")
    
#     def check_data_delay(self, source: str, threshold_minutes: int = 15):
#         """Vérifier si les données sont en retard"""
#         now = datetime.now()
#         last_time = self.last_data_received.get(source)
        
#         if last_time:
#             delay_minutes = (now - last_time).total_seconds() / 60
            
#             if delay_minutes > threshold_minutes:
#                 severity = AlertSeverity.CRITICAL if delay_minutes > 30 else AlertSeverity.HIGH
                
#                 self.create_alert(
#                     AlertType.DATA_QUALITY,
#                     severity,
#                     f"Données {source} en retard de {delay_minutes:.1f} minutes",
#                     source,
#                     {"delay_minutes": delay_minutes}
#                 )
#                 return False
#         else:
#             self.create_alert(
#                 AlertType.DATA_QUALITY,
#                 AlertSeverity.MEDIUM,
#                 f"Première réception de données {source}",
#                 source
#             )
        
#         return True
    
#     def validate_data_batch(self, batch_df, source: str):
#         """Validation rapide d'un batch"""
#         try:
#             count = batch_df.count()
            
#             if count == 0:
#                 self.create_alert(
#                     AlertType.DATA_QUALITY,
#                     AlertSeverity.MEDIUM,
#                     f"Batch vide reçu de {source}",
#                     source
#                 )
#                 return
            
#             # Marquer réception des données
#             self.last_data_received[source] = datetime.now()
            
#             # Validation simple température
#             if "temperature" in batch_df.columns:
#                 temp_rows = batch_df.select("temperature").filter(
#                     (batch_df.temperature < -40) | (batch_df.temperature > 50)
#                 ).count()
                
#                 if temp_rows > 0:
#                     self.create_alert(
#                         AlertType.DATA_QUALITY,
#                         AlertSeverity.HIGH,
#                         f"{temp_rows} températures aberrantes détectées dans {source}",
#                         source,
#                         {"aberrant_count": temp_rows, "total_count": count}
#                     )
            
#             # Log succès
#             self.logger.info(f"✅ Batch {source} validé: {count} lignes")
            
#         except Exception as e:
#             self.create_alert(
#                 AlertType.PROCESSING_ERROR,
#                 AlertSeverity.CRITICAL,
#                 f"Erreur validation batch {source}: {str(e)}",
#                 source,
#                 {"error": str(e)}
#             )

# def setup_monitoring_for_pipeline(spark, logger, df_api, df_local, df_unified):
#     """Configuration monitoring pour ton pipeline"""
    
#     alert_manager = AlertManager(logger)
    
#     # Créer la table des alertes
#     alert_manager._create_alerts_table()
    
#     # Fonction à utiliser dans tes foreachBatch
#     def validate_and_alert(batch_df, batch_id, source):
#         alert_manager.validate_data_batch(batch_df, source)
#         alert_manager.check_data_delay(source)
    
#     # Check périodique (optionnel - à lancer dans un thread)
#     def periodic_check():
#         import threading
#         import time
        
#         def check_loop():
#             while True:
#                 time.sleep(300)  # Toutes les 5 minutes
#                 alert_manager.check_data_delay("api", 15)
#                 alert_manager.check_data_delay("local", 20)
        
#         thread = threading.Thread(target=check_loop, daemon=True)
#         thread.start()
    
#     return {
#         "alert_manager": alert_manager,
#         "validate_batch": validate_and_alert,
#         "start_periodic_check": periodic_check
#     }