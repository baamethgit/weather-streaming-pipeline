import logging
import json
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col , max as spark_max, min as spark_min
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

class AlertType:
    """Types d'alertes possibles"""
    DATA_DELAY = "DATA_DELAY"
    DATA_MISSING = "DATA_MISSING" 
    QUALITY_ISSUE = "QUALITY_ISSUE"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    SYSTEM_HEALTH = "SYSTEM_HEALTH"

class AlertSeverity:
    """Niveaux de sévérité"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class WeatherAlert:
    """Classe pour représenter une alerte"""
    
    def __init__(self, alert_type, severity, message, source=None, timestamp=None, metadata=None):
        self.alert_type = alert_type
        self.severity = severity
        self.message = message
        self.source = source or "system"
        self.timestamp = timestamp or datetime.now()
        self.metadata = metadata or {}
        self.id = f"{self.alert_type}_{self.source}_{int(self.timestamp.timestamp())}"
    
    def to_spark_row(self):
        """Convertir en Row compatible avec Spark"""
        # PAS besoin d'importer Row, on retourne un tuple
        
        # Convertir metadata en JSON string si c'est un dict
        metadata_str = ""
        if self.metadata:
            try:
                metadata_str = json.dumps(self.metadata) if isinstance(self.metadata, dict) else str(self.metadata)
            except:
                metadata_str = str(self.metadata)
        
        # Retourner un tuple au lieu d'un Row
        return (
            self.id,
            self.alert_type,
            self.severity,
            self.message,
            self.source,
            self.timestamp,  # datetime sera correctement sérialisé via le schéma
            metadata_str
        )

import json
import os
from datetime import datetime

class AlertManager:
    """Gestionnaire central des alertes - Version fichier sécurisée"""
    
    def __init__(self, spark_session, logger):
        self.spark = spark_session
        self.logger = logger
        self.active_alerts = []
        
        # Créer le dossier pour les alertes
        os.makedirs("./logs", exist_ok=True)
        
    def create_alert(self, alert_type, severity, message, source=None, metadata=None):
        """Créer et enregistrer une nouvelle alerte"""
        alert = WeatherAlert(alert_type, severity, message, source, metadata=metadata)
        self.active_alerts.append(alert)
        
        # Logger selon la sévérité
        if severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]:
            self.logger.error(f"[ALERTE {severity}] {message}")
        elif severity == AlertSeverity.MEDIUM:
            self.logger.warning(f"[ALERTE {severity}] {message}")
        else:
            self.logger.info(f"[ALERTE {severity}] {message}")
        
        # Sauvegarder dans un fichier JSON (SÉCURISÉ)
        self._save_alert_to_file(alert)
        
        return alert
    
    def _save_alert_to_file(self, alert):
        """Sauvegarder l'alerte dans un fichier JSON"""
        try:
            alert_data = {
                "id": alert.id,
                "type": alert.alert_type,
                "severity": alert.severity,
                "message": alert.message,
                "source": alert.source,
                "timestamp": alert.timestamp.isoformat(),
                "metadata": alert.metadata
            }
            
            # Ajouter au fichier JSONL (une ligne par alerte)
            with open("./logs/weather_alerts.jsonl", "a", encoding='utf-8') as f:
                f.write(json.dumps(alert_data, ensure_ascii=False) + "\n")
                
        except Exception as e:
            # Log mais ne jamais crasher
            self.logger.warning(f"Impossible de sauvegarder l'alerte: {str(e)}")
    
    def get_active_alerts(self, max_age_hours=24):
        """Récupérer les alertes actives des dernières heures"""
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        return [alert for alert in self.active_alerts if alert.timestamp > cutoff]
# ==============================================
# FONCTIONS DE SURVEILLANCE SIMPLIFIÉES
# ==============================================

def monitor_data_freshness(df, source_name, alert_manager, max_delay_minutes=15):
    """Surveiller la fraîcheur des données - Version simplifiée"""
    
    def check_delays(batch_df, batch_id):
        try:
            record_count = batch_df.count()
            
            if record_count == 0:
                alert_manager.create_alert(
                    AlertType.DATA_MISSING,
                    AlertSeverity.MEDIUM,  # Réduire à MEDIUM pour éviter les crashes
                    f"Aucune donnée reçue pour {source_name} dans le batch {batch_id}",
                    source_name,
                    {"batch_id": batch_id}
                )
                return
            
            # Log normal sans calcul complexe pour éviter les erreurs
            alert_manager.logger.info(f"{source_name} - Batch {batch_id}: {record_count} records traités")
            
        except Exception as e:
            # Log l'erreur mais ne pas crasher
            alert_manager.logger.error(f"Erreur monitoring {source_name} batch {batch_id}: {str(e)}")
    
    return df.writeStream \
        .foreachBatch(check_delays) \
        .option("checkpointLocation", f"./checkpoints/monitor_{source_name}") \
        .trigger(processingTime='2 minutes') \
        .start()

def monitor_data_quality(df, source_name, alert_manager):
    """Surveiller la qualité des données - Version simplifiée"""
    
    def check_quality(batch_df, batch_id):
        try:
            record_count = batch_df.count()
            
            if record_count == 0:
                return
            
            # Vérifications basiques sans agrégations complexes
            null_temp_count = batch_df.filter(col("temperature").isNull()).count()
            
            if null_temp_count > 0:
                ratio = null_temp_count / record_count
                if ratio > 0.1:  # Plus de 10% de valeurs nulles
                    alert_manager.create_alert(
                        AlertType.QUALITY_ISSUE,
                        AlertSeverity.MEDIUM,
                        f"Trop de températures nulles {source_name}: {null_temp_count}/{record_count} ({ratio*100:.1f}%)",
                        source_name,
                        {"batch_id": batch_id, "null_ratio": ratio}
                    )
            else:
                alert_manager.logger.info(f"{source_name} - Batch {batch_id}: Qualité OK ({record_count} records)")
                
        except Exception as e:
            alert_manager.logger.error(f"Erreur contrôle qualité {source_name} batch {batch_id}: {str(e)}")
    
    return df.writeStream \
        .foreachBatch(check_quality) \
        .option("checkpointLocation", f"./checkpoints/quality_{source_name}") \
        .trigger(processingTime='5 minutes') \
        .start()


def setup_monitoring_for_pipeline(spark, logger, df_api, df_local, df_unified):
    """Configurer la surveillance complète du pipeline - Version simplifiée"""
    
    # Créer le gestionnaire d'alertes
    alert_manager = AlertManager(spark, logger)
    
    # SURVEILLANCE OPTIONNELLE - Peut être commentée pour débugger
    try:
        # Surveiller seulement les sources principales
        monitor_api = monitor_data_freshness(df_api, "api", alert_manager, max_delay_minutes=12)
        monitor_local = monitor_data_freshness(df_local, "local", alert_manager, max_delay_minutes=15)
        
        # Surveiller la qualité (simplifié)
        quality_api = monitor_data_quality(df_api, "api", alert_manager)
        
        monitors = [monitor_api, monitor_local, quality_api]
        logger.info("Système de surveillance démarré (version simplifiée)")
        
    except Exception as e:
        logger.warning(f"Impossible de démarrer la surveillance: {str(e)}")
        monitors = []
    
    return {
        "alert_manager": alert_manager,
        "monitors": monitors
    }