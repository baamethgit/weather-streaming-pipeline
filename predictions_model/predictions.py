from .model import predict_temperature_4h , get_prediction_confidence
from pyspark.sql import SparkSession

def generate_predictions(batch_df, batch_id):
    """Fonction appelée APRÈS sauvegarde de chaque batch horaire"""
    spark_session = SparkSession.getActiveSession()
    try:
        # Vérifier qu'on a bien des nouvelles données horaires
        if batch_df.count() == 0:
            print(f"Batch {batch_id}: Pas de nouvelles données horaires")
            return
        
        print(f"Batch {batch_id}: Nouvelles données horaires détectées, génération des prédictions...")
        
        # 1. Récupérer les 8 dernières heures depuis PostgreSQL
        # (après que le batch actuel soit sauvé)
        recent_hourly = spark_session.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/weather_db") \
            .option("dbtable", "(SELECT * FROM weather_hourly ORDER BY timestamp DESC LIMIT 8) AS recent") \
            .option("user", "postgres") \
            .option("password", "postgreSQL") \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .orderBy("timestamp")
        
        rows = recent_hourly.collect()
        
        if len(rows) < 6:
            print(f"Batch {batch_id}: Pas assez de données ({len(rows)}) pour prédiction")
            return
        
        # 2. Préparer les données pour le modèle
        temperature_data = [row['temperature_avg'] for row in rows if row['temperature_avg'] is not None]
        humidity_data = [row['humidity_avg'] for row in rows if row['humidity_avg'] is not None]
        wind_data = [row['wind_speed_avg'] for row in rows if row['wind_speed_avg'] is not None]
        
        if len(temperature_data) < 6:
            print(f"Batch {batch_id}: Données température insuffisantes")
            return
        
        # 3. Générer prédictions
        predictions = predict_temperature_4h(temperature_data, humidity_data, wind_data)
        
        if "error" in predictions:
            print(f"Batch {batch_id}: {predictions['error']}")
            return
        
        confidence = get_prediction_confidence(temperature_data)
        
        # 4. Créer DataFrame pour sauvegarde
        from datetime import datetime
        now = datetime.now()
        
        prediction_data = [(
            now,  # prediction_time
            predictions.get('H+1'),
            predictions.get('H+2'), 
            predictions.get('H+3'),
            predictions.get('H+4'),
            confidence
        )]
        
        df_pred = spark_session.createDataFrame(
            prediction_data, 
            ['prediction_time', 'h_plus_1', 'h_plus_2', 'h_plus_3', 'h_plus_4', 'confidence']
        )
        
        # 5. Sauvegarder
        df_pred.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/weather_db") \
            .option("dbtable", "weather_predictions") \
            .option("user", "postgres") \
            .option("password", "postgreSQL") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(f"Batch {batch_id}: Prédictions sauvées - H+1:{predictions['H+1']}°C, Confiance:{confidence*100:.0f}%")
        
    except Exception as e:
        print(f"Batch {batch_id}: Erreur prédiction - {e}")