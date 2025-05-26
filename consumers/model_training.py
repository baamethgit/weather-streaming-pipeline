from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Démarrer la SparkSession (si ce n'est pas déjà fait)
spark = SparkSession.builder \
    .appName("WeatherModelTraining") \
    .getOrCreate()

# --- Supposons que tu as déjà df_fused (10min agrégées) ---
# Pour exemple, on recrée df_fused ici (tu dois adapter cette partie à ton contexte) :

# df_fused = ... (ton DataFrame fusionné avec colonnes : interval_start, temperature, humidity, pressure, wind_speed, etc.)

# --- Préparation des données pour la prédiction ---

# Objectif : prédire la température moyenne 1 intervalle (10 min) dans le futur
# On crée une colonne "target_temp" = temperature décalée d'un intervalle dans le futur

window_spec = Window.orderBy("interval_start")

df_ml = df_fused \
    .select(
        col("interval_start"),
        col("temperature"),
        col("humidity"),
        col("pressure"),
        col("wind_speed"),
        col("wind_deg"),
        col("feels_like"),
        col("uvi")
    ) \
    .withColumn("target_temp", lead("temperature", 1).over(window_spec)) \
    .na.drop(subset=["target_temp"])  # On enlève les lignes où target est null (fin de série)

# 2. Assemblage des features en un vecteur
feature_cols = ["temperature", "humidity", "pressure", "wind_speed", "wind_deg", "feels_like", "uvi"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

df_ml_features = assembler.transform(df_ml).select("features", "target_temp")

# 3. Split train/test
train_data, test_data = df_ml_features.randomSplit([0.8, 0.2], seed=42)

# 4. Création et entraînement du modèle de régression linéaire
lr = LinearRegression(featuresCol="features", labelCol="target_temp")

lr_model = lr.fit(train_data)

# 5. Évaluation sur le jeu de test
predictions = lr_model.transform(test_data)

evaluator = RegressionEvaluator(labelCol="target_temp", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

# 6. Affichage des coefficients du modèle
print("Coefficients:", lr_model.coefficients)
print("Intercept:", lr_model.intercept)

# 7. Optionnel : sauvegarder le modèle pour réutilisation
lr_model.save("./models/weather_temp_lr_model")

