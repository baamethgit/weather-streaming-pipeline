from pyspark.sql import SparkSession
from feature_engineering import build_feature_pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline, PipelineModel

# 1. Démarrer SparkSession
spark = SparkSession.builder \
    .appName("ModelTraining") \
    .master("local[*]") \
    .getOrCreate()

# 2. Lire les données historiques fusionnées (10 minutes)
df = spark.read.option("header", True).csv("./resultats/weather_fused_10min/")

# 3. Nettoyage minimum et typage
df = df.withColumn("temperature", df["temperature"].cast("double")) \
       .withColumn("humidity", df["humidity"].cast("double")) \
       .withColumn("pressure", df["pressure"].cast("double")) \
       .withColumn("wind_speed", df["wind_speed"].cast("double")) \
       .withColumn("wind_deg", df["wind_deg"].cast("double"))

# 4. Appliquer le pipeline de feature engineering
pipeline = build_feature_pipeline()
df_transformed = pipeline.fit(df).transform(df)

# 5. Simuler un vecteur de sortie pour test (à remplacer par vraies moyennes futures)
from pyspark.sql.functions import col
df_final = df_transformed.withColumnRenamed("temperature", "label")

# 6. Entraîner un modèle de régression (ex : température uniquement pour test)
rf = RandomForestRegressor(featuresCol="features", labelCol="label")
model = rf.fit(df_final)

# 7. Sauvegarder le modèle
model.save("./modeles/rf_temperature_model")

print("✅ Modèle entraîné et sauvegardé.")