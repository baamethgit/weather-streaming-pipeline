Données météo en temps réel et en prédiction : Kafka + Spark + Aws

docker cp creation_donnees.sql postgres-container:/tmp/creation_donnees.sql

docker exec -it postgres-container psql -U postgres -d weather_db -f /tmp/creation_donnees.sql
