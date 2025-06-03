# Données Météo en Temps Réel et Prédiction


Pipeline Big Data qui collecte des données météo depuis deux sources (API OpenWeather + capteur IoT simulé), les traite en temps réel avec Spark Streaming, génère des prédictions à 4h et les visualise dans un dashboard web interactif.

**Technologies :** Kafka, Spark Structured Streaming, PostgreSQL, Flask, Chart.js, Docker


## Structure du projet

```
weather-pipeline/
├── .env                     # Variables d'environnement (à créer)
├── requirements.txt         # Dépendances Python
├── kafka/
│   └── docker-compose.yaml     # Services Kafka + Zookeeper + UI
├── postgres_db/
│   ├── script_schema.sql       # Création des tables
│   └── creation_donnees.sql    # Données de test pour dashboard
├── producers/
│   ├── weather_api.py          # Producteur API OpenWeather
│   └── iot_device.py           # Producteur capteur IoT simulé
├── consumers/
│   └── final2.py               # Pipeline Spark principal
├── predictions_model/
│   ├── model.py                # Algorithme de prédiction
│   └── predictions.py          # Intégration Spark
├── dashboard/
│   ├── app_flask.py            # Backend API
│   ├── dashboard.html          # Interface web
│   ├── script.js               # Logique frontend
│   └── style.css               # Styles
└── logs/
    └── weather_pipeline.log    # Logs du pipeline
```

---

## Installation et Configuration

### 1. Prérequis
- Spark installé
- Docker & Docker Compose
- Java 8+ (pour Spark)

### 2. Cloner le projet 
```bash
git clone <repo-url>
cd weather-pipeline

```

### 3. installer les dépendances
```bash
python -m venv venv

pip install -r requirements.txt
```

### 4. Créer le fichier .env à la racine
```bash
# Base de données PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_db
DB_USER=postgres
DB_PASSWORD=postgreSQL

```

### 5. Démarrer les services Docker

**Kafka (peut nécessiter 2 tentatives)**
```bash
cd kafka/
docker-compose up -d

# Si le service kafka est "down", le redémarrer :
docker-compose restart kafka
docker-compose ps  # Vérifier que tout est "Up"
```

**PostgreSQL**
```bash
# Démarrer PostgreSQL en Docker
docker run --name postgres-container -e POSTGRES_PASSWORD=postgreSQL -e POSTGRES_DB=weather_db -p 5432:5432 -d postgres:13
```

### 6. Créer les tables PostgreSQL (postgres sur docker)
```bash
# Copier le script dans le container
docker cp postgres_db/script_schema.sql postgres-container:/tmp/script_schema.sql

# Exécuter le script de création
docker exec -it postgres-container psql -U postgres -d weather_db -f /tmp/script_schema.sql
```

### 7. (Optionnel) Peupler avec des données de test
```bash
# Pour tester le dashboard sans attendre des heures
docker cp postgres_db/creation_donnees.sql postgres-container:/tmp/creation_donnees.sql
docker exec -it postgres-container psql -U postgres -d weather_db -f /tmp/creation_donnees.sql
```

---

## Lancement du Pipeline


### 1. Vérifier les services Docker
```bash
# Kafka UI accessible sur http://localhost:8080
# PostgreSQL sur localhost:5432
```

### 2. Démarrer les producteurs de données
```bash

python producers/weather_api.py

 
python producers/iot_device.py
```

### 3. Lancer le pipeline Spark
```bash

python consumers/final2.py
```

### 4. Démarrer l'API Flask
```bash
python dashboard/app_flask.py
```

### 5. Ouvrir le dashboard
```bash
# Ouvrir dashboard/dashboard.html dans un navigateur

```

---

### Logs et debugging
- **Logs Spark** : `logs/weather_pipeline.log`
- **Alertes** : Table `weather_alerts` en base + logs
- **Console** : Messages temps réel lors de l'exécution

---

## Problèmes connus

### Kafka ne démarre pas au premier essai
```bash
# Solution : redémarrer le service
cd kafka/
docker-compose restart kafka
```

### Erreurs mémoire Spark sur Windows
- Les paramètres sont configurés à 4G (executor + driver)
- Réduire si nécessaire dans `final2.py` :
```python
.config("spark.executor.memory", "2g")
.config("spark.driver.memory", "2g")
```
