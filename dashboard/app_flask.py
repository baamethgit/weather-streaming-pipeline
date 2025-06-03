from flask import Flask, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from flask_cors import CORS
import logging
from datetime import datetime

import os
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



load_dotenv('../.env')

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'weather_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgreSQL')
}

def get_conn():
    """Connexion à la bd"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logger.error(f"Erreur connexion DB: {e}")
        raise

def safe_execute_query(query, params=None):
    """Exécution sécurisée des requêtes"""
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params or ())
                if query.strip().upper().startswith('SELECT'):
                    return cur.fetchall()
                return cur.rowcount
    except Exception as e:
        logger.error(f"Erreur requête SQL: {e}")
        return None

@app.route('/api/latest-raw')
def latest_raw():
    """Récupérer la dernière mesure 10min avec timestamp"""
    query = """
        SELECT *, 
               EXTRACT(EPOCH FROM (NOW() - timestamp)) / 60 AS age_minutes
        FROM weather_raw_10min
        ORDER BY timestamp DESC
        LIMIT 1
    """
    
    try:
        rows = safe_execute_query(query)
        if not rows:
            return jsonify({'error': 'Aucune donnée disponible'}), 204
        
        row = rows[0]
        
        # Convertir en dict normal et ajouter des métadonnées
        result = dict(row)
        result['timestamp'] = row['timestamp'].isoformat() if row['timestamp'] else None
        result['data_age_minutes'] = round(row.get('age_minutes', 0), 1)
        result['is_fresh'] = row.get('age_minutes', 999) <= 20  # Fresh si < 20min
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erreur API latest-raw: {e}")
        return jsonify({'error': 'Erreur serveur'}), 500

@app.route('/api/hourly-temperature')
def hourly_temperature():
    """Récupérer les températures horaires des 8 dernières heures"""
    query = """
        SELECT timestamp, temperature_avg,
               EXTRACT(EPOCH FROM (NOW() - timestamp)) / 60 AS age_minutes
        FROM weather_hourly
        WHERE temperature_avg IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 8
    """
    
    try:
        rows = safe_execute_query(query)
        if not rows:
            return jsonify([])
        
        # Convertir et inverser l'ordre (plus ancien en premier)
        result = []
        for row in reversed(rows):
            result.append({
                'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None,
                'temperature_avg': float(row['temperature_avg']) if row['temperature_avg'] is not None else None,
                'age_minutes': round(row.get('age_minutes', 0), 1)
            })
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erreur API hourly-temperature: {e}")
        return jsonify([])

@app.route('/api/predictions')
def predictions():
    """Récupérer les dernières prédictions"""
    query = """
        SELECT *, 
               EXTRACT(EPOCH FROM (NOW() - prediction_time)) / 60 AS age_minutes
        FROM weather_predictions
        ORDER BY prediction_time DESC
        LIMIT 1
    """
    
    try:
        rows = safe_execute_query(query)
        if not rows:
            return jsonify({'error': 'Aucune prédiction disponible'}), 404
        
        row = rows[0]
        result = dict(row)
        result['prediction_time'] = row['prediction_time'].isoformat() if row['prediction_time'] else None
        result['age_minutes'] = round(row.get('age_minutes', 0), 1)
        result['is_fresh'] = row.get('age_minutes', 999) <= 240  # Fresh si < 4h
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erreur API predictions: {e}")
        return jsonify({'error': 'Erreur serveur'}), 500

@app.route('/api/daily-extremes')
def daily_extremes():
    """Récupérer les extrêmes des 7 derniers jours"""
    query = """
        SELECT timestamp, temperature_min, temperature_max
        FROM weather_daily
        WHERE temperature_min IS NOT NULL AND temperature_max IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 7
    """
    
    try:
        rows = safe_execute_query(query)
        if not rows:
            return jsonify([])
        
        # Convertir et inverser (plus ancien en premier)
        result = []
        for row in reversed(rows):
            result.append({
                'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None,
                'temperature_min': float(row['temperature_min']) if row['temperature_min'] is not None else None,
                'temperature_max': float(row['temperature_max']) if row['temperature_max'] is not None else None
            })
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erreur API daily-extremes: {e}")
        return jsonify([])

@app.route('/api/wind-distribution')
def wind_distribution():
    """Rose des vents - CORRIGÉE : moyenne au lieu de somme"""
    query = """
        SELECT wind_deg, wind_speed 
        FROM weather_raw_10min
        WHERE wind_deg IS NOT NULL 
        AND wind_speed IS NOT NULL 
        AND wind_speed > 0
        AND timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY timestamp DESC
        LIMIT 200
    """
    
    try:
        rows = safe_execute_query(query)
        if not rows:
            return jsonify({
                'N': 0, 'NE': 0, 'E': 0, 'SE': 0,
                'S': 0, 'SO': 0, 'O': 0, 'NO': 0
            })
        
        # Accumuler vitesses ET compteurs par direction
        directions_data = {
            'N': {'total_speed': 0, 'count': 0},
            'NE': {'total_speed': 0, 'count': 0},
            'E': {'total_speed': 0, 'count': 0},
            'SE': {'total_speed': 0, 'count': 0},
            'S': {'total_speed': 0, 'count': 0},
            'SO': {'total_speed': 0, 'count': 0},
            'O': {'total_speed': 0, 'count': 0},
            'NO': {'total_speed': 0, 'count': 0}
        }
        
        for row in rows:
            deg = float(row['wind_deg'])
            speed = float(row['wind_speed'])
            
            # Déterminer la direction
            if deg < 22.5 or deg >= 337.5:
                direction = 'N'
            elif deg < 67.5:
                direction = 'NE'
            elif deg < 112.5:
                direction = 'E'
            elif deg < 157.5:
                direction = 'SE'
            elif deg < 202.5:
                direction = 'S'
            elif deg < 247.5:
                direction = 'SO'
            elif deg < 292.5:
                direction = 'O'
            else:
                direction = 'NO'
            
            directions_data[direction]['total_speed'] += speed
            directions_data[direction]['count'] += 1
        
        # Calculer les moyennes
        result = {}
        for direction, data in directions_data.items():
            if data['count'] > 0:
                result[direction] = round(data['total_speed'] / data['count'], 1)
            else:
                result[direction] = 0.0
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Erreur API wind-distribution: {e}")
        return jsonify({
            'N': 0, 'NE': 0, 'E': 0, 'SE': 0,
            'S': 0, 'SO': 0, 'O': 0, 'NO': 0
        })

@app.route('/api/health')
def health_check():
    """Endpoint de santé pour vérifier l'API et la DB"""
    try:
        # Test connexion DB
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        
        # Test données récentes
        recent_data = safe_execute_query("""
            SELECT COUNT(*) as count, 
                   MAX(timestamp) as last_update
            FROM weather_raw_10min 
            WHERE timestamp >= NOW() - INTERVAL '1 hour'
        """)
        
        result = {
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.now().isoformat(),
            'recent_data_count': recent_data[0]['count'] if recent_data else 0,
            'last_data_update': recent_data[0]['last_update'].isoformat() if recent_data and recent_data[0]['last_update'] else None
        }
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# Gestionnaire d'erreur global
@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Erreur 500: {error}")
    return jsonify({'error': 'Erreur interne du serveur'}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint non trouvé'}), 404

@app.route('/api/alerts')
def get_alerts():
    """Récupérer les alertes récentes"""
    query = """
        SELECT alert_type, severity, message, component, created_at,
               EXTRACT(EPOCH FROM (NOW() - created_at)) / 60 AS age_minutes
        FROM weather_alerts 
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        ORDER BY created_at DESC 
        LIMIT 20
    """
    
    try:
        rows = safe_execute_query(query)
        if not rows:
            return jsonify([])
        
        alerts = []
        for row in rows:
            alerts.append({
                'type': row['alert_type'],
                'severity': row['severity'],
                'message': row['message'],
                'component': row['component'],
                'age_minutes': round(row['age_minutes'], 1),
                'timestamp': row['created_at'].isoformat()
            })
        
        return jsonify(alerts)
        
    except Exception as e:
        logger.error(f"Erreur API alerts: {e}")
        return jsonify([])

@app.route('/api/system-health')
def system_health():
    """Status général du système"""
    try:
        # Compter les alertes critiques récentes
        critical_alerts = safe_execute_query("""
            SELECT COUNT(*) as count FROM weather_alerts 
            WHERE severity IN ('high', 'critical') 
            AND created_at >= NOW() - INTERVAL '1 hour'
        """)
        
        # Vérifier la fraîcheur des données
        last_data = safe_execute_query("""
            SELECT MAX(timestamp) as last_update,
                   EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) / 60 AS age_minutes
            FROM weather_raw_10min
        """)
        
        critical_count = critical_alerts[0]['count'] if critical_alerts else 0
        data_age = last_data[0]['age_minutes'] if last_data and last_data[0]['age_minutes'] else 999
        
        status = "healthy"
        if critical_count > 0 or data_age > 20:
            status = "degraded"
        if data_age > 60:
            status = "critical"
        
        return jsonify({
            'status': status,
            'critical_alerts_last_hour': critical_count,
            'data_age_minutes': round(data_age, 1),
            'last_update': last_data[0]['last_update'].isoformat() if last_data and last_data[0]['last_update'] else None
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})
    
if __name__ == '__main__':
    logger.info("Démarrage de l'API Flask...")
    app.run(debug=True, host='127.0.0.1', port=5000)