DELETE FROM weather_predictions;
DELETE FROM weather_daily;
DELETE FROM weather_hourly;
DELETE FROM weather_raw_10min;


-- 1. DONNÉES BRUTES TOUTES LES 10 MINUTES (48 dernières heures)


INSERT INTO weather_raw_10min (
    timestamp, temperature, humidity, pressure, wind_speed, 
    wind_deg, feels_like, weather_main, weather_description
) 
WITH time_series AS (
    SELECT 
        NOW() - INTERVAL '48 hours' + (INTERVAL '10 minutes' * generate_series(0, 287)) as ts,
        generate_series(0, 287) as seq
),
weather_data AS (
    SELECT 
        ts as timestamp,
        seq,
        EXTRACT(hour FROM ts) as hour_of_day,
        EXTRACT(day FROM ts - INTERVAL '48 hours') as day_offset,
        
    
        ROUND(
            (30 + 6 * SIN(2 * PI() * EXTRACT(hour FROM ts) / 24 - PI()/2) + 
             2 * SIN(2 * PI() * seq / 144) + 
             (RANDOM() - 0.5) * 3 +
             (seq / 288.0) * 1 
            )::numeric, 1
        ) as temperature,
        
      
        ROUND(
            (67 - 18 * SIN(2 * PI() * EXTRACT(hour FROM ts) / 24 - PI()/2) + 
             5 * SIN(2 * PI() * seq / 200) +
             (RANDOM() - 0.5) * 12)::numeric, 1
        ) as humidity,
        
        ROUND(
            (1013 + 3 * SIN(2 * PI() * seq / 600) + 
             (RANDOM() - 0.5) * 4)::numeric, 1
        ) as pressure,
        
    
        ROUND(
            (8 + 7 * RANDOM() + 3 * SIN(2 * PI() * seq / 180))::numeric, 1
        ) as wind_speed,
        
        
        (30 + (RANDOM() * 60) + 10 * SIN(2 * PI() * seq / 100))::int as wind_deg
        
    FROM time_series
)
SELECT 
    timestamp,
    LEAST(GREATEST(temperature, 22), 38) as temperature, -- Limites réalistes
    LEAST(GREATEST(humidity, 35), 90) as humidity,       -- Limites humidité
    LEAST(GREATEST(pressure, 1005), 1020) as pressure,   -- Limites pression
    LEAST(GREATEST(wind_speed, 2), 25) as wind_speed,    -- Limites vent
    wind_deg % 360 as wind_deg,
    
    -- TEMPÉRATURE RESSENTIE 
    ROUND(
        (temperature + 0.3 * (humidity/100) * (temperature - 15) + 1.5)::numeric, 1
    ) as feels_like,
    
    -- CONDITIONS MÉTÉO RÉALISTES POUR LE SÉNÉGAL EN JUIN
    CASE 
        WHEN humidity > 75 AND pressure < 1010 AND hour_of_day BETWEEN 14 AND 18 THEN 'Rain'
        WHEN humidity > 85 OR pressure < 1008 THEN 'Clouds'
        WHEN humidity < 50 AND temperature > 32 THEN 'Clear'
        WHEN hour_of_day BETWEEN 6 AND 18 AND humidity < 65 THEN 'Clear'
        ELSE 'Clouds'
    END as weather_main,
    
    -- DESCRIPTIONS DÉTAILLÉES
    CASE 
        WHEN humidity > 80 AND pressure < 1008 AND hour_of_day BETWEEN 15 AND 17 THEN 'orage tropical'
        WHEN humidity > 75 AND pressure < 1010 THEN 'pluie modérée'
        WHEN humidity > 70 AND temperature > 34 THEN 'nuages épars'
        WHEN humidity < 45 AND temperature > 35 THEN 'ciel dégagé, très chaud'
        WHEN hour_of_day BETWEEN 6 AND 10 AND humidity < 60 THEN 'ciel clair'
        WHEN hour_of_day BETWEEN 11 AND 17 AND humidity < 55 THEN 'ensoleillé'
        WHEN hour_of_day BETWEEN 18 AND 22 THEN 'quelques nuages'
        ELSE 'partiellement nuageux'
    END as weather_description
    
FROM weather_data
ORDER BY timestamp;

-- 2 AGRÉGATION DONNÉES HORAIRES

INSERT INTO weather_hourly (
    timestamp, temperature_avg, temperature_min, temperature_max, 
    humidity_avg, pressure_avg, wind_speed_avg, feels_like_avg, weather_main
)
SELECT 
    date_trunc('hour', timestamp) as timestamp,
    ROUND(AVG(temperature)::numeric, 1) as temperature_avg,
    ROUND(MIN(temperature)::numeric, 1) as temperature_min,
    ROUND(MAX(temperature)::numeric, 1) as temperature_max,
    ROUND(AVG(humidity)::numeric, 1) as humidity_avg,
    ROUND(AVG(pressure)::numeric, 1) as pressure_avg,
    ROUND(AVG(wind_speed)::numeric, 1) as wind_speed_avg,
    ROUND(AVG(feels_like)::numeric, 1) as feels_like_avg,
    MODE() WITHIN GROUP (ORDER BY weather_main) as weather_main
FROM weather_raw_10min 
WHERE timestamp >= NOW() - INTERVAL '48 hours'
GROUP BY date_trunc('hour', timestamp)
ON CONFLICT (timestamp) DO UPDATE SET
    temperature_avg = EXCLUDED.temperature_avg,
    temperature_min = EXCLUDED.temperature_min,
    temperature_max = EXCLUDED.temperature_max,
    humidity_avg = EXCLUDED.humidity_avg,
    pressure_avg = EXCLUDED.pressure_avg,
    wind_speed_avg = EXCLUDED.wind_speed_avg,
    feels_like_avg = EXCLUDED.feels_like_avg,
    weather_main = EXCLUDED.weather_main;


-- 3. AGRÉGATION DONNÉES JOURNALIÈRES


INSERT INTO weather_daily (
    timestamp, temperature_avg, temperature_min, temperature_max, 
    humidity_avg, pressure_avg, wind_speed_avg, feels_like_avg, weather_main
)
SELECT 
    date_trunc('day', timestamp)::date as timestamp,
    ROUND(AVG(temperature)::numeric, 1) as temperature_avg,
    ROUND(MIN(temperature)::numeric, 1) as temperature_min,
    ROUND(MAX(temperature)::numeric, 1) as temperature_max,
    ROUND(AVG(humidity)::numeric, 1) as humidity_avg,
    ROUND(AVG(pressure)::numeric, 1) as pressure_avg,
    ROUND(AVG(wind_speed)::numeric, 1) as wind_speed_avg,
    ROUND(AVG(feels_like)::numeric, 1) as feels_like_avg,
    MODE() WITHIN GROUP (ORDER BY weather_main) as weather_main
FROM weather_raw_10min 
WHERE timestamp >= NOW() - INTERVAL '48 hours'
GROUP BY date_trunc('day', timestamp)::date
ON CONFLICT (timestamp) DO UPDATE SET
    temperature_avg = EXCLUDED.temperature_avg,
    temperature_min = EXCLUDED.temperature_min,
    temperature_max = EXCLUDED.temperature_max,
    humidity_avg = EXCLUDED.humidity_avg,
    pressure_avg = EXCLUDED.pressure_avg,
    wind_speed_avg = EXCLUDED.wind_speed_avg,
    feels_like_avg = EXCLUDED.feels_like_avg,
    weather_main = EXCLUDED.weather_main;


-- 4. PRÉDICTIONS MÉTÉO (4 heures suivantes)


-- Nettoyer anciennes prédictions
DELETE FROM weather_predictions WHERE prediction_time < NOW() - INTERVAL '24 hours';

-- Générer nouvelles prédictions réalistes
INSERT INTO weather_predictions (
    prediction_time, h_plus_1, h_plus_2, h_plus_3, h_plus_4, confidence
)
WITH latest_temp AS (
    SELECT temperature, humidity, pressure, wind_speed
    FROM weather_raw_10min 
    ORDER BY timestamp DESC 
    LIMIT 1
),
prediction_base AS (
    SELECT 
        NOW() as prediction_time,
        temperature as current_temp,
        humidity as current_humidity,
        pressure as current_pressure,
        EXTRACT(hour FROM NOW()) as current_hour
    FROM latest_temp
)
SELECT 
    prediction_time,
    
    -- H+1: Évolution naturelle selon l'heure
    ROUND(
        (current_temp + 
         CASE 
             WHEN current_hour BETWEEN 6 AND 12 THEN 1.5 + RANDOM() * 1.5  -- Montée matinale
             WHEN current_hour BETWEEN 13 AND 17 THEN -0.5 + RANDOM() * 1   -- Plateau/légère baisse
             WHEN current_hour BETWEEN 18 AND 22 THEN -1.5 + RANDOM() * 0.5 -- Baisse soirée
             ELSE -0.5 + RANDOM() * 1                                        -- Nuit stable
         END)::numeric, 1
    ) as h_plus_1,
    
    -- H+2: Continuation de la tendance
    ROUND(
        (current_temp + 
         CASE 
             WHEN current_hour BETWEEN 6 AND 11 THEN 2.5 + RANDOM() * 2     -- Montée continue
             WHEN current_hour BETWEEN 12 AND 16 THEN 0 + RANDOM() * 1.5    -- Stabilité chaude
             WHEN current_hour BETWEEN 17 AND 21 THEN -2.5 + RANDOM() * 1   -- Baisse marquée
             ELSE -1 + RANDOM() * 1                                          -- Nuit
         END)::numeric, 1
    ) as h_plus_2,
    
    -- H+3: Tendance prolongée
    ROUND(
        (current_temp + 
         CASE 
             WHEN current_hour BETWEEN 6 AND 10 THEN 3.5 + RANDOM() * 2     -- Pic matinal
             WHEN current_hour BETWEEN 11 AND 15 THEN 1 + RANDOM() * 2      -- Maintien chaleur
             WHEN current_hour BETWEEN 16 AND 20 THEN -3.5 + RANDOM() * 1.5 -- Refroidissement
             ELSE -1.5 + RANDOM() * 1.5                                      -- Nuit fraîche
         END)::numeric, 1
    ) as h_plus_3,
    
    -- H+4: Évolution vers cycle suivant
    ROUND(
        (current_temp + 
         CASE 
             WHEN current_hour BETWEEN 6 AND 9 THEN 4 + RANDOM() * 2.5      -- Vers pic
             WHEN current_hour BETWEEN 10 AND 14 THEN 2 + RANDOM() * 2      -- Pic atteint
             WHEN current_hour BETWEEN 15 AND 19 THEN -4 + RANDOM() * 2     -- Descente
             ELSE -2 + RANDOM() * 2                                          -- Vers minimum nocturne
         END)::numeric, 1
    ) as h_plus_4,
    
    -- Confiance élevée (données récentes)
    0.85 + RANDOM() * 0.12 as confidence
    
FROM prediction_base;



-- Statistiques générales
SELECT 
    'DONNÉES BRUTES' as type,
    COUNT(*) as total_records,
    MIN(timestamp) as first_record,
    MAX(timestamp) as last_record,
    ROUND(AVG(temperature)::numeric, 1) as avg_temp,
    ROUND(MIN(temperature)::numeric, 1) as min_temp,
    ROUND(MAX(temperature)::numeric, 1) as max_temp,
    ROUND(AVG(humidity)::numeric, 1) as avg_humidity
FROM weather_raw_10min
WHERE timestamp >= NOW() - INTERVAL '48 hours'

UNION ALL

SELECT 
    'DONNÉES HORAIRES' as type,
    COUNT(*) as total_records,
    MIN(timestamp) as first_record,
    MAX(timestamp) as last_record,
    ROUND(AVG(temperature_avg)::numeric, 1) as avg_temp,
    ROUND(MIN(temperature_min)::numeric, 1) as min_temp,
    ROUND(MAX(temperature_max)::numeric, 1) as max_temp,
    ROUND(AVG(humidity_avg)::numeric, 1) as avg_humidity
FROM weather_hourly
WHERE timestamp >= NOW() - INTERVAL '48 hours'

UNION ALL

SELECT 
    'PRÉDICTIONS' as type,
    COUNT(*) as total_records,
    prediction_time as first_record,
    prediction_time as last_record,
    ROUND(((h_plus_1 + h_plus_2 + h_plus_3 + h_plus_4) / 4)::numeric, 1) as avg_temp,
    ROUND(LEAST(h_plus_1, h_plus_2, h_plus_3, h_plus_4)::numeric, 1) as min_temp,
    ROUND(GREATEST(h_plus_1, h_plus_2, h_plus_3, h_plus_4)::numeric, 1) as max_temp,
    NULL as avg_humidity
FROM weather_predictions
WHERE prediction_time >= NOW() - INTERVAL '1 hour';
