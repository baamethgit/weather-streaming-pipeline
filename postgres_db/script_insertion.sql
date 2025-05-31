
-- donées brutes par 10MINUTES

INSERT INTO weather_raw_10min (
    timestamp, temperature, humidity, pressure, wind_speed, 
    wind_deg, feels_like, weather_main, weather_description
    ) WITH demo_data AS ( SELECT 
            -- Timestamp toutes les 10 minutes sur 7 jours
            NOW() - INTERVAL '7 days' + (INTERVAL '10 minutes' * generate_series(0, 1007)) as ts,
            generate_series(0, 1007) as seq
    ),
    weather_patterns AS (
        SELECT 
            ts as timestamp,
            seq,
            -- Température avec cycle jour/nuit + variations aléatoirss
            ROUND(
                (22 + 8 * SIN(2 * PI() * EXTRACT(hour FROM ts) / 24) + 
                3 * SIN(2 * PI() * seq / 144) + 
                (RANDOM() - 0.5) * 4)::numeric, 2
            ) as temperature,
            
            -- Humidité inversement corrélée à la température
            ROUND(
                (65 - 15 * SIN(2 * PI() * EXTRACT(hour FROM ts) / 24) + 
                (RANDOM() - 0.5) * 20)::numeric, 2
            ) as humidity,
            
            -- Pression atmosphérique avec tendances
            ROUND(
                (1013 + 10 * SIN(2 * PI() * seq / 500) + 
                (RANDOM() - 0.5) * 8)::numeric, 2
            ) as pressure,
            
            -- Vitesse du vent
            ROUND(
                (3 + 4 * RANDOM() + 2 * SIN(2 * PI() * seq / 200))::numeric, 2
            ) as wind_speed,
            
            -- Direction du vent (0-360°)
            FLOOR(RANDOM() * 360) as wind_deg
            
        FROM demo_data
    )
    SELECT 
        timestamp,
        GREATEST(temperature, -10) as temperature, -- Limite min
        LEAST(GREATEST(humidity, 20), 100) as humidity, -- Limite 20-100%
        GREATEST(pressure, 950) as pressure, -- Limite min pression
        GREATEST(wind_speed, 0) as wind_speed,
        wind_deg,
        
        -- Température ressentie (approximation simple)
        ROUND(
            (temperature - 0.4 * (temperature - 10) * (1 - humidity/100))::numeric, 2
        ) as feels_like,
        
        -- Conditions météo basées sur humidité et pression
        CASE 
            WHEN humidity > 80 AND pressure < 1000 THEN 'Rain'
            WHEN humidity > 70 AND pressure < 1005 THEN 'Clouds'
            WHEN humidity < 40 AND pressure > 1020 THEN 'Clear'
            WHEN humidity BETWEEN 40 AND 70 THEN 'Clear'
            ELSE 'Clouds'
        END as weather_main,
        
        -- Description détaillée
        CASE 
            WHEN humidity > 85 AND pressure < 995 THEN 'heavy rain'
            WHEN humidity > 80 AND pressure < 1000 THEN 'light rain'
            WHEN humidity > 75 AND pressure < 1005 THEN 'overcast clouds'
            WHEN humidity > 60 AND pressure < 1010 THEN 'few clouds'
            WHEN humidity < 35 AND pressure > 1020 THEN 'clear sky'
            WHEN humidity BETWEEN 35 AND 60 THEN 'clear sky'
            ELSE 'scattered clouds'
        END as weather_description
        
    FROM weather_patterns
    ORDER BY timestamp;

-- test(affcihage de quelques statistiques)
--  SELECT
--     COUNT(*) as total_records,
--     MIN(timestamp) as first_record,
--     MAX(timestamp) as last_record,
--     AVG(temperature) as avg_temp,
--     AVG(humidity) as avg_humidity,
--     COUNT(DISTINCT weather_main) as weather_conditions
-- FROM weather_raw_10min;

-- 


-- données horaires(aggrégé)

INSERT INTO weather_hourly (
  timestamp, temperature_avg, temperature_min, temperature_max, 
  humidity_avg, pressure_avg, wind_speed_avg, feels_like_avg, weather_main
)
SELECT 
  date_trunc('hour', timestamp) as timestamp,
  AVG(temperature) as temperature_avg,
  MIN(temperature) as temperature_min,
  MAX(temperature) as temperature_max,
  AVG(humidity) as humidity_avg,
  AVG(pressure) as pressure_avg,
  AVG(wind_speed) as wind_speed_avg,
  AVG(feels_like) as feels_like_avg,
  MODE() WITHIN GROUP (ORDER BY weather_main) as weather_main
FROM weather_raw_10min 
WHERE weather_main IS NOT NULL
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




-- données journalières ()

INSERT INTO weather_daily (
  timestamp, temperature_avg, temperature_min, temperature_max, 
  humidity_avg, pressure_avg, wind_speed_avg, feels_like_avg, weather_main
)
SELECT 
  date_trunc('day', timestamp)::date as timestamp,
  AVG(temperature) as temperature_avg,
  MIN(temperature) as temperature_min,
  MAX(temperature) as temperature_max,
  AVG(humidity) as humidity_avg,
  AVG(pressure) as pressure_avg,
  AVG(wind_speed) as wind_speed_avg,
  AVG(feels_like) as feels_like_avg,
  MODE() WITHIN GROUP (ORDER BY weather_main) as weather_main
FROM weather_raw_10min 
WHERE weather_main IS NOT NULL
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