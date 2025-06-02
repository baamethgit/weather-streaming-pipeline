
CREATE DATABASE weather_db;

-- Se connecter a weather_db 
\c weather_db;


CREATE TABLE weather_raw_10min (
    timestamp TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    wind_deg DOUBLE PRECISION,
    feels_like DOUBLE PRECISION,
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    PRIMARY KEY (timestamp)
);

CREATE INDEX idx_weather_raw_timestamp ON weather_raw_10min(timestamp);


CREATE TABLE weather_hourly (
    timestamp TIMESTAMP NOT NULL,
    temperature_avg DOUBLE PRECISION,
    temperature_min DOUBLE PRECISION,
    temperature_max DOUBLE PRECISION,
    humidity_avg DOUBLE PRECISION,
    pressure_avg DOUBLE PRECISION,
    wind_speed_avg DOUBLE PRECISION,
    feels_like_avg DOUBLE PRECISION,
    weather_main VARCHAR(50),
    PRIMARY KEY (timestamp)
);

CREATE INDEX idx_weather_hourly_timestamp ON weather_hourly(timestamp);


CREATE TABLE weather_daily (
    timestamp DATE NOT NULL,
    temperature_avg DOUBLE PRECISION,
    temperature_min DOUBLE PRECISION,
    temperature_max DOUBLE PRECISION,
    humidity_avg DOUBLE PRECISION,
    pressure_avg DOUBLE PRECISION,
    wind_speed_avg DOUBLE PRECISION,
    feels_like_avg DOUBLE PRECISION,
    weather_main VARCHAR(50),
    PRIMARY KEY (timestamp)
);

CREATE INDEX idx_weather_daily_timestamp ON weather_daily(timestamp);

CREATE TABLE weather_predictions (
    prediction_time TIMESTAMP NOT NULL,
    h_plus_1 DOUBLE PRECISION,
    h_plus_2 DOUBLE PRECISION,
    h_plus_3 DOUBLE PRECISION,
    h_plus_4 DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    PRIMARY KEY (prediction_time)
);

CREATE INDEX idx_weather_predictions_time ON weather_predictions(prediction_time);
CREATE INDEX idx_weather_predictions_hour ON weather_predictions(DATE_TRUNC('hour', prediction_time));


DROP TABLE IF EXISTS weather_alerts CASCADE;

CREATE TABLE weather_alerts (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    component VARCHAR(50) NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved BOOLEAN DEFAULT FALSE
);