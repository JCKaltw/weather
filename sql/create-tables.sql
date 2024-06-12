-- Create the schema
CREATE SCHEMA IF NOT EXISTS weather;

-- Create the weather.weather_data table
CREATE TABLE IF NOT EXISTS weather.weather_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    address VARCHAR(255) NOT NULL,
    temp DECIMAL(5,2),
    tempmin DECIMAL(5,2),
    tempmax DECIMAL(5,2),
    tz VARCHAR,
    UNIQUE (date, address)
);

-- Create the weather.hourly_data table
CREATE TABLE weather.hourly_data (
    id SERIAL PRIMARY KEY,
    weather_data_id INTEGER,
    hour TIMESTAMP,
    temp NUMERIC(5, 2),
    FOREIGN KEY (weather_data_id) REFERENCES weather.weather_data(id),
    UNIQUE (weather_data_id, hour)
);
