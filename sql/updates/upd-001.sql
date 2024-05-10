SET search_path TO weather;

-- Create the weather.hourly_data table
CREATE TABLE IF NOT EXISTS weather.hourly_data (
    id SERIAL PRIMARY KEY,
    weather_data_id INTEGER,
    hour TIMESTAMP,
    temp NUMERIC,
    tempmin NUMERIC,
    tempmax NUMERIC,
    FOREIGN KEY (weather_data_id) REFERENCES weather.weather_data(id),
    UNIQUE (weather_data_id, hour)
);
