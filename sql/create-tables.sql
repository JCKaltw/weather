-- Create the schema
CREATE SCHEMA IF NOT EXISTS weather;

-- Create the weather.weather_location table (master list of locations)
CREATE TABLE IF NOT EXISTS weather.weather_location (
    address VARCHAR(255) PRIMARY KEY
);

-- Create the weather.weather_data table
CREATE TABLE IF NOT EXISTS weather.weather_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,             -- local timezone (tz)
    address VARCHAR(255) NOT NULL,
    temp DECIMAL(5,2),
    tempmin DECIMAL(5,2),
    tempmax DECIMAL(5,2),
    tz VARCHAR,
    FOREIGN KEY (address) REFERENCES weather.weather_location(address) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE (date, address)
);

-- Create the weather.hourly_data table
CREATE TABLE weather.hourly_data (
    id SERIAL PRIMARY KEY,
    weather_data_id INTEGER,
    hour TIMESTAMP,                     -- local timezone
    temp NUMERIC(5, 2),
    FOREIGN KEY (weather_data_id) REFERENCES weather.weather_data(id),
    UNIQUE (weather_data_id, hour)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_weather_data_address ON weather.weather_data(address);
CREATE INDEX IF NOT EXISTS idx_weather_data_date ON weather.weather_data(date);
CREATE INDEX IF NOT EXISTS idx_weather_data_address_date ON weather.weather_data(address, date);
CREATE INDEX IF NOT EXISTS idx_hourly_data_weather_data_id ON weather.hourly_data(weather_data_id);
CREATE INDEX IF NOT EXISTS idx_hourly_data_hour ON weather.hourly_data(hour);

