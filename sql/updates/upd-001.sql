ALTER TABLE weather.weather_data ADD COLUMN id SERIAL PRIMARY KEY;

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

ALTER TABLE weather.weather_data DROP CONSTRAINT weather_data_pkey;
ALTER TABLE weather.weather_data ADD PRIMARY KEY (id);
CREATE UNIQUE INDEX weather_data_date_address_idx ON weather.weather_data (date, address);