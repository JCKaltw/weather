\echo 'Dropping table weather.hourly_data if exists'
DROP TABLE IF EXISTS weather.hourly_data;

\echo 'Renaming index weather_data_date_address_idx to old_weather_data_date_address_idx'
ALTER INDEX IF EXISTS weather_data_date_address_idx RENAME TO old_weather_data_date_address_idx;

\echo 'Dropping primary key constraint weather_data_pkey if exists'
ALTER TABLE weather.weather_data DROP CONSTRAINT IF EXISTS weather_data_pkey;

\echo 'Dropping column id from weather.weather_data if exists'
ALTER TABLE weather.weather_data DROP COLUMN IF EXISTS id;

\echo 'Adding column id to weather.weather_data'
ALTER TABLE weather.weather_data ADD COLUMN id SERIAL;

\echo 'Adding primary key constraint on id column'
ALTER TABLE weather.weather_data ADD PRIMARY KEY (id);

\echo 'Creating unique index weather_data_date_address_idx'
CREATE UNIQUE INDEX weather_data_date_address_idx ON weather.weather_data (date, address);

\echo 'Dropping old index old_weather_data_date_address_idx if exists'
DROP INDEX IF EXISTS old_weather_data_date_address_idx;

\echo 'Creating table weather.hourly_data'
CREATE TABLE weather.hourly_data (
    id SERIAL PRIMARY KEY,
    weather_data_id INTEGER,
    hour TIMESTAMP,
    temp NUMERIC,
    tempmin NUMERIC,
    tempmax NUMERIC,
    FOREIGN KEY (weather_data_id) REFERENCES weather.weather_data(id),
    UNIQUE (weather_data_id, hour)
);
