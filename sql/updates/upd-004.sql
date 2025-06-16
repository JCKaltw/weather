-- upd-004.sql: Add indexes and foreign key constraint between weather_location and weather_data

\echo 'Starting upd-004.sql - Adding indexes and foreign key constraint'

-- First, add PRIMARY KEY constraint to weather_location if it doesn't exist
\echo 'Adding PRIMARY KEY constraint to weather.weather_location(address)'
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.table_constraints 
        WHERE table_schema = 'weather' 
        AND table_name = 'weather_location' 
        AND constraint_type = 'PRIMARY KEY'
    ) THEN
        ALTER TABLE weather.weather_location ADD PRIMARY KEY (address);
    END IF;
END$$;

-- Now ensure all addresses in weather_data exist in weather_location
\echo 'Inserting missing addresses into weather.weather_location'
INSERT INTO weather.weather_location (address)
SELECT DISTINCT address 
FROM weather.weather_data 
WHERE address NOT IN (SELECT address FROM weather.weather_location)
ORDER BY address;

-- Add foreign key constraint from weather_data to weather_location
\echo 'Adding foreign key constraint fk_weather_data_address'
ALTER TABLE weather.weather_data 
ADD CONSTRAINT fk_weather_data_address 
FOREIGN KEY (address) 
REFERENCES weather.weather_location(address) 
ON DELETE CASCADE 
ON UPDATE CASCADE;

-- Create indexes for better query performance
\echo 'Creating index idx_weather_data_address'
CREATE INDEX IF NOT EXISTS idx_weather_data_address ON weather.weather_data(address);

\echo 'Creating index idx_weather_data_date'
CREATE INDEX IF NOT EXISTS idx_weather_data_date ON weather.weather_data(date);

\echo 'Creating index idx_weather_data_address_date'
CREATE INDEX IF NOT EXISTS idx_weather_data_address_date ON weather.weather_data(address, date);

\echo 'Creating index idx_hourly_data_weather_data_id'
CREATE INDEX IF NOT EXISTS idx_hourly_data_weather_data_id ON weather.hourly_data(weather_data_id);

\echo 'Creating index idx_hourly_data_hour'
CREATE INDEX IF NOT EXISTS idx_hourly_data_hour ON weather.hourly_data(hour);

-- Add NOT NULL constraints where appropriate (if not already present)
\echo 'Adding NOT NULL constraint to weather.hourly_data.weather_data_id'
ALTER TABLE weather.hourly_data ALTER COLUMN weather_data_id SET NOT NULL;

\echo 'Adding NOT NULL constraint to weather.hourly_data.hour'
ALTER TABLE weather.hourly_data ALTER COLUMN hour SET NOT NULL;

-- Verify the foreign key constraint was created
\echo 'Verifying foreign key constraint'
SELECT 
    tc.constraint_name, 
    tc.table_schema,
    tc.table_name, 
    kcu.column_name, 
    ccu.table_schema AS foreign_table_schema,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' 
  AND tc.table_schema = 'weather'
  AND tc.table_name = 'weather_data'
  AND tc.constraint_name = 'fk_weather_data_address';

\echo 'Completed upd-004.sql'
