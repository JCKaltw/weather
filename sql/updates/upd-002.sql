ALTER TABLE weather.hourly_data
    DROP COLUMN IF EXISTS tempmin,
    DROP COLUMN IF EXISTS tempmax;