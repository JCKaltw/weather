-- Create the schema
CREATE SCHEMA IF NOT EXISTS weather;
-- Set the schema
SET search_path TO weather;

-- Create the table
CREATE TABLE weather_data (
    date DATE NOT NULL,
    address VARCHAR(255) NOT NULL,
    temp DECIMAL(5,2),
    tempmin DECIMAL(5,2),
    tempmax DECIMAL(5,2),
    PRIMARY KEY (date, address)
);
