-- Weather Data Freshness Check for All Locations
-- Run this query to verify your cron job is keeping data current
-- e.g.
-- 10 * * * * . $HOME/.bashrc; cd /home/chris/projects/weather && . ./source-venv.sh && python src/get-weather-data.py --run-for-all-locations --hourly > /tmp/cron-weather-hourly.log 2>&1

WITH location_status AS (
    SELECT 
        wl.address as location,
        -- Most recent daily data
        MAX(wd.date) as latest_daily_date,
        -- Most recent hourly data  
        MAX(h.hour) as latest_hourly_timestamp,
        -- Count of hourly records for the most recent date
        COUNT(CASE WHEN wd.date = (SELECT MAX(date) FROM weather.weather_data wd2 WHERE wd2.address = wl.address) THEN h.id END) as hours_on_latest_date,
        -- Days since last update
        CURRENT_DATE - MAX(wd.date) as days_since_last_update
    FROM weather.weather_location wl
    LEFT JOIN weather.weather_data wd ON wl.address = wd.address
    LEFT JOIN weather.hourly_data h ON wd.id = h.weather_data_id
    GROUP BY wl.address
),
summary AS (
    SELECT 
        location,
        latest_daily_date,
        latest_hourly_timestamp,
        hours_on_latest_date,
        days_since_last_update,
        CASE 
            WHEN latest_daily_date IS NULL THEN 'NO DATA'
            WHEN days_since_last_update = 0 THEN 'CURRENT'
            WHEN days_since_last_update = 1 THEN 'YESTERDAY'  
            WHEN days_since_last_update <= 3 THEN 'RECENT'
            ELSE 'STALE'
        END as data_status,
        CASE 
            WHEN hours_on_latest_date >= 20 THEN 'COMPLETE'
            WHEN hours_on_latest_date >= 15 THEN 'MOSTLY_COMPLETE'
            WHEN hours_on_latest_date >= 5 THEN 'PARTIAL'
            WHEN hours_on_latest_date > 0 THEN 'MINIMAL'
            ELSE 'NO_HOURLY'
        END as hourly_completeness
    FROM location_status
)
SELECT 
    location,
    latest_daily_date,
    latest_hourly_timestamp,
    hours_on_latest_date,
    days_since_last_update,
    data_status,
    hourly_completeness,
    -- Overall health check
    CASE 
        WHEN data_status IN ('CURRENT', 'YESTERDAY') AND hourly_completeness IN ('COMPLETE', 'MOSTLY_COMPLETE') THEN '✅ HEALTHY'
        WHEN data_status IN ('RECENT') THEN '⚠️  AGING'
        WHEN data_status = 'STALE' THEN '❌ STALE'
        ELSE '🔍 NEEDS_ATTENTION'
    END as health_status
FROM summary
ORDER BY 
    CASE data_status 
        WHEN 'NO DATA' THEN 1 
        WHEN 'STALE' THEN 2 
        WHEN 'RECENT' THEN 3 
        WHEN 'YESTERDAY' THEN 4 
        WHEN 'CURRENT' THEN 5 
    END,
    location;

-- Summary statistics
SELECT 
    COUNT(*) as total_locations,
    COUNT(CASE WHEN latest_daily_date IS NOT NULL THEN 1 END) as locations_with_data,
    COUNT(CASE WHEN days_since_last_update <= 1 THEN 1 END) as current_locations,
    COUNT(CASE WHEN days_since_last_update > 3 THEN 1 END) as stale_locations,
    COUNT(CASE WHEN hours_on_latest_date >= 20 THEN 1 END) as locations_with_complete_hourly
FROM location_status;

-- Show any locations that might need attention
SELECT 
    'ATTENTION NEEDED' as alert_type,
    location,
    data_status,
    hourly_completeness,
    days_since_last_update,
    latest_daily_date
FROM summary 
WHERE data_status IN ('NO DATA', 'STALE') 
   OR (data_status = 'RECENT' AND hourly_completeness IN ('MINIMAL', 'NO_HOURLY'))
ORDER BY days_since_last_update DESC;
