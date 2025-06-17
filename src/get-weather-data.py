# File: /home/chris/projects/weather/src/get-weather-data.py
#
# TIMEZONE HANDLING STRATEGY:
# - Date calculations use UTC time for consistency across all locations
# - Visual Crossing API interprets date parameters in each location's local timezone
# - Daily data: stored as YYYY-MM-DD dates + timezone string in 'tz' column  
# - Hourly data: stored as local timestamps (YYYY-MM-DD HH:MM:SS) in location's timezone
# - To interpret hourly data, always reference the corresponding weather_data.tz value
#
# DATABASE SCHEMA:
# The following tables are expected to exist in the database:
#
# CREATE TABLE weather.weather_location (
#     address VARCHAR(255) PRIMARY KEY,
#     dirty BOOLEAN DEFAULT FALSE
# );
#
# CREATE TABLE weather.weather_data (
#     id SERIAL PRIMARY KEY,
#     date DATE NOT NULL,
#     address VARCHAR(255) NOT NULL,
#     temp DECIMAL(5,2),
#     tempmin DECIMAL(5,2),
#     tempmax DECIMAL(5,2),
#     tz VARCHAR,
#     FOREIGN KEY (address) REFERENCES weather.weather_location(address) ON DELETE CASCADE ON UPDATE CASCADE,
#     UNIQUE (date, address)
# );
#
# CREATE TABLE weather.hourly_data (
#     id SERIAL PRIMARY KEY,
#     weather_data_id INTEGER NOT NULL,
#     hour TIMESTAMP NOT NULL,
#     temp NUMERIC(5, 2),
#     FOREIGN KEY (weather_data_id) REFERENCES weather.weather_data(id) ON DELETE CASCADE,
#     UNIQUE (weather_data_id, hour)
# );
#
# Note: The weather_location table serves as a master list of valid weather addresses.
# The foreign key constraint ensures data integrity - weather data can only be inserted
# for addresses that exist in weather_location. The script automatically adds new
# addresses to weather_location as needed.

import argparse
import requests
import psycopg2
import os
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from psycopg2 import IntegrityError
import pytz
from collections import defaultdict


def read_api_key():
    try:
        with open('apikey/visual-crossing-apikey', 'r') as file:
            api_key = file.read().strip()
            return api_key
    except IOError:
        print("Error reading the API key file.")
        return None


def fetch_weather_data(address, start_date, end_date, api_key, include_hourly=False):
    encoded_address = quote(address)
    include_param = "days,hours,timezone" if include_hourly else "days,timezone"

    if include_hourly:
        # Split the date range into smaller intervals
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        delta = end_date_obj - start_date_obj
        total_days = delta.days + 1

        print(f"Fetching hourly weather data for {total_days} days...")

        weather_data = []
        for i in range(total_days):
            request_date = start_date_obj + timedelta(days=i)
            request_date_str = request_date.strftime("%Y-%m-%d")
            url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{encoded_address}/{request_date_str}/{request_date_str}?unitGroup=us&include={include_param}&key={api_key}&contentType=json"

            print(f"Fetching data for {request_date_str}...")

            try:
                response = requests.get(url)
                response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
                weather_data.extend(response.json()['days'])

                print(f"Data fetched successfully for {request_date_str}")
            except requests.exceptions.RequestException as e:
                print(
                    f"Error occurred while fetching weather data for {request_date_str}: {e}")
                return None
            except requests.exceptions.JSONDecodeError as e:
                print(
                    f"Error decoding JSON response for {request_date_str}: {e}")
                print("Response content:", response.text)
                return None

        print("Hourly weather data fetched successfully")
        return {'days': weather_data, 'timezone': response.json()['timezone']}
    else:
        encoded_start_date = quote(start_date)
        encoded_end_date = quote(end_date)
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{encoded_address}/{encoded_start_date}/{encoded_end_date}?unitGroup=us&include={include_param}&key={api_key}&contentType=json"

        print(f"Fetching daily weather data for {start_date} to {end_date}...")

        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes

            print("Daily weather data fetched successfully")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while fetching weather data: {e}")
            return None
        except requests.exceptions.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")
            print("Response content:", response.text)
            return None


def ensure_location_exists(db_conn, address):
    """
    Ensure that the given address exists in the weather.weather_location table.
    If it doesn't exist, insert it.
    
    Args:
        db_conn: Database connection
        address: Weather location address
    """
    if not db_conn:
        return
        
    with db_conn.cursor() as cur:
        # Check if address exists in weather_location
        cur.execute("SELECT 1 FROM weather.weather_location WHERE address = %s", (address,))
        if not cur.fetchone():
            # Insert the address into weather_location if it doesn't exist
            print(f"Adding new location '{address}' to weather.weather_location")
            cur.execute("INSERT INTO weather.weather_location (address) VALUES (%s)", (address,))
            db_conn.commit()


def insert_weather_data(db_conn, address, weather_data, include_hourly=False, dry_run=False):
    timezone = weather_data['timezone']
    
    # Ensure weather_location entry exists before inserting weather data
    if not dry_run:
        ensure_location_exists(db_conn, address)
    
    # Get the timezone object for comparing times
    try:
        local_tz = pytz.timezone(timezone)
        current_time_local = datetime.now(local_tz)
    except pytz.exceptions.UnknownTimeZoneError:
        # If timezone is invalid, we'll insert all data as before
        local_tz = None
        current_time_local = None
    
    for day in weather_data['days']:
        sql_daily = "INSERT INTO weather.weather_data (date, address, temp, tempmin, tempmax, tz) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (date, address) DO UPDATE SET temp = EXCLUDED.temp, tempmin = EXCLUDED.tempmin, tempmax = EXCLUDED.tempmax, tz = EXCLUDED.tz RETURNING id;"
        params_daily = (day['datetime'], address, day['temp'],
                        day['tempmin'], day['tempmax'], timezone)

        if dry_run:
            print("Dry run mode: SQL queries that would be executed:")
            print("-- Ensure location exists:")
            print(f"SELECT 1 FROM weather.weather_location WHERE address = '{address}';")
            print(f"INSERT INTO weather.weather_location (address) VALUES ('{address}'); -- if not exists")
            print("-- Insert weather data:")
            print(sql_daily % params_daily)
        else:
            try:
                with db_conn.cursor() as cur:
                    cur.execute(sql_daily, params_daily)
                    weather_data_id = cur.fetchone()[0]

                    if include_hourly and 'hours' in day:
                        skipped_future_hours = 0
                        for hour in day['hours']:
                            # CRITICAL: Visual Crossing API returns hourly data in LOCAL TIME
                            # - day['datetime'] = "2025-06-11" (date in local timezone)  
                            # - hour['datetime'] = "14:00:00" (time in local timezone)
                            # - Combined: "2025-06-11 14:00:00" = LOCAL TIME for this location
                            # - The timezone is stored separately in weather_data.tz column
                            # This ensures proper alignment with device data in the same local timezone
                            timestamp = f"{day['datetime']} {hour['datetime']}"
                            
                            # Skip future hours (only if we have a valid timezone)
                            if current_time_local and local_tz:
                                # Parse the timestamp in the local timezone
                                hour_datetime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                                hour_datetime_local = local_tz.localize(hour_datetime)
                                
                                # Skip future hours entirely
                                if hour_datetime_local > current_time_local:
                                    skipped_future_hours += 1
                                    continue
                            
                            sql_hourly = "INSERT INTO weather.hourly_data (weather_data_id, hour, temp) VALUES (%s, %s, %s) ON CONFLICT (weather_data_id, hour) DO UPDATE SET temp = EXCLUDED.temp;"
                            params_hourly = (
                                weather_data_id, timestamp, hour['temp'])
                            cur.execute(sql_hourly, params_hourly)
                        
                        if skipped_future_hours > 0 and not dry_run:
                            print(f"  Skipped {skipped_future_hours} future hours for {day['datetime']}")

            except IntegrityError as e:
                print(
                    f"Duplicate entry for {day['datetime']} at {address}. Skipping.")
                db_conn.rollback()
            else:
                db_conn.commit()


def calculate_yesterday(tz_offset=None):
    """
    Legacy function for calculating yesterday's date.
    Used by single-location operations for backward compatibility.
    
    For --run-for-all-locations, use calculate_previous_24_hours() instead
    which provides consistent UTC-based date calculation.
    """
    if tz_offset is None:
        return (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    else:
        tz_delta = timedelta(hours=tz_offset)
        return (datetime.now(timezone.utc) + tz_delta - timedelta(1)).strftime('%Y-%m-%d')


def calculate_previous_24_hours():
    """
    Calculate the date range for the previous 24 hours starting from the last completed hour.
    Uses UTC time to ensure consistency across all locations.
    Returns a tuple of (start_date, end_date) in YYYY-MM-DD format.
    
    Note: The Visual Crossing API will interpret these dates in each location's 
    local timezone, which is the desired behavior.
    """
    # Use UTC time for consistency across all locations
    now_utc = datetime.now(timezone.utc)
    # Go back to the last completed hour (truncate minutes and seconds)
    last_completed_hour_utc = now_utc.replace(minute=0, second=0, microsecond=0)
    # Calculate 24 hours before that
    start_time_utc = last_completed_hour_utc - timedelta(hours=24)
    
    # Convert to date strings
    start_date = start_time_utc.strftime('%Y-%m-%d')
    end_date = last_completed_hour_utc.strftime('%Y-%m-%d')
    
    return start_date, end_date


def get_all_locations(db_conn):
    """
    Retrieve all addresses from the weather.weather_location table.
    Returns a list of address strings.
    """
    with db_conn.cursor() as cur:
        cur.execute("SELECT address FROM weather.weather_location ORDER BY address")
        locations = [row[0] for row in cur.fetchall()]
    return locations


def get_all_display_group_ids(db_conn, include_inactive=False):
    """
    Retrieve all display group IDs from the display_group table.
    Returns a list of display group ID integers.
    
    Args:
        include_inactive: If False (default), only return active display groups.
                         If True, return all display groups regardless of active status.
    """
    with db_conn.cursor() as cur:
        if include_inactive:
            cur.execute("""
                SELECT display_group_id 
                FROM display_group 
                WHERE is_removed = false OR is_removed IS NULL
                ORDER BY display_group_id
            """)
        else:
            cur.execute("""
                SELECT display_group_id 
                FROM display_group 
                WHERE active = true 
                AND (is_removed = false OR is_removed IS NULL)
                ORDER BY display_group_id
            """)
        display_group_ids = [row[0] for row in cur.fetchall()]
    return display_group_ids

#---------- BEGIN NEW ------
def get_active_weather_addresses(db_conn, include_inactive=False):
    """
    Retrieve weather addresses that have display groups needing recent weather data.

    A display group needs recent data if:
    1. test_end_date is None (ongoing test), OR
    2. test_end_date + day_start_seconds in local timezone is within the last 48 hours

    Args:
        include_inactive: If False (default), only consider active display groups.
                         If True, consider all display groups regardless of active status.

    Returns a list of unique weather addresses that need recent data.
    """
    with db_conn.cursor() as cur:
        if include_inactive:
            cur.execute("""
                SELECT DISTINCT test_start_date, test_end_date, day_start_seconds, timezone, weather_address
                FROM display_group
                WHERE weather_address IS NOT NULL
                AND test_start_date IS NOT NULL
                AND (is_removed = false OR is_removed IS NULL)
                ORDER BY weather_address
            """)
        else:
            cur.execute("""
                SELECT DISTINCT test_start_date, test_end_date, day_start_seconds, timezone, weather_address
                FROM display_group
                WHERE weather_address IS NOT NULL
                AND test_start_date IS NOT NULL
                AND active = true
                AND (is_removed = false OR is_removed IS NULL)
                ORDER BY weather_address
            """)

        display_groups = cur.fetchall()

    active_addresses = set()
    now_utc = datetime.now(timezone.utc)
    cutoff_hours = 48  # Consider display groups active if they ended within this many hours

    print(f"Analyzing {len(display_groups)} display groups to determine active weather addresses...")

    for test_start_date, test_end_date, day_start_seconds, tz_name, weather_address in display_groups:
        try:
            # Parse timezone
            local_tz = pytz.timezone(tz_name)

            # If test_end_date is None, the test is ongoing and needs recent data
            if test_end_date is None:
                active_addresses.add(weather_address)
                print(f"  Active (ongoing): {weather_address}")
                continue

            # Compute the actual end time: test_end_date + day_start_seconds in local timezone
            end_datetime = datetime.combine(test_end_date, datetime.min.time())
            end_datetime += timedelta(seconds=day_start_seconds)
            end_datetime_local = local_tz.localize(end_datetime)

            # Convert to UTC for comparison
            end_datetime_utc = end_datetime_local.astimezone(timezone.utc)

            # Check if the test ended within the cutoff period
            hours_since_end = (now_utc - end_datetime_utc).total_seconds() / 3600

            if hours_since_end <= cutoff_hours:
                active_addresses.add(weather_address)
                print(f"  Active (ended {hours_since_end:.1f}h ago): {weather_address}")
            else:
                print(f"  Inactive (ended {hours_since_end:.1f}h ago): {weather_address}")

        except pytz.exceptions.UnknownTimeZoneError:
            print(f"  Skipping (invalid timezone '{tz_name}'): {weather_address}")
            continue

    return sorted(list(active_addresses))
#---------- END NEW ------

def get_dirty_weather_addresses(db_conn):
    """
    Retrieve all weather addresses that are marked as dirty.
    
    Returns a list of weather addresses where dirty = true.
    """
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT address 
            FROM weather.weather_location 
            WHERE dirty = true
            ORDER BY address
        """)
        addresses = [row[0] for row in cur.fetchall()]
    return addresses


def mark_address_as_clean(db_conn, address):
    """
    Mark a weather address as clean (dirty = false).
    
    Args:
        db_conn: Database connection
        address: Weather address to mark as clean
    """
    with db_conn.cursor() as cur:
        cur.execute("""
            UPDATE weather.weather_location 
            SET dirty = false 
            WHERE address = %s
        """, (address,))
        db_conn.commit()


def check_missing_hours_for_display_group(db_conn, display_group_id):
    """
    Check if a specific display group has missing hourly weather data.
    Returns True if missing hours are found, False otherwise.
    
    Uses similar logic to update_for_display_group_id but only checks without fetching/inserting.
    """
    with db_conn.cursor() as cur:
        # Get display group parameters
        cur.execute("""
            SELECT test_start_date, test_end_date, day_start_seconds, timezone, weather_address
            FROM display_group 
            WHERE display_group_id = %s
            AND (is_removed = false OR is_removed IS NULL)
        """, (display_group_id,))
        
        row = cur.fetchone()
        if row is None:
            return False  # Display group not found or is_removed
        
        test_start_date, test_end_date, day_start_seconds, tz_name, weather_address = row
        
        if not weather_address:
            return False  # No weather address configured
        
        if not test_start_date:
            return False  # No test start date configured
        
        # Parse timezone
        try:
            local_tz = pytz.timezone(tz_name)
        except pytz.exceptions.UnknownTimeZoneError:
            return False  # Invalid timezone
        
        # Compute start time: test_start_date + day_start_seconds in local timezone
        start_datetime = datetime.combine(test_start_date, datetime.min.time())
        start_datetime += timedelta(seconds=day_start_seconds)
        start_datetime_local = local_tz.localize(start_datetime)
        
        # Compute end time
        if test_end_date:
            # Use test_end_date + day_start_seconds
            end_datetime = datetime.combine(test_end_date, datetime.min.time())
            end_datetime += timedelta(seconds=day_start_seconds)
            end_datetime_local = local_tz.localize(end_datetime)
        else:
            # Use current time minus 1 hour (last completed hour)
            now_local = datetime.now(local_tz)
            # Round down to the last completed hour
            end_datetime_local = now_local.replace(minute=0, second=0, microsecond=0)
        
        # Check for missing daily data first
        current_date = start_datetime_local.date()
        end_date = end_datetime_local.date()
        
        while current_date <= end_date:
            cur.execute("""
                SELECT COUNT(*) FROM weather.weather_data 
                WHERE address = %s AND date = %s
            """, (weather_address, current_date))
            
            count = cur.fetchone()[0]
            if count == 0:
                return True  # Missing daily data
            
            current_date += timedelta(days=1)
        
        # Check for missing hourly data in the specified time range
        cur.execute("""
            SELECT w.id, w.date 
            FROM weather.weather_data w
            WHERE w.address = %s 
            AND w.date >= %s 
            AND w.date <= %s
            ORDER BY w.date
        """, (weather_address, start_datetime_local.date(), end_datetime_local.date()))
        
        weather_data_records = cur.fetchall()
        
        for weather_data_id, date in weather_data_records:
            # Generate expected hours for this date within our time range
            date_start = datetime.combine(date, datetime.min.time())
            date_start_local = local_tz.localize(date_start)
            
            # Find the overlap between this date and our target range
            day_start = max(date_start_local, start_datetime_local)
            day_end = min(date_start_local + timedelta(days=1), end_datetime_local + timedelta(hours=1))
            
            if day_start >= day_end:
                continue  # No overlap
            
            # Generate expected hours
            current_hour = day_start.replace(minute=0, second=0, microsecond=0)
            while current_hour < day_end:
                # Check if this hour exists in hourly_data
                # Convert timezone-aware datetime to naive for database comparison
                naive_hour = current_hour.replace(tzinfo=None)
                cur.execute("""
                    SELECT COUNT(*) FROM weather.hourly_data 
                    WHERE weather_data_id = %s AND hour = %s
                """, (weather_data_id, naive_hour))
                
                count = cur.fetchone()[0]
                if count == 0:
                    return True  # Found missing hourly data
                
                current_hour += timedelta(hours=1)
        
        return False  # No missing hours found


def list_display_group_ids_missing_hours(db_conn, include_inactive=False):
    """
    Scan all display group IDs and identify those with missing hourly weather data.
    Returns a list of display group IDs that have missing hours.
    
    Args:
        include_inactive: If False (default), only scan active display groups.
                         If True, scan all display groups regardless of active status.
    """
    # Get all display group IDs
    display_group_ids = get_all_display_group_ids(db_conn, include_inactive=include_inactive)
    missing_hours_group_ids = []
    
    print(f"Scanning {len(display_group_ids)} {'display groups' if include_inactive else 'active display groups'} for missing hourly weather data...")
    
    for i, display_group_id in enumerate(display_group_ids, 1):
        print(f"[{i}/{len(display_group_ids)}] Checking display group {display_group_id}...")
        
        if check_missing_hours_for_display_group(db_conn, display_group_id):
            missing_hours_group_ids.append(display_group_id)
            print(f"  -> Display group {display_group_id} has missing hours")
        else:
            print(f"  -> Display group {display_group_id} is complete")
    
    return missing_hours_group_ids


def get_missing_hours_for_display_group(db_conn, display_group_id):
    """
    Get the specific missing hour timestamps for a display group.
    Returns a list of (weather_address, missing_hour_timestamp) tuples.
    
    Uses similar logic to check_missing_hours_for_display_group but collects the actual missing hours.
    """
    with db_conn.cursor() as cur:
        # Get display group parameters
        cur.execute("""
            SELECT test_start_date, test_end_date, day_start_seconds, timezone, weather_address
            FROM display_group 
            WHERE display_group_id = %s
            AND (is_removed = false OR is_removed IS NULL)
        """, (display_group_id,))
        
        row = cur.fetchone()
        if row is None:
            return []  # Display group not found or is_removed
        
        test_start_date, test_end_date, day_start_seconds, tz_name, weather_address = row
        
        if not weather_address or not test_start_date:
            return []  # No weather address or test start date configured
        
        # Parse timezone
        try:
            local_tz = pytz.timezone(tz_name)
        except pytz.exceptions.UnknownTimeZoneError:
            return []  # Invalid timezone
        
        # Compute start time: test_start_date + day_start_seconds in local timezone
        start_datetime = datetime.combine(test_start_date, datetime.min.time())
        start_datetime += timedelta(seconds=day_start_seconds)
        start_datetime_local = local_tz.localize(start_datetime)
        
        # Compute end time
        if test_end_date:
            # Use test_end_date + day_start_seconds
            end_datetime = datetime.combine(test_end_date, datetime.min.time())
            end_datetime += timedelta(seconds=day_start_seconds)
            end_datetime_local = local_tz.localize(end_datetime)
        else:
            # Use current time minus 1 hour (last completed hour)
            now_local = datetime.now(local_tz)
            # Round down to the last completed hour
            end_datetime_local = now_local.replace(minute=0, second=0, microsecond=0)
        
        missing_hours = []
        
        # Check for missing daily data first
        current_date = start_datetime_local.date()
        end_date = end_datetime_local.date()
        
        missing_dates = []
        while current_date <= end_date:
            cur.execute("""
                SELECT COUNT(*) FROM weather.weather_data 
                WHERE address = %s AND date = %s
            """, (weather_address, current_date))
            
            count = cur.fetchone()[0]
            if count == 0:
                missing_dates.append(current_date)
            
            current_date += timedelta(days=1)
        
        # For missing daily data, add all hours in the expected range for those dates
        for missing_date in missing_dates:
            date_start = datetime.combine(missing_date, datetime.min.time())
            date_start_local = local_tz.localize(date_start)
            
            # Find the overlap between this date and our target range
            day_start = max(date_start_local, start_datetime_local)
            day_end = min(date_start_local + timedelta(days=1), end_datetime_local + timedelta(hours=1))
            
            if day_start < day_end:
                # Generate missing hours for this date
                current_hour = day_start.replace(minute=0, second=0, microsecond=0)
                while current_hour < day_end:
                    missing_hours.append((weather_address, current_hour))
                    current_hour += timedelta(hours=1)
        
        # Check for missing hourly data in the specified time range for dates that have daily data
        cur.execute("""
            SELECT w.id, w.date 
            FROM weather.weather_data w
            WHERE w.address = %s 
            AND w.date >= %s 
            AND w.date <= %s
            ORDER BY w.date
        """, (weather_address, start_datetime_local.date(), end_datetime_local.date()))
        
        weather_data_records = cur.fetchall()
        
        for weather_data_id, date in weather_data_records:
            # Skip dates that we already identified as completely missing
            if date in missing_dates:
                continue
                
            # Generate expected hours for this date within our time range
            date_start = datetime.combine(date, datetime.min.time())
            date_start_local = local_tz.localize(date_start)
            
            # Find the overlap between this date and our target range
            day_start = max(date_start_local, start_datetime_local)
            day_end = min(date_start_local + timedelta(days=1), end_datetime_local + timedelta(hours=1))
            
            if day_start >= day_end:
                continue  # No overlap
            
            # Generate expected hours
            current_hour = day_start.replace(minute=0, second=0, microsecond=0)
            while current_hour < day_end:
                # Check if this hour exists in hourly_data
                # Convert timezone-aware datetime to naive for database comparison
                naive_hour = current_hour.replace(tzinfo=None)
                cur.execute("""
                    SELECT COUNT(*) FROM weather.hourly_data 
                    WHERE weather_data_id = %s AND hour = %s
                """, (weather_data_id, naive_hour))
                
                count = cur.fetchone()[0]
                if count == 0:
                    missing_hours.append((weather_address, current_hour))
                
                current_hour += timedelta(hours=1)
        
        return missing_hours


def merge_time_ranges(timestamps):
    """
    Merge consecutive and overlapping timestamps into consolidated ranges.
    
    Args:
        timestamps: List of datetime objects
    
    Returns:
        List of (start_time, end_time) tuples representing merged ranges
    """
    if not timestamps:
        return []
    
    # Sort timestamps
    sorted_times = sorted(timestamps)
    
    ranges = []
    range_start = sorted_times[0]
    range_end = sorted_times[0]
    
    for i in range(1, len(sorted_times)):
        current_time = sorted_times[i]
        
        # Check if current timestamp is consecutive (1 hour after the previous)
        if current_time == range_end + timedelta(hours=1):
            # Extend the current range
            range_end = current_time
        else:
            # Gap found, close current range and start a new one
            ranges.append((range_start, range_end))
            range_start = current_time
            range_end = current_time
    
    # Add the final range
    ranges.append((range_start, range_end))
    
    return ranges


def fetch_missing_hours_for_addresses(db_conn, api_key, addresses_with_ranges, dry_run=False, max_days_per_request=None):
    """
    Fetch and insert missing hourly weather data for specified addresses and date ranges.
    
    Args:
        db_conn: Database connection
        api_key: Visual Crossing API key
        addresses_with_ranges: Dict mapping addresses to lists of (start_date, end_date) tuples
        dry_run: If True, show what would be done without executing
        max_days_per_request: Maximum days to fetch in a single request (None = no limit)
    """
    total_addresses = len(addresses_with_ranges)
    total_ranges_processed = 0
    total_days_fetched = 0
    
    for address_idx, (address, date_ranges) in enumerate(addresses_with_ranges.items(), 1):
        print(f"\n[{address_idx}/{total_addresses}] Processing missing hours for: {address}")
        print(f"  Total date ranges to fetch: {len(date_ranges)}")
        
        for range_idx, (start_date, end_date) in enumerate(date_ranges, 1):
            print(f"  [{range_idx}/{len(date_ranges)}] Date range: {start_date} to {end_date}")
            
            # Calculate the number of days in this range
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            days_in_range = (end_date_obj - start_date_obj).days + 1
            
            print(f"    Days to fetch: {days_in_range}")
            
            # If max_days_per_request is None or the range is within limit, fetch all at once
            if max_days_per_request is None or days_in_range <= max_days_per_request:
                if dry_run:
                    print(f"    [DRY RUN] Would fetch: {start_date} to {end_date}")
                else:
                    print(f"    Fetching: {start_date} to {end_date}")
                    
                    # Fetch weather data with hourly information
                    weather_data = fetch_weather_data(
                        address, start_date, end_date, api_key, include_hourly=True)
                    
                    if weather_data is None:
                        print(f"    ERROR: Failed to fetch weather data for {start_date} to {end_date}")
                    else:
                        # Insert the weather data
                        insert_weather_data(db_conn, address, weather_data, include_hourly=True)
                        total_days_fetched += days_in_range
                        print(f"    Successfully inserted hourly data for {days_in_range} days")
            else:
                # Split large date ranges into smaller chunks
                current_start = start_date_obj
                while current_start <= end_date_obj:
                    # Calculate chunk end date
                    chunk_end = min(current_start + timedelta(days=max_days_per_request - 1), end_date_obj)
                    
                    chunk_start_str = current_start.strftime("%Y-%m-%d")
                    chunk_end_str = chunk_end.strftime("%Y-%m-%d")
                    
                    if dry_run:
                        print(f"    [DRY RUN] Would fetch: {chunk_start_str} to {chunk_end_str}")
                    else:
                        print(f"    Fetching: {chunk_start_str} to {chunk_end_str}")
                        
                        # Fetch weather data with hourly information
                        weather_data = fetch_weather_data(
                            address, chunk_start_str, chunk_end_str, api_key, include_hourly=True)
                        
                        if weather_data is None:
                            print(f"    ERROR: Failed to fetch weather data for {chunk_start_str} to {chunk_end_str}")
                            # Continue with next chunk despite error
                        else:
                            # Insert the weather data
                            insert_weather_data(db_conn, address, weather_data, include_hourly=True)
                            chunk_days = (chunk_end - current_start).days + 1
                            total_days_fetched += chunk_days
                            print(f"    Successfully inserted hourly data for {chunk_days} days")
                    
                    # Move to next chunk
                    current_start = chunk_end + timedelta(days=1)
            
            total_ranges_processed += 1
    
    # Summary
    print("\n" + "="*80)
    print("FETCH MISSING HOURS SUMMARY")
    print("="*80)
    print(f"Addresses processed: {total_addresses}")
    print(f"Date ranges processed: {total_ranges_processed}")
    if not dry_run:
        print(f"Total days fetched: {total_days_fetched}")
    else:
        print("DRY RUN - No data was actually fetched")


def collect_missing_hours_by_address_optimized(db_conn, include_inactive=False):
    """
    Collect all missing hours organized by address with consolidated date ranges.
    
    This is an OPTIMIZED version that:
    1. Groups display groups by weather address first
    2. Computes consolidated time ranges for each address
    3. Checks for missing data only in the merged ranges
    
    Args:
        db_conn: Database connection
        include_inactive: If False (default), only consider active display groups.
                         If True, consider all display groups regardless of active status.
    
    Returns:
        Dict mapping addresses to lists of (start_date, end_date) tuples
    """
    print("Collecting missing hours by address (optimized)...")
    
    # Step 1: Get all display groups grouped by weather address
    with db_conn.cursor() as cur:
        if include_inactive:
            cur.execute("""
                SELECT display_group_id, test_start_date, test_end_date, 
                       day_start_seconds, timezone, weather_address
                FROM display_group
                WHERE weather_address IS NOT NULL
                AND test_start_date IS NOT NULL
                AND (is_removed = false OR is_removed IS NULL)
                ORDER BY weather_address, display_group_id
            """)
        else:
            cur.execute("""
                SELECT display_group_id, test_start_date, test_end_date, 
                       day_start_seconds, timezone, weather_address
                FROM display_group
                WHERE weather_address IS NOT NULL
                AND test_start_date IS NOT NULL
                AND active = true
                AND (is_removed = false OR is_removed IS NULL)
                ORDER BY weather_address, display_group_id
            """)
        
        all_display_groups = cur.fetchall()
    
    # Group display groups by weather address
    groups_by_address = defaultdict(list)
    for row in all_display_groups:
        display_group_id, test_start_date, test_end_date, day_start_seconds, tz_name, weather_address = row
        groups_by_address[weather_address].append({
            'display_group_id': display_group_id,
            'test_start_date': test_start_date,
            'test_end_date': test_end_date,
            'day_start_seconds': day_start_seconds,
            'timezone': tz_name
        })
    
    print(f"Found {len(groups_by_address)} unique weather addresses across {len(all_display_groups)} {'display groups' if include_inactive else 'active display groups'}")
    
    # Step 2: For each address, compute consolidated time ranges
    addresses_with_ranges = {}
    
    for address_idx, (weather_address, display_groups) in enumerate(groups_by_address.items(), 1):
        print(f"\n[{address_idx}/{len(groups_by_address)}] Processing address: {weather_address}")
        print(f"  Display groups at this address: {len(display_groups)}")
        
        # Collect all time ranges needed for this address
        time_ranges = []
        
        for dg in display_groups:
            try:
                # Parse timezone
                local_tz = pytz.timezone(dg['timezone'])
                
                # Compute start time: test_start_date + day_start_seconds in local timezone
                start_datetime = datetime.combine(dg['test_start_date'], datetime.min.time())
                start_datetime += timedelta(seconds=dg['day_start_seconds'])
                start_datetime_local = local_tz.localize(start_datetime)
                
                # Compute end time
                if dg['test_end_date']:
                    # Use test_end_date + day_start_seconds
                    end_datetime = datetime.combine(dg['test_end_date'], datetime.min.time())
                    end_datetime += timedelta(seconds=dg['day_start_seconds'])
                    end_datetime_local = local_tz.localize(end_datetime)
                else:
                    # Use current time minus 1 hour (last completed hour)
                    now_local = datetime.now(local_tz)
                    # Round down to the last completed hour
                    end_datetime_local = now_local.replace(minute=0, second=0, microsecond=0)
                
                time_ranges.append((start_datetime_local, end_datetime_local))
                
            except pytz.exceptions.UnknownTimeZoneError:
                print(f"    Skipping display group {dg['display_group_id']} - invalid timezone: {dg['timezone']}")
                continue
        
        if not time_ranges:
            print(f"  No valid time ranges for {weather_address}")
            continue
        
        # Merge overlapping time ranges
        print(f"  Merging {len(time_ranges)} time ranges...")
        merged_ranges = merge_datetime_ranges(time_ranges)
        print(f"  Consolidated to {len(merged_ranges)} non-overlapping ranges")
        
        # Step 3: Check for missing data only in the merged ranges
        missing_hours = []
        
        with db_conn.cursor() as cur:
            for range_start, range_end in merged_ranges:
                # Check for missing daily data first
                current_date = range_start.date()
                end_date = range_end.date()
                
                while current_date <= end_date:
                    cur.execute("""
                        SELECT COUNT(*) FROM weather.weather_data 
                        WHERE address = %s AND date = %s
                    """, (weather_address, current_date))
                    
                    count = cur.fetchone()[0]
                    if count == 0:
                        # Add all hours for this missing date that fall within our range
                        date_start = datetime.combine(current_date, datetime.min.time())
                        date_start_local = range_start.tzinfo.localize(date_start)
                        
                        day_start = max(date_start_local, range_start)
                        day_end = min(date_start_local + timedelta(days=1), range_end + timedelta(hours=1))
                        
                        if day_start < day_end:
                            current_hour = day_start.replace(minute=0, second=0, microsecond=0)
                            while current_hour < day_end:
                                missing_hours.append(current_hour)
                                current_hour += timedelta(hours=1)
                    
                    current_date += timedelta(days=1)
                
                # Check for missing hourly data
                cur.execute("""
                    SELECT w.id, w.date 
                    FROM weather.weather_data w
                    WHERE w.address = %s 
                    AND w.date >= %s 
                    AND w.date <= %s
                    ORDER BY w.date
                """, (weather_address, range_start.date(), range_end.date()))
                
                weather_data_records = cur.fetchall()
                
                for weather_data_id, date in weather_data_records:
                    # Generate expected hours for this date within our range
                    date_start = datetime.combine(date, datetime.min.time())
                    date_start_local = range_start.tzinfo.localize(date_start)
                    
                    day_start = max(date_start_local, range_start)
                    day_end = min(date_start_local + timedelta(days=1), range_end + timedelta(hours=1))
                    
                    if day_start >= day_end:
                        continue
                    
                    # Check each hour
                    current_hour = day_start.replace(minute=0, second=0, microsecond=0)
                    while current_hour < day_end:
                        naive_hour = current_hour.replace(tzinfo=None)
                        cur.execute("""
                            SELECT COUNT(*) FROM weather.hourly_data 
                            WHERE weather_data_id = %s AND hour = %s
                        """, (weather_data_id, naive_hour))
                        
                        count = cur.fetchone()[0]
                        if count == 0:
                            missing_hours.append(current_hour)
                        
                        current_hour += timedelta(hours=1)
        
        if missing_hours:
            print(f"  Found {len(missing_hours)} missing hours")
            
            # Convert missing hours to date ranges
            dates_needed = sorted(set(hour.date() for hour in missing_hours))
            
            # Merge consecutive dates into ranges
            date_ranges = []
            if dates_needed:
                range_start = dates_needed[0]
                range_end = dates_needed[0]
                
                for i in range(1, len(dates_needed)):
                    current_date = dates_needed[i]
                    
                    if current_date == range_end + timedelta(days=1):
                        range_end = current_date
                    else:
                        date_ranges.append((range_start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")))
                        range_start = current_date
                        range_end = current_date
                
                date_ranges.append((range_start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")))
            
            addresses_with_ranges[weather_address] = date_ranges
            print(f"  Consolidated to {len(date_ranges)} date ranges for API fetching")
        else:
            print(f"  No missing hours found")
    
    return addresses_with_ranges


def merge_datetime_ranges(ranges):
    """
    Merge overlapping datetime ranges into non-overlapping ranges.
    
    Args:
        ranges: List of (start_datetime, end_datetime) tuples
    
    Returns:
        List of merged (start_datetime, end_datetime) tuples
    """
    if not ranges:
        return []
    
    # Sort ranges by start time
    sorted_ranges = sorted(ranges, key=lambda x: x[0])
    
    merged = []
    current_start, current_end = sorted_ranges[0]
    
    for start, end in sorted_ranges[1:]:
        if start <= current_end + timedelta(hours=1):
            # Ranges overlap or are adjacent, merge them
            current_end = max(current_end, end)
        else:
            # No overlap, save current range and start a new one
            merged.append((current_start, current_end))
            current_start, current_end = start, end
    
    # Add the final range
    merged.append((current_start, current_end))
    
    return merged


def collect_missing_hours_by_address(db_conn, include_inactive=False):
    """
    Collect all missing hours organized by address with consolidated date ranges.
    
    This is a refactored version of report_missing_hours_by_address that returns
    the data structure instead of printing a report.
    
    NOTE: This function now calls the optimized version for better performance.
    
    Args:
        db_conn: Database connection
        include_inactive: If False (default), only consider active display groups.
                         If True, consider all display groups regardless of active status.
    
    Returns:
        Dict mapping addresses to lists of (start_date, end_date) tuples
    """
    # Use the optimized version
    return collect_missing_hours_by_address_optimized(db_conn, include_inactive)


def report_missing_hours_by_address(db_conn, output_json_path=None, include_inactive=False):
    """
    Generate a comprehensive report of missing hourly weather data by address.
    
    Scans all display groups, collects missing hours by weather address,
    and consolidates overlapping time ranges into minimal sets of ranges.
    
    Args:
        db_conn: Database connection
        output_json_path: Optional path to output JSON file with structured missing hours data
        include_inactive: If False (default), only report on active display groups.
                         If True, report on all display groups regardless of active status.
    """



    # ------------------------------------------------------------------
    # 1. Build a mapping of weather_address  ->  [display_group_id, …]
    #    This will let us add the new "display_groups" array later.
    # ------------------------------------------------------------------
    address_to_groups = defaultdict(list)
    with db_conn.cursor() as cur:
        if include_inactive:
            cur.execute("""
                SELECT display_group_id, weather_address
                FROM display_group
                WHERE weather_address IS NOT NULL
                AND (is_removed = false OR is_removed IS NULL)
            """)
        else:
            cur.execute("""
                SELECT display_group_id, weather_address
                FROM display_group
                WHERE weather_address IS NOT NULL
                AND active = true
                AND (is_removed = false OR is_removed IS NULL)
            """)
        for display_group_id, weather_address in cur.fetchall():
            address_to_groups[weather_address].append(display_group_id)

    # 2. Continue with the existing logic (unchanged)  ------------------
    display_group_ids = get_all_display_group_ids(db_conn, include_inactive=include_inactive)
    
    print(f"Analyzing {len(display_group_ids)} {'display groups' if include_inactive else 'active display groups'} for missing hourly weather data...")
    
    # Collect missing hours by address
    missing_by_address = {}
    
    for i, display_group_id in enumerate(display_group_ids, 1):
        print(f"[{i}/{len(display_group_ids)}] Analyzing display group {display_group_id}...")
        
        missing_hours = get_missing_hours_for_display_group(db_conn, display_group_id)
        
        for weather_address, missing_hour in missing_hours:
            if weather_address not in missing_by_address:
                missing_by_address[weather_address] = []
            missing_by_address[weather_address].append(missing_hour)
    
    # Prepare data for JSON output if requested
    json_data = None
    if output_json_path:
        json_data = {
            "report_generated": datetime.now(timezone.utc).isoformat(),
            "total_display_groups_analyzed": len(display_group_ids),
            "addresses_with_missing_data": len(missing_by_address),
            "include_inactive": include_inactive,
            "missing_hours_by_address": {}
        }
    
    # Generate report
    print("\n" + "="*80)
    print("MISSING HOURLY WEATHER DATA REPORT")
    if not include_inactive:
        print("(Active Display Groups Only)")
    print("="*80)
    
    if not missing_by_address:
        print(f"No missing hourly weather data found across all {'display groups' if include_inactive else 'active display groups'}.")
        
        if output_json_path:
            json_data["total_missing_hours"] = 0
            json_data["total_consolidated_ranges"] = 0
            
            with open(output_json_path, 'w') as f:
                json.dump(json_data, f, indent=2)
            print(f"\nJSON report saved to: {output_json_path}")
        
        return
    
    print(f"Found missing hourly data for {len(missing_by_address)} weather addresses.\n")
    
    total_missing_hours = 0
    total_ranges = 0
    
    # Sort addresses for consistent output
    for address in sorted(missing_by_address.keys()):
        timestamps = missing_by_address[address]
        
        print(f"Weather Address: {address}")
        print(f"Total missing hours: {len(timestamps)}")
        
        # Merge consecutive time ranges
        merged_ranges = merge_time_ranges(timestamps)
        
        print(f"Consolidated missing time ranges: {len(merged_ranges)}")
        
        # Prepare JSON data for this address
        if output_json_path:
            address_data = {
                "total_missing_hours": len(timestamps),
                "consolidated_ranges": len(merged_ranges),
                "missing_time_ranges": [],
                "display_groups": sorted(address_to_groups.get(address, []))

            }
        
        for start_time, end_time in merged_ranges:
            if start_time == end_time:
                # Single hour missing
                print(f"  {start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                
                if output_json_path:
                    address_data["missing_time_ranges"].append({
                        "start_time": start_time.isoformat(),
                        "end_time": start_time.isoformat(),
                        "timezone": str(start_time.tzinfo),
                        "duration_hours": 1
                    })
            else:
                # Range of hours missing
                print(f"  {start_time.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                
                if output_json_path:
                    duration = int((end_time - start_time).total_seconds() / 3600) + 1
                    address_data["missing_time_ranges"].append({
                        "start_time": start_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "timezone": str(start_time.tzinfo),
                        "duration_hours": duration
                    })
        
        if output_json_path:
            json_data["missing_hours_by_address"][address] = address_data
        
        total_missing_hours += len(timestamps)
        total_ranges += len(merged_ranges)
        
        print()  # Empty line between addresses
    
    # Summary
    print("="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Addresses with missing data: {len(missing_by_address)}")
    print(f"Total missing hours: {total_missing_hours}")
    print(f"Total consolidated ranges: {total_ranges}")
    if not include_inactive:
        print("Note: Only active display groups were analyzed")
    
    # Save JSON output if requested
    if output_json_path:
        json_data["total_missing_hours"] = total_missing_hours
        json_data["total_consolidated_ranges"] = total_ranges
        
        with open(output_json_path, 'w') as f:
            json.dump(json_data, f, indent=2)
        print(f"\nJSON report saved to: {output_json_path}")

#----- BEGIN NEW -------
def process_active_locations(api_key, include_hourly=False, dry_run=False, include_inactive=False):
    """
    Process weather data for active locations only (those with display groups needing recent data).
    Fetches data for the previous 24 hours for each active location.

    This is more efficient than process_all_locations() as it only fetches data for addresses
    that have display groups with ongoing tests or tests that ended recently.

    Timezone handling:
    - Calculates date range using UTC for consistency
    - API interprets dates in each location's local timezone
    - This ensures each location gets its local "previous 24 hours"
    
    Args:
        api_key: Visual Crossing API key
        include_hourly: Whether to fetch hourly data
        dry_run: If True, show what would be done without executing
        include_inactive: If False (default), only process locations with active display groups.
                         If True, process all locations regardless of active status.
    """
    # Connect to database
    db_conn = psycopg2.connect(
        host=os.environ['PGHOST_2'],
        user=os.environ['PGUSER_2'],
        dbname=os.environ['PGDATABASE_2'],
        port=os.environ['PGPORT_2']
    )

    try:
        # Get active locations from database
        locations = get_active_weather_addresses(db_conn, include_inactive=include_inactive)
        print(f"Found {len(locations)} active locations to process")

        if not locations:
            print(f"No active locations found. All {'display groups' if include_inactive else 'active display groups'} have ended outside the 48-hour window.")
            return

        # Calculate date range for previous 24 hours
        start_date, end_date = calculate_previous_24_hours()
        print(f"Processing data for date range: {start_date} to {end_date}")

        # Process each active location
        for i, address in enumerate(locations, 1):
            print(f"\n[{i}/{len(locations)}] Processing active location: {address}")

            # Fetch weather data for this location
            weather_data = fetch_weather_data(
                address, start_date, end_date, api_key, include_hourly=include_hourly)

            if weather_data is None:
                print(f"Failed to fetch weather data for {address}. Skipping.")
                continue

            # Insert/update weather data
            if dry_run:
                print(f"Dry run mode: Would insert/update data for {address}")
                insert_weather_data(None, address, weather_data,
                                  include_hourly=include_hourly, dry_run=True)
            else:
                insert_weather_data(db_conn, address, weather_data,
                                  include_hourly=include_hourly, dry_run=False)
                print(f"Weather data for {address} processed successfully.")

    finally:
        db_conn.close()

#----- END NEW -------

def process_all_locations(api_key, include_hourly=False, dry_run=False):
    """
    Process weather data for all locations in the weather_location table.
    Fetches data for the previous 24 hours for each location.
    
    Timezone handling:
    - Calculates date range using UTC for consistency
    - API interprets dates in each location's local timezone
    - This ensures each location gets its local "previous 24 hours"
    """
    # Connect to database
    db_conn = psycopg2.connect(
        host=os.environ['PGHOST_2'],
        user=os.environ['PGUSER_2'],
        dbname=os.environ['PGDATABASE_2'],
        port=os.environ['PGPORT_2']
    )
    
    try:
        # Get all locations from database
        locations = get_all_locations(db_conn)
        print(f"Found {len(locations)} locations to process")
        
        # Calculate date range for previous 24 hours
        start_date, end_date = calculate_previous_24_hours()
        print(f"Processing data for date range: {start_date} to {end_date}")
        
        # Process each location
        for i, address in enumerate(locations, 1):
            print(f"\n[{i}/{len(locations)}] Processing location: {address}")
            
            # Fetch weather data for this location
            weather_data = fetch_weather_data(
                address, start_date, end_date, api_key, include_hourly=include_hourly)
            
            if weather_data is None:
                print(f"Failed to fetch weather data for {address}. Skipping.")
                continue
            
            # Insert/update weather data
            if dry_run:
                print(f"Dry run mode: Would insert/update data for {address}")
                insert_weather_data(None, address, weather_data, 
                                  include_hourly=include_hourly, dry_run=True)
            else:
                insert_weather_data(db_conn, address, weather_data, 
                                  include_hourly=include_hourly, dry_run=False)
                print(f"Weather data for {address} processed successfully.")
                
    finally:
        db_conn.close()


def get_missing_hours(db_conn, address, api_key, dry_run=False):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT date FROM weather.weather_data WHERE address = %s", (address,))
        dates = [row[0] for row in cur.fetchall()]

        for date in dates:
            # Convert date object to string
            date_str = date.strftime("%Y-%m-%d")
            if not dry_run:
                print(f"Fetching hourly data for {date_str}...")
                weather_data = fetch_weather_data(
                    address, date_str, date_str, api_key, include_hourly=True)

                if weather_data is None:
                    print(
                        f"Failed to fetch hourly data for {date_str}. Skipping.")
                    continue

                insert_weather_data(
                    db_conn, address, weather_data, include_hourly=True, dry_run=dry_run)
                print(
                    f"Hourly data for {date_str} inserted/updated successfully.")
            else:
                print(
                    f"Dry run mode: Simulating fetch and insert/update for {date_str}...")
                print(
                    f"API request: fetch_weather_data(address='{address}', start_date='{date_str}', end_date='{date_str}', api_key=<read_from_file>, include_hourly=True)")
                print("SQL queries that would be executed:")

                sql_daily = "INSERT INTO weather.weather_data (date, address, temp, tempmin, tempmax, tz) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (date, address) DO UPDATE SET temp = EXCLUDED.temp, tempmin = EXCLUDED.tempmin, tempmax = EXCLUDED.tempmax, tz = EXCLUDED.tz RETURNING id;"
                print(sql_daily)

                sql_hourly = "INSERT INTO weather.hourly_data (weather_data_id, hour, temp) VALUES (%s, %s, %s) ON CONFLICT (weather_data_id, hour) DO UPDATE SET temp = EXCLUDED.temp;"
                print(sql_hourly)

                print("Data that would be inserted/updated:")
                print(
                    f"Daily data: (date='{date_str}', address='{address}', temp=<temp>, tempmin=<tempmin>, tempmax=<tempmax>, tz=<timezone>)")
                print(
                    f"Hourly data: (weather_data_id=<weather_data_id>, hour=<hour>, temp=<temp>)")
                print("---")

def get_missing_tz(db_conn, address, api_key, dry_run=False):
    with db_conn.cursor() as cur:
        cur.execute("SELECT DISTINCT date FROM weather.weather_data WHERE address = %s AND tz IS NULL ORDER BY date DESC LIMIT 1", (address,))
        row = cur.fetchone()
        
        if row is not None:
            date_str = row[0].strftime("%Y-%m-%d")  # Convert date object to string
            if not dry_run:
                print(f"Fetching weather data for {date_str} to populate missing tz...")
                weather_data = fetch_weather_data(address, date_str, date_str, api_key)
                
                if weather_data is None:
                    print(f"Failed to fetch weather data for {date_str}. Skipping.")
                    return
                
                timezone = weather_data['timezone']
                cur.execute("UPDATE weather.weather_data SET tz = %s WHERE address = %s AND tz IS NULL", (timezone, address))
                db_conn.commit()
                print(f"Missing tz values populated successfully for {address}.")
            else:
                print(f"Dry run mode: Simulating fetch and update for missing tz values...")
                print(f"API request: fetch_weather_data(address='{address}', start_date='{date_str}', end_date='{date_str}', api_key=<read_from_file>)")
                print("SQL query that would be executed:")
                print("UPDATE weather.weather_data SET tz = <timezone> WHERE address = %s AND tz IS NULL")
                print(f"Address: {address}")
                print("---")
        else:
            print(f"No missing tz values found for {address}.")