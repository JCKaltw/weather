# /home/chris/projects/weather/src/get-weather-data.py
#
# TIMEZONE HANDLING STRATEGY:
# - Date calculations use UTC time for consistency across all locations
# - Visual Crossing API interprets date parameters in each location's local timezone
# - Daily data: stored as YYYY-MM-DD dates + timezone string in 'tz' column  
# - Hourly data: stored as local timestamps (YYYY-MM-DD HH:MM:SS) in location's timezone
# - To interpret hourly data, always reference the corresponding weather_data.tz value

import argparse
import requests
import psycopg2
import os
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from psycopg2 import IntegrityError


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


def insert_weather_data(db_conn, address, weather_data, include_hourly=False, dry_run=False):
    timezone = weather_data['timezone']
    for day in weather_data['days']:
        sql_daily = "INSERT INTO weather.weather_data (date, address, temp, tempmin, tempmax, tz) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (date, address) DO UPDATE SET temp = EXCLUDED.temp, tempmin = EXCLUDED.tempmin, tempmax = EXCLUDED.tempmax, tz = EXCLUDED.tz RETURNING id;"
        params_daily = (day['datetime'], address, day['temp'],
                        day['tempmin'], day['tempmax'], timezone)

        if dry_run:
            print("Dry run mode: SQL query that would be executed:")
            print(sql_daily % params_daily)
        else:
            try:
                with db_conn.cursor() as cur:
                    cur.execute(sql_daily, params_daily)
                    weather_data_id = cur.fetchone()[0]

                    if include_hourly and 'hours' in day:
                        for hour in day['hours']:
                            # CRITICAL: Visual Crossing API returns hourly data in LOCAL TIME
                            # - day['datetime'] = "2025-06-11" (date in local timezone)  
                            # - hour['datetime'] = "14:00:00" (time in local timezone)
                            # - Combined: "2025-06-11 14:00:00" = LOCAL TIME for this location
                            # - The timezone is stored separately in weather_data.tz column
                            # This ensures proper alignment with device data in the same local timezone
                            timestamp = f"{day['datetime']} {hour['datetime']}"
                            sql_hourly = "INSERT INTO weather.hourly_data (weather_data_id, hour, temp) VALUES (%s, %s, %s) ON CONFLICT (weather_data_id, hour) DO UPDATE SET temp = EXCLUDED.temp;"
                            params_hourly = (
                                weather_data_id, timestamp, hour['temp'])
                            cur.execute(sql_hourly, params_hourly)

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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--address")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--dry-run", action='store_true',
                        help="Perform a dry run that only shows the request and response in JSON format and the SQL queries")
    parser.add_argument("--tzoffset", type=int,
                        help="Time zone offset in hours (e.g., -7 for Scottsdale, AZ) - used only for single location operations")
    parser.add_argument("--hourly", action='store_true',
                        help="Fetch and insert hourly weather data")
    parser.add_argument("--get-missing-hours", action='store_true',
                        help="Fetch and update missing hourly data for the specified address")
    parser.add_argument("--get-missing-tz", action='store_true',
                        help="Fetch and populate missing tz values for the specified address")
    parser.add_argument("--run-for-all-locations", action='store_true',
                        help="Run weather data collection for all locations in the weather_location table, fetching the previous 24 hours of data using UTC-based date calculation")
    args = parser.parse_args()

    api_key = read_api_key()
    if not api_key:
        return

    # Handle the new --run-for-all-locations option
    if args.run_for_all_locations:
        if args.address or args.start_date or args.end_date or args.tzoffset or args.get_missing_hours or args.get_missing_tz:
            print("Error: --run-for-all-locations cannot be used with --address, --start-date, --end-date, --tzoffset, --get-missing-hours, or --get-missing-tz options.")
            return
        
        print("Running weather data collection for all locations...")
        process_all_locations(api_key, include_hourly=args.hourly, dry_run=args.dry_run)
        return

    # Existing functionality - require address for all other operations
    if not args.address:
        print("Error: --address is required when not using --run-for-all-locations.")
        return

    # Calculate yesterday's date based on the time zone offset
    yesterday = calculate_yesterday(args.tzoffset)

    if not args.start_date:
        args.start_date = yesterday
    if not args.end_date:
        args.end_date = yesterday

    if args.get_missing_tz:
        db_conn = psycopg2.connect(
            host=os.environ['PGHOST_2'],
            user=os.environ['PGUSER_2'],
            dbname=os.environ['PGDATABASE_2'],
            port=os.environ['PGPORT_2']
        )
        get_missing_tz(db_conn, args.address, api_key, dry_run=args.dry_run)
        db_conn.close()
    elif args.get_missing_hours:
        if args.start_date != yesterday or args.end_date != yesterday:
            print(
                "Error: --start-date and --end-date are not allowed when using --get-missing-hours.")
            return

        db_conn = psycopg2.connect(
            host=os.environ['PGHOST_2'],
            user=os.environ['PGUSER_2'],
            dbname=os.environ['PGDATABASE_2'],
            port=os.environ['PGPORT_2']
        )
        get_missing_hours(db_conn, args.address, api_key, dry_run=args.dry_run)
        db_conn.close()
    else:
        weather_data = fetch_weather_data(
            args.address, args.start_date, args.end_date, api_key, include_hourly=args.hourly)

        if weather_data is None:
            print("Failed to fetch weather data. Please check the API key and try again.")
            return

        if args.dry_run:
            print(json.dumps({
                'Request': {
                    'address': args.address,
                    'start_date': args.start_date,
                    'end_date': args.end_date
                },
                'Response': weather_data
            }, indent=4))
            insert_weather_data(None, args.address, weather_data,
                                include_hourly=args.hourly, dry_run=True)
        elif args.debug:
            print(json.dumps({'Request': {'address': args.address, 'start_date': args.start_date,
                  'end_date': args.end_date}, 'Response': weather_data}, indent=4))
        else:
            db_conn = psycopg2.connect(
                host=os.environ['PGHOST_2'],
                user=os.environ['PGUSER_2'],
                dbname=os.environ['PGDATABASE_2'],
                port=os.environ['PGPORT_2']
            )
            insert_weather_data(db_conn, args.address,
                                weather_data, include_hourly=args.hourly)
            db_conn.close()

if __name__ == "__main__":
    main()
