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
    include_param = "days,hours" if include_hourly else "days"
    
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
                print(f"Error occurred while fetching weather data for {request_date_str}: {e}")
                return None
            except requests.exceptions.JSONDecodeError as e:
                print(f"Error decoding JSON response for {request_date_str}: {e}")
                print("Response content:", response.text)
                return None
        
        print("Hourly weather data fetched successfully")
        return {'days': weather_data}
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
    for day in weather_data['days']:
        sql_daily = "INSERT INTO weather.weather_data (date, address, temp, tempmin, tempmax) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (date, address) DO UPDATE SET temp = EXCLUDED.temp, tempmin = EXCLUDED.tempmin, tempmax = EXCLUDED.tempmax RETURNING id;"
        params_daily = (day['datetime'], address, day['temp'], day['tempmin'], day['tempmax'])

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
                            sql_hourly = "INSERT INTO weather.hourly_data (weather_data_id, hour, temp, tempmin, tempmax) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (weather_data_id, hour) DO UPDATE SET temp = EXCLUDED.temp, tempmin = EXCLUDED.tempmin, tempmax = EXCLUDED.tempmax;"
                            params_hourly = (weather_data_id, hour['datetime'], hour['temp'], hour.get('tempmin', None), hour.get('tempmax', None))
                            cur.execute(sql_hourly, params_hourly)

            except IntegrityError as e:
                print(f"Duplicate entry for {day['datetime']} at {address}. Skipping.")
                db_conn.rollback()
            else:
                db_conn.commit()

def calculate_yesterday(tz_offset=None):
    if tz_offset is None:
        return (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    else:
        tz_delta = timedelta(hours=tz_offset)
        return (datetime.now(timezone.utc) + tz_delta - timedelta(1)).strftime('%Y-%m-%d')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--address", required=True)
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--dry-run", action='store_true', help="Perform a dry run that only shows the request and response in JSON format and the SQL queries")
    parser.add_argument("--tzoffset", type=int, help="Time zone offset in hours (e.g., -7 for Scottsdale, AZ)")
    parser.add_argument("--hourly", action='store_true', help="Fetch and insert hourly weather data")
    args = parser.parse_args()

    # Calculate yesterday's date based on the time zone offset
    yesterday = calculate_yesterday(args.tzoffset)

    if not args.start_date:
        args.start_date = yesterday
    if not args.end_date:
        args.end_date = yesterday

    api_key = read_api_key()
    if not api_key:
        return

    weather_data = fetch_weather_data(args.address, args.start_date, args.end_date, api_key, include_hourly=args.hourly)

    if weather_data is None:
        print("Failed to fetch weather data. Please check the API key and try again.")
        return

    if args.dry_run:
        print(json.dumps({
            'Request': {
                'address': args.address,
                'start_date': args.start_date,
                'end_date': args.end_date,
                'api_key': 'XXXXX'  # API key masked
            },
            'Response': weather_data
        }, indent=4))
        insert_weather_data(None, args.address, weather_data, include_hourly=args.hourly, dry_run=True)
    elif args.debug:
        print(json.dumps({'Request': {'address': args.address, 'start_date': args.start_date, 'end_date': args.end_date}, 'Response': weather_data}, indent=4))
    else:
        db_conn = psycopg2.connect(
            host=os.environ['PGHOST_2'],
            user=os.environ['PGUSER_2'],
            dbname=os.environ['PGDATABASE_2'],
            port=os.environ['PGPORT_2']
        )
        insert_weather_data(db_conn, args.address, weather_data, include_hourly=args.hourly)
        db_conn.close()

if __name__ == "__main__":
    main()