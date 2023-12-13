import argparse
import requests
import psycopg2
import os
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

def fetch_weather_data(address, start_date, end_date, api_key):
    encoded_address = quote(address)
    encoded_start_date = quote(start_date)
    encoded_end_date = quote(end_date)
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{encoded_address}/{encoded_start_date}/{encoded_end_date}?unitGroup=us&include=days&key={api_key}&contentType=json"
    response = requests.get(url)
    return response.json()

def insert_weather_data(db_conn, address, weather_data):
    with db_conn.cursor() as cur:
        for day in weather_data['days']:
            try:
                cur.execute(
                    "INSERT INTO weather.weather_data (date, address, temp, tempmin, tempmax) VALUES (%s, %s, %s, %s, %s);",
                    (day['datetime'], address, day['temp'], day['tempmin'], day['tempmax'])
                )
            except IntegrityError as e:
                print(f"Duplicate entry for {day['datetime']} at {address}. Skipping.")
                db_conn.rollback()  # Rollback the current transaction for retry
            else:
                db_conn.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--address", required=True)
    args = parser.parse_args()

    api_key = read_api_key()
    if not api_key:
        return

    weather_data = fetch_weather_data(args.address, args.start_date, args.end_date, api_key)

    db_conn = psycopg2.connect(
        host=os.environ['PGHOST_2'],
        user=os.environ['PGUSER_2'],
        dbname=os.environ['PGDATABASE_2'],
        port=os.environ['PGPORT_2']
    )

    insert_weather_data(db_conn, args.address, weather_data)
    db_conn.close()

if __name__ == "__main__":
    main()
