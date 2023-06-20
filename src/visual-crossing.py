import requests
from datetime import datetime, timedelta
import json
import logging
import argparse

CURL_COMMANDS = []

# Set up logging
logging.basicConfig(filename='visual-crossing.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def read_api_key():
    try:
        with open('apikey/visual-crossing-apikey', 'r') as file:
            api_key = file.read().strip()
        return api_key
    except IOError:
        print("Error reading the API key file.")
        return None

# Global constants
API_KEY = read_api_key()

SIMILAR_TEMP_THRESHOLD = 2  # temperature difference to consider as 'similar', in Celsius

def get_temperature_range(location, start_date, end_date, args, debug=False):
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}?unitGroup=metric&key={API_KEY}&contentType=json"
    
    if args.dry_run or debug:
        curl_command = f"curl -X GET '{url}'"
        CURL_COMMANDS.append(curl_command)
        if args.dry_run:
            return

    response = requests.get(url)

    if response.status_code != 200 or response.headers['Content-Type'] != 'application/json':
        print(f"Error retrieving weather data for {location} from {start_date} to {end_date}")
        print("Response status code:", response.status_code)
        print("Response content type:", response.headers['Content-Type'])
        print("Response content:", response.content)
        exit(1)  # terminate the script with error status

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Error retrieving weather data for {location} from {start_date} to {end_date}: {e}")
        return None

    try:
        data = response.json()
        logging.info(f"Response data for {location} from {start_date} to {end_date}: \n{json.dumps(data, indent=4, sort_keys=True)}")
    except ValueError as e:
        print(f"Error decoding JSON response for {location} from {start_date} to {end_date}: {e}")
        print("Response content:", response.content)
        return None

    if 'days' not in data or len(data['days']) == 0:
        print(f"No weather data available for {location} from {start_date} to {end_date}")
        if debug:
            print("Complete response:", json.dumps(data, indent=4, sort_keys=True))
        return None

    return data['days']

def find_similar_days(location, date, args, debug=False):
    target_day_data = get_temperature_range(location, date, date, args, debug)
    if target_day_data is None or len(target_day_data) == 0:
        return []
    target_avg_temp = target_day_data[0]['temp']
    target_max_temp = target_day_data[0].get('tempmax')
    target_min_temp = target_day_data[0].get('tempmin')

    if target_max_temp is None:
        logging.info(f"Expected field 'tempmax' was missing in the response data for {location} on {date}. Adjusting computations to ignore maximum temperature.")
    if target_min_temp is None:
        logging.info(f"Expected field 'tempmin' was missing in the response data for {location} on {date}. Adjusting computations to ignore minimum temperature.")
    if target_avg_temp is None:
        logging.info(f"Expected field 'temp' was missing in the response data for {location} on {date}. Adjusting computations to ignore average temperature.")

    similar_days = []
    years_back = 2 if debug else 5
    window_size = 15 if debug else 50

    today = datetime.strptime(date, "%Y-%m-%d")
    for year in range(years_back + 1):  # +1 to include current year
        if year == 0:
            start_date = (today - timedelta(days=window_size)).strftime("%Y-%m-%d")
            end_date = date
        else:
            date_previous_year = today.replace(year=today.year - year)
            start_date = (date_previous_year - timedelta(days=window_size/2)).strftime("%Y-%m-%d")
            end_date = (date_previous_year + timedelta(days=window_size/2)).strftime("%Y-%m-%d")
        
        days_data = get_temperature_range(location, start_date, end_date, args, debug)

        if days_data is None:
            continue

        for day_data in days_data:
            avg_temp = day_data['temp']
            max_temp = day_data.get('tempmax')
            min_temp = day_data.get('tempmin')

            if (avg_temp is not None and abs(target_avg_temp - avg_temp) <= SIMILAR_TEMP_THRESHOLD and
                max_temp is not None and abs(target_max_temp - max_temp) <= SIMILAR_TEMP_THRESHOLD and
                min_temp is not None and abs(target_min_temp - min_temp) <= SIMILAR_TEMP_THRESHOLD):
                similar_days.append(day_data['datetime'])

            if debug:
                print(f"Comparing {date} with {day_data['datetime']}:")
                print(f"Target Average Temp: {target_avg_temp}°C, Check Average Temp: {avg_temp}°C")
                print(f"Target Max Temp: {target_max_temp}°C, Check Max Temp: {max_temp}°C")
                print(f"Target Min Temp: {target_min_temp}°C, Check Min Temp: {min_temp}°C")
                print("--------")

    return similar_days

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dry-run", help="Perform a dry run, display the cURL command and exit.", action="store_true")
    args = parser.parse_args()

    location = "London"
    date = "2023-06-14"
    debug_mode = True  # Set debug mode to True or False as per your requirement
    similar_days = find_similar_days(location, date, args, debug=debug_mode)

    if args.dry_run:
        print("Dry run mode active. Displaying cURL commands:")
        for command in CURL_COMMANDS:
            print(command)

    if debug_mode:
        print(f"Similar days in terms of temperature for {location} on {date} in the last 2 years are:")
    else:
        print(f"Similar days in terms of temperature for {location} on {date} in the last 5 years are:")
    for day in similar_days:
        print(day)

