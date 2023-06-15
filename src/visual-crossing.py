import requests
from datetime import datetime, timedelta

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

def get_temperature(location, date, debug=False):
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{date}/{date}?unitGroup=metric&key={API_KEY}&contentType=json"
    
    if debug:
        curl_command = f"curl -X GET '{url}'"
        print("Curl command:", curl_command)

    response = requests.get(url)
    
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Error retrieving weather data for {location} on {date}: {e}")
        return None, None, None
    
    try:
        data = response.json()
    except ValueError as e:
        print(f"Error decoding JSON response for {location} on {date}: {e}")
        print("Response content:", response.content)
        return None, None, None

    if 'days' not in data or len(data['days']) == 0:
        print(f"No weather data available for {location} on {date}")
        if debug:
            print("Complete response:", data)
        return None, None, None

    avg_temp = data['days'][0]['temp']
    max_temp = data['days'][0].get('maxt')
    min_temp = data['days'][0].get('mint')

    return avg_temp, max_temp, min_temp

def find_similar_days(location, date, debug=False):
    target_avg_temp, target_max_temp, target_min_temp = get_temperature(location, date, debug)

    if target_avg_temp is None:
        return []

    similar_days = []
    if debug:
        window_size = 30
    else:
        window_size = 365 * 5

    today = datetime.strptime(date, "%Y-%m-%d")
    for i in range(1, window_size + 1):
        check_date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        avg_temp, max_temp, min_temp = get_temperature(location, check_date, debug)

        if avg_temp is None:
            continue

        if (abs(target_avg_temp - avg_temp) <= SIMILAR_TEMP_THRESHOLD and
            abs(target_max_temp - max_temp) <= SIMILAR_TEMP_THRESHOLD and
            abs(target_min_temp - min_temp) <= SIMILAR_TEMP_THRESHOLD):
            similar_days.append(check_date)

        if debug:
            print(f"Comparing {date} with {check_date}:")
            print(f"Target Average Temp: {target_avg_temp}°C, Check Average Temp: {avg_temp}°C")
            print(f"Target Max Temp: {target_max_temp}°C, Check Max Temp: {max_temp}°C")
            print(f"Target Min Temp: {target_min_temp}°C, Check Min Temp: {min_temp}°C")
            print("--------")

    return similar_days

if __name__ == "__main__":
    location = "London"
    date = "2023-06-14"
    debug_mode = True  # Set debug mode to True or False as per your requirement
    similar_days = find_similar_days(location, date, debug=debug_mode)

    if debug_mode:
        print(f"Similar days in terms of temperature for {location} on {date} in the last month are:")
    else:
        print(f"Similar days in terms of temperature for {location} on {date} in the last 5 years are:")
    for day in similar_days:
        print(day)
