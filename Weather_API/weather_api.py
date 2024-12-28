import requests 
import logging

# Function for fetching the data 
def fetch_weather_data(api_key,cities):
    base_url = "http://api.weatherapi.com/v1/current.json"
    weather_data_dic ={"city":[], "temp_c":[], "is_day":[], "time_stamp":[], "condition":[], "wind_kph":[], "humidity":[], "feelslike_c":[]}

    for city in cities:
        try:
            url = f"{base_url}?key={api_key}&q={city}"
            response= requests.get(url).json()

            if "current" not in response:
                logging.error(f"Invalid response for{city},{response}")
                continue

            weather_data_dic["city"].append(city)
            weather_data_dic["temp_c"].append(response["current"]["temp_c"])
            weather_data_dic["is_day"].append(response["current"]["is_day"])
            weather_data_dic["time_stamp"].append(response["current"]["last_updated"])
            weather_data_dic["condition"].append(response["current"]["condition"]["text"])
            weather_data_dic["wind_kph"].append(response["current"]["wind_kph"])
            weather_data_dic["humidity"].append(response["current"]["humidity"])
            weather_data_dic["feelslike_c"].append(response["current"]["feelslike_c"])

        except Exception as e:
            logging.error(f"Error fetching data for {city}: {e}")

    return weather_data_dic