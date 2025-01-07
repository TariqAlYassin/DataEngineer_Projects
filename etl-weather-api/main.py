from dotenv import load_dotenv
import logging
import os
import pandas as pd 
from weather_api import fetch_weather_data
from db_utils import *

logging.basicConfig(
    level= logging.INFO,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()
api_key = os.getenv("API_KEY")
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

arab_capitals = [
    "Algiers",       # Algeria
    "Manama",        # Bahrain
    "Cairo",         # Egypt
    "Baghdad",       # Iraq
    "Amman",         # Jordan
    "Kuwait City",   # Kuwait
    "Beirut",        # Lebanon
    "Tripoli",       # Libya
    "Nouakchott",    # Mauritania
    "Rabat",         # Morocco
    "Muscat",        # Oman
    "Doha",          # Qatar
    "Riyadh",        # Saudi Arabia
    "Mogadishu",     # Somalia
    "Khartoum",      # Sudan
    "Damascus",      # Syria
    "Tunis",         # Tunisia
    "Abu Dhabi",     # United Arab Emirates
    "Jerusalem"     # Palestine
]
db_name = "weather_api"
table_name ="weather"

logging.info("Fetching weather data....")
weather_date_dic = fetch_weather_data(api_key=api_key, cities=arab_capitals)
df_weather = pd.DataFrame(weather_date_dic)


connection = connect_to_db(host=db_host, user=db_user, password=db_password)
if connection :
    setup_db(connection=connection, db_name=db_name, table_name=table_name)
    insert_weather_data(connection=connection, db_name=db_name, table_name=table_name, df_weather=df_weather)

