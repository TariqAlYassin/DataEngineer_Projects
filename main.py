from transformation import data_transformation
from scrapping import ftech_data
from db_utils import *
import os
from dotenv import load_dotenv
import pandas as pd
import logging
from mysql.connector import connect


logging.basicConfig(
    level= logging.INFO,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

load_dotenv()
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = "GDP_data"
table_name = "CountryGDP"

logging.info("Fetching the data.......")
df_gdp = ftech_data(url=url)

logging.info("Transforming the data.......")
df_gdp_trans = data_transformation(df_gdp)


connection = connect(host = db_host, user = db_user, password = db_password)
if connection:
    create_db(connection = connection,db_name = db_name)
    create_table(connection = connection, db_name = db_name, table_name = table_name)
    insert_data(connection = connection, db_name = db_name, table_name = table_name, df= df_gdp_trans)







