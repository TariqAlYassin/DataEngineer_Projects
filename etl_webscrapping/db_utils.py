from mysql.connector import connect, Error
import logging

def create_db(connection, db_name):
    """
    Create a MySQL database if it doesn't exist.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            logging.info(f"Database '{db_name}' created or already exists.")
    except Error as e:
        logging.error(f"Failed to create database '{db_name}': {e}")

def create_table(connection, db_name, table_name):
    """
    Create a table within the specified database.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"USE {db_name}")
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    country VARCHAR(255) ,
                    gdp_usd_billion FLOAT NULL
                )
            """)
            logging.info(f"Table '{table_name}' created successfully in database '{db_name}'.")
    except Error as e:
        logging.error(f"Failed to create table '{table_name}': {e}")

def insert_data(connection, db_name, table_name, df):
    """
    Insert data from a DataFrame into the specified table.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"USE {db_name}")
            data_to_insert = [(row["Country"], row["GDP_USD_billion"]) for _, row in df.iterrows()]
            cursor.executemany(
                f"INSERT INTO {table_name} (country, gdp_usd_billion) VALUES (%s, %s)", 
                data_to_insert
            )
            connection.commit()
            logging.info(f"Data inserted successfully into table '{table_name}'.")
    except Error as e:
        logging.error(f"Failed to insert data into table '{table_name}': {e}")