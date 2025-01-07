from mysql.connector import connect, Error
import logging

# Connect to the MySQL database
def connect_to_db(host, user, password):
    try:
        connection = connect(host=host, user=user, password=password)
        connection.autocommit = True
        logging.info("Database connection successful")
        return connection
    except Error as e:
        logging.error(f"Database connection failed: {e}")
        return None

# Set up the database and table
def setup_db(connection, db_name, table_name):
    try:
        cursor = connection.cursor()
        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        logging.info(f"Database '{db_name}' created or exists")

        # Create table
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            city VARCHAR(255) NOT NULL,
            temp_c FLOAT,
            is_day INT CHECK (is_day IN (0, 1)),
            timestamp DATETIME NOT NULL,
            description VARCHAR(255),
            wind_kph FLOAT,
            humidity FLOAT,
            feelslike_c FLOAT
        )
        """)
        logging.info(f"Table '{table_name}' created or exists")
    except Error as e:
        logging.error(f"Error setting up database or table: {e}")

# Insert weather data into the table
def insert_weather_data(connection, db_name, table_name, df_weather):
    try:
        cursor = connection.cursor()
        # Prepare data for insertion
        data_to_insert = [
            (
                row["city"], row["temp_c"], row["is_day"], row["time_stamp"],
                row["condition"], row["wind_kph"], row["humidity"], row["feelslike_c"]
            )
            for _, row in df_weather.iterrows()
        ]
        # Execute the insert query
        cursor.executemany(f"""
        INSERT INTO {db_name}.{table_name} 
        (city, temp_c, is_day, timestamp, description, wind_kph, humidity, feelslike_c)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, data_to_insert)
        logging.info("Data inserted successfully")
    except Error as e:
        logging.error(f"Error inserting data: {e}")

