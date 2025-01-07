from bs4 import BeautifulSoup
import requests
import logging 
import pandas as pd 

def ftech_data(url):
    """ 
    The function fetchs GDP data from the provided wekipedia URL and returns it as a DataFrame.

    Args:
        url: URL of the webpage containing the GDP data

    Returns:
        pd.DataFrame: DataFrame containing "Country" and 'GDP_USD_billion' columns.
    """
    data_dic ={"Country":[], "GDP_USD_billion":[]} 
    try:
        response = requests.get(url)

        if response.status_code != 200:
            logging.error(f"Failed to connect to the website, status code: {response.status_code}")
            return pd.DataFrame(data_dic)
        
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", {"class":"wikitable"}).find("tbody").find_all("tr")
        if not table:
            logging.error("Could not find the table with class 'wikitable'")
            return pd.DataFrame(data_dic)

        for row in table[2:]:
                try:
                    columns = row.find_all("td")
                    if len(columns) >= 3:
                        data_dic["Country"].append(columns[0].text.strip())
                        data_dic["GDP_USD_billion"].append(columns[2].text.strip())
                except Exception as e:
                     logging.warning(f"Error processing row: {row} - {e}")
        
        df_gdb = pd.DataFrame(data_dic)
        logging.info("Data fetched successfully.")
        return df_gdb
    
    except Exception as e:
         logging.error(f"An error occurred while fetching the data: {e}")
         return df_gdb
