import logging
import pandas as pd
import numpy as np

def data_transformation(df_gdp):
    """
    Applies transformations to the GDP DataFrame:
    - Replaces "—" with NaN.
    - Converts GDP values to float, rounding to two decimal points.

    Args:
        df_gdp (pd.DataFrame): The GDP DataFrame.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    try:
        # Replace "—" with NaN and convert GDP column to float with two decimal places
        df_gdp["GDP_USD_billion"] = (
            df_gdp["GDP_USD_billion"]
            .replace("—", 0)  # Replace "—" with None (interpreted as NULL in databases)
            .replace(",", "", regex=True)  # Remove commas from numbers
            .astype(float)  # Convert to float
            .round(2)  # Round to two decimal points
        )
        logging.info("Data transformation applied successfully.")
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
    
    return df_gdp



