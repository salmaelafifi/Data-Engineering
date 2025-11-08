import os
import pandas as pd

def save_csv(df: pd.DataFrame, filename: str, directory: str = "clean_data_set") -> str:
    """
    Save a DataFrame as a CSV file in the specified directory.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        filename (str): The name of the output CSV file (e.g., 'cleaned_data.csv').
        directory (str): The folder where the CSV will be saved (default: 'clean').

    Returns:
        str: The full file path of the saved CSV file.
    """
    # Ensure the target directory exists
    os.makedirs(directory, exist_ok=True)

    # Construct the full file path
    filepath = os.path.join(directory, filename)

    # Save the DataFrame to CSV
    df.to_csv(filepath, index=False)

    print(f"Data saved successfully to: {filepath}")
    return filepath
