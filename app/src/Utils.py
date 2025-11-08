import os
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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


def create_database():
    try:
        # Connect to PostgreSQL server (without specifying database)
        conn = psycopg2.connect(
            host='pgdatabase',
            user='root',
            password='root',
            port=5432
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'tradesdb'")
        exists = cursor.fetchone()
        
        if not exists:
            # Create database
            cursor.execute('CREATE DATABASE tradesdb')
            print("Database 'tradesdb' created successfully")
        else:
            print("Database 'tradesdb' already exists")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating database: {e}")

def save_to_db(cleaned, table_name):
    create_database()
    # postgresql://username:password@container_name:port/database_name
    engine = create_engine('postgresql://root:root@pgdatabase:5432/tradesdb')
    if(engine.connect()):
        print('Connected to Database')
        try:
            print('Writing cleaned dataset to database')
            cleaned.to_sql(table_name, con=engine, if_exists='replace')
            print('Done writing to database')
        except ValueError as vx:
            print('Cleaned Table already exists.')
        except Exception as ex:
            print(ex)
    else:
        print('Failed to connect to Database')