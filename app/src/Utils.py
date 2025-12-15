import os
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def save_csv(df: pd.DataFrame, filename: str, directory: str = "app/clean_data_set") -> str:
    os.makedirs(directory, exist_ok=True)

    filepath = os.path.join(directory, filename)
    df.to_csv(filepath, index=False)

    print(f"Data saved successfully to: {filepath}")
    return filepath


def create_database():
    try:
        conn = psycopg2.connect(
            host='pgdatabase',
            user='myuser',
            password='mypassword',
            port=5432
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        

        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'trades_db'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute('CREATE DATABASE trades_db')
            print("Database 'trades_db' created successfully")
        else:
            print("Database 'trades_db' already exists")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating database: {e}")

def save_to_db(cleaned, table_name):
    create_database()
    engine = create_engine('postgresql://myuser:mypassword@pgdatabase:5432/trades_db')
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

def save_lookup_csv(df: pd.DataFrame, filename: str, directory: str = "app/lookup_tables") -> str:
    os.makedirs(directory, exist_ok=True)

    filepath = os.path.join(directory, filename)
    df.to_csv(filepath, index=False)

    print(f"Data saved successfully to: {filepath}")
    return filepath

def save_visualization_csv(df: pd.DataFrame, filename: str, directory: str = "app/streamlit/visualization_data") -> str:
    os.makedirs(directory, exist_ok=True)

    filepath = os.path.join(directory, filename)
    df.to_csv(filepath, index=False)

    print(f"Visualization data saved successfully to: {filepath}")
    return filepath