import pandas as pd
import numpy as np
from Utils import save_csv
import os

def extract_data(data_path):
  return pd.read_csv(data_path)


def impute_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    print("\nMissing values before linear interpolation:")
    print(df.isnull().sum())

    
    date_cols = [c for c in df_copy.columns if 'date' in c.lower()]
    if not date_cols:
        raise ValueError("No date column found in the dataset.")
    
    date_col = date_cols[0]

    
    df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors='coerce')

    df_imputed = df_copy.interpolate(method='linear')

    
    print("\nMissing values after linear interpolation:")
    print(df_imputed.isnull().sum())

    
    filled_summary = df_copy.isnull().sum() - df_imputed.isnull().sum()
    print("\nNumber of values imputed per stock:")
    print(filled_summary[filled_summary > 0])

    return df_imputed


def check_outliers(df, col) -> bool:
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    outliers = df[(df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))]
    return (len(outliers)/len(df)) *100 > 10

def handle_outliers(df, col, multiplier=1.5):
    df_copy = df.copy()
    
    Q1 = df_copy[col].quantile(0.25)
    Q3 = df_copy[col].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_cap = Q1 - multiplier * IQR
    upper_cap = Q3 + multiplier * IQR

    df_copy[col] = np.where(
        df_copy[col] > upper_cap, upper_cap,
        np.where(df_copy[col] < lower_cap, lower_cap, df_copy[col])
    )

    return df_copy

def detect_and_handle_outliers(df):
    numeric_cols = df.select_dtypes(include=['number']).columns
    df_cleaned_stock_prices = df.copy()
    for col in numeric_cols:
        if check_outliers(df_cleaned_stock_prices, col):
            print(f"Handling outliers for {col}...")
            df_cleaned_stock_prices = handle_outliers(df_cleaned_stock_prices, col)
        else:
            print(f"No significant outliers detected in {col} (<=10%)")

    
def drop_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    df_copy = df.copy()
    if column_name in df_copy.columns:
        df_copy.drop(column_name, axis=1, inplace=True)
        print(f"Column '{column_name}' dropped.")
    else:
        print(f"Column '{column_name}' does not exist in DataFrame.")
    return df_copy