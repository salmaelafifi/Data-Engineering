import pandas as pd
import numpy as np
import os
# from db_utils import save_to_db
import DataPipline as dp

def rename_columns(df, column_name, columns_mapping):
    """
    Rename columns of the DataFrame based on the provided mapping.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    columns_mapping (dict): A dictionary mapping old column names to new column names.

    Returns:
    pd.DataFrame: DataFrame with renamed columns.
    """
    return df.rename(columns={column_name : columns_mapping}, inplace=True)

def merge_dataframes(df1, df2, on_columns):
    """
    Merge two DataFrames on specified columns.

    Parameters:
    df1 (pd.DataFrame): The first DataFrame.
    df2 (pd.DataFrame): The second DataFrame.
    on_columns (list): List of columns to merge on.
    how (str): Type of merge to be performed.

    Returns:
    pd.DataFrame: Merged DataFrame.
    """
    return pd.merge(df1, df2, on=on_columns, how='left')

def melt_dataframe(df, id_vars, value_vars, var_name='variable', value_name='value'):
    """
    Melt the DataFrame from wide format to long format.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    id_vars (list): Columns to use as identifier variables.
    value_vars (list): Columns to unpivot.
    var_name (str): Name for the variable column.
    value_name (str): Name for the value column.

    Returns:
    pd.DataFrame: Melted DataFrame.
    """
    return pd.melt(df, id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name)

def total_trade_amount(D1):
    """
    Calculate the total trade amount for each record in the DataFrame.

    Parameters:
    D1 (pd.DataFrame): The input DataFrame containing 'trade_quantity' and 'stock_price' columns.

    Returns:
    pd.DataFrame: DataFrame with an additional 'total_trade_amount' column.
    """
    D1['total_trade_amount'] = D1['quantity'] * D1['stock_price']
    return D1

def return_wanted_columns(df, wanted_columns):
    """
    Returns a DataFrame with only the specified columns.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    wanted_columns (list): List of columns to retain.

    Returns:
    pd.DataFrame: DataFrame with only the wanted columns.
    """
    return df[wanted_columns]

# if __name__ == '__main__':
#     stocks_clean = dp.extract_data('C:/Users/nouna/OneDrive/Documents/GitHub/Data-Engineering/app/data/stocks_clean.csv')
#     dim_customers_clean = dp.extract_data('C:/Users/nouna/OneDrive/Documents/GitHub/Data-Engineering/app/data/dim_customer_clean.csv')
#     dim_date = dp.extract_data('C:/Users/nouna/OneDrive/Documents/GitHub/Data-Engineering/app/data/dim_date.csv')
#     trades_clean = dp.extract_data('C:/Users/nouna/OneDrive/Documents/GitHub/Data-Engineering/app/data/trades_clean.csv')
#     dim_stock = dp.extract_data('C:/Users/nouna/OneDrive/Documents/GitHub/Data-Engineering/app/data/dim_stock.csv')

#     D1 = merge_dataframes(trades_clean, dim_stock, on_columns='stock_ticker')
#     dim_customers_clean = renameColumns(dim_customers_clean, 'account_type', 'customer_account_type')
#     D1 = merge_dataframes(D1, dim_customers_clean, on_columns='customer_id')
#     dim_date = renameColumns(dim_date, 'date', 'timestamp')
#     D1 = merge_dataframes(D1, dim_date, on_columns=['timestamp'])
#     stocks_clean = renameColumns(stocks_clean, 'date', 'timestamp')
#     stocks_clean_long = melt_dataframe(stocks_clean, id_vars=['timestamp'], value_vars='timestamp', var_name='stock_ticker', value_name='stock_price')
#     D1 = merge_dataframes(D1, stocks_clean_long, on_columns=['timestamp', 'stock_ticker'])
#     D1 = renameColumns(D1,'liquidity_tier', 'stock_liquidity_tier')
#     D1 = renameColumns(D1,'sector', 'stock_sector')
#     D1 = renameColumns(D1,'industry', 'stock_industry')
#     D1 = total_trade_amount(D1)
#     wanted_columns = return_wanted_columns(['timestamp', 'customer_id', 'stock_ticker', 'transaction_type' , 'quantity' , 'average_trade_size', 'stock_price', 'total_trade_amount', 'customer_account_type', 'day_name','is_weekend', 'is_holiday', 'stock_liquidity_tier', 'stock_sector', 'stock_industry'])


    
    # save_to_db(wanted_columns, 'cleaned_vg_sales')

