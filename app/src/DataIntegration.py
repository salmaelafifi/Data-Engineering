import pandas as pd
import numpy as np
import os
import DataPipline as dp
from Utils import *

def rename_columns(df, column_name, columns_mapping):
    return df.rename(columns={column_name : columns_mapping}, inplace=True)

def merge_dataframes(df1, df2, on_columns):
    return pd.merge(df1, df2, on=on_columns, how='left')

def melt_dataframe(df, id_vars, value_vars, var_name='variable', value_name='value'):
    return pd.melt(df, id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name)

def total_trade_amount(D1):
    D1['total_trade_amount'] = D1['quantity'] * D1['stock_price']
    return D1

def return_wanted_columns(df, wanted_columns):
    return df[wanted_columns]

def integrate_data():
    stocks_clean = dp.extract_data("/app/clean_data_set/final_daily_stock_prices.csv")
    dim_customers_clean = dp.extract_data("/app/clean_data_set/final_customer.csv")
    dim_date = dp.extract_data("/app/data/dim_date.csv")
    trades_clean = dp.extract_data("/app/clean_data_set/final_trades.csv")
    dim_stock = dp.extract_data("/app/data/dim_stock.csv")

    D1 = merge_dataframes(trades_clean, dim_stock, on_columns=['stock_ticker'])
    rename_columns(dim_customers_clean, 'account_type', 'customer_account_type')
    D1 = merge_dataframes(D1, dim_customers_clean, on_columns='customer_id')
    rename_columns(dim_date, 'date', 'timestamp')
    D1 = merge_dataframes(D1, dim_date, on_columns=['timestamp'])
    rename_columns(stocks_clean, 'date', 'timestamp')
    stocks_clean_long = melt_dataframe(stocks_clean, id_vars=['timestamp'], value_vars=[col for col in stocks_clean.columns if col != 'timestamp'], var_name='stock_ticker', value_name='stock_price')
    D1 = merge_dataframes(D1, stocks_clean_long, on_columns=['timestamp', 'stock_ticker'])
    rename_columns(D1,'liquidity_tier', 'stock_liquidity_tier')
    rename_columns(D1,'sector', 'stock_sector')
    rename_columns(D1,'industry', 'stock_industry')
    D1 = total_trade_amount(D1)
    final_data_set = return_wanted_columns(D1, ['timestamp', 'customer_id', 'stock_ticker', 'transaction_type' , 'quantity' , 'average_trade_size', 'stock_price', 'total_trade_amount', 'customer_account_type', 'day_name','is_weekend', 'is_holiday', 'stock_liquidity_tier', 'stock_sector', 'stock_industry'])

    return final_data_set


