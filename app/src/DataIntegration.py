import pandas as pd
import numpy as np
import os
import DataPipline as dp

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



