from DataPipline import *
from DataIntegration import *
from Utils import *
from PrepareStream import *
from KafkaConsumer import *
from SparkProcessing import *
from DataEncoding import *

if __name__ == '__main__':
    df_stock_prices=extract_data('/app/data/daily_trade_prices.csv')
    df_imputed_stock_prices=impute_missing_data(df_stock_prices)
    save_csv(df_imputed_stock_prices, "imputed_stock_prices.csv")

    numeric_cols = df_imputed_stock_prices.select_dtypes(include=['number']).columns

    df_cleaned_stock_prices = df_imputed_stock_prices.copy()
    for col in numeric_cols:
        if check_outliers(df_cleaned_stock_prices, col):
            print(f"Handling outliers for {col}...")
            df_cleaned_stock_prices = handle_outliers(df_cleaned_stock_prices, col)
        else:
            print(f"No significant outliers detected in {col} (<=10%)")

    
    save_csv(df_cleaned_stock_prices, "final_daily_stock_prices.csv")

    df_trades=extract_data("/app/data/trades.csv")
    if(check_outliers(df_trades, 'cumulative_portfolio_value')):
        df_cleaned_trades = handle_outliers(df_trades, 'cumulative_portfolio_value')
    
    if(check_outliers(df_trades, 'average_trade_size')):
        df_cleaned_trades = handle_outliers(df_trades, 'average_trade_size')

    save_csv(df_cleaned_trades, "final_trades.csv")

    df_customers = extract_data("/app/data/dim_customer.csv")

    df_dropped_customer = drop_column(df_customers, "customer_key")

    save_csv(df_dropped_customer, "final_customer.csv")


# data integration
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

save_to_db(final_data_set, "stock_trades_data_set")

# ____________Milestone 2______________

prepare_stream()
remaining_95= extract_data("/app/streaming_data/remaining_95.csv")
encode_data(remaining_95)

consumer_stream()
spark_anaylsis()
