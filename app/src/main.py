from DataPipline import *

if __name__ == '__main__':
    df_stock_prices=extract_data('/Users/salmaelafifi/Documents/GitHub/Data-Engineering/app/data/daily_trade_prices.csv')
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

    df_trades=extract_data("/Users/salmaelafifi/Documents/GitHub/Data-Engineering/app/data/trades.csv")
    if(check_outliers(df_trades, 'cumulative_portfolio_value')):
        df_cleaned_trades = handle_outliers(df_trades, 'cumulative_portfolio_value')
    
    if(check_outliers(df_trades, 'average_trade_size')):
        df_cleaned_trades = handle_outliers(df_trades, 'average_trade_size')

    save_csv(df_cleaned_trades, "final_trades.csv")

    df_customers = extract_data("/Users/salmaelafifi/Documents/GitHub/Data-Engineering/app/data/dim_customer.csv")

    df_dropped_customer = drop_column(df_customers, "customer_key")

    save_csv(df_dropped_customer, "final_customer.csv")
