import pandas as pd
from sklearn.preprocessing import LabelEncoder
from Utils import *
import json
from DataPipline import *

def prepare_visualization():
    df = pd.read_csv("app/final_data_set/FINAL_STOCKS.csv")

    df["holiday_flag"] = df["is_holiday_enc"].map({
    1: "Holiday",
    0: "Non-Holiday"
    })

    # remove encoded columns if any
    encoded_cols = [col for col in df.columns if "_enc" in col]
    df = df.drop(columns=encoded_cols)

    #Add time-based helper columns
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month

    #Trading volume per stock ticker
    volume_by_stock = (
    df.groupby("stock_ticker")
      .agg(
          total_volume=("quantity", "sum"),
          total_trade_amount=("total_trade_amount", "sum")
      )
      .reset_index()
    )

    save_visualization_csv(volume_by_stock, "volume_by_stock.csv")

    # STock price trends by sector over time
    price_trend_sector = (
    df.groupby(["date", "stock_sector"])
      .agg(avg_stock_price=("stock_price", "mean"))
      .reset_index()
    )

    save_visualization_csv(price_trend_sector, "price_trend_sector.csv")

    # Buy vs sell transactions
    buy_sell = (
    df.groupby("transaction_type")
      .agg(
          trade_count=("transaction_type", "count"),
          total_trade_amount=("total_trade_amount", "sum")
      )
      .reset_index()
    )

    save_visualization_csv(buy_sell, "buy_sell_analysis.csv")


    # Trading Activity by Day of Week
    trading_by_day = (
    df.groupby("day_name")
      .agg(trade_count=("day_name", "count"))
      .reset_index()
    )

    save_visualization_csv(trading_by_day, "trading_by_day.csv") 

    # Customer Transaction Distribution
    customer_transaction_distribution = (
    df.groupby("customer_id")
      .agg(transaction_count=("customer_id", "count"))
      .reset_index()
    )

    save_visualization_csv(customer_transaction_distribution, "customer_transaction_distribution.csv")

    # Top 10 Customers by Trade Amount
    top_customers = (
    df.groupby("customer_id")
      .agg(total_trade_amount=("total_trade_amount", "sum"))
      .reset_index()
      .sort_values(by="total_trade_amount", ascending=False)
      .head(10)
    )
    save_visualization_csv(top_customers, "top_customers.csv")

    # Portofolio performance Metrics 
    kpis = pd.DataFrame({
        "total_trades": [len(df)],
        "total_traded_value": [df["total_trade_amount"].sum()],
        "average_trade_size": [df["total_trade_amount"].mean()],
        "average_stock_price": [df["stock_price"].mean()],
        "total_quantity_traded": [df["quantity"].sum()],
    })

    save_visualization_csv(kpis, "kpis.csv")

    # Sector Comparison Dashboard
    sector_comparison = (
        df.groupby("stock_sector")
          .agg(
              total_traded_value=("total_trade_amount", "sum"),
              avg_stock_price=("stock_price", "mean"),
              total_volume=("quantity", "sum"),
              trade_count=("stock_sector", "count"),
          )
          .reset_index()
    )
    save_visualization_csv(sector_comparison, "sector_comparison.csv")

    # Holiday vs Non-Holiday Trading Analysis
    if "holiday_flag" in df.columns:
        holiday_vs_nonholiday = (
            df.groupby("holiday_flag")
              .agg(
                  trade_count=("holiday_flag", "count"),
                  total_traded_value=("total_trade_amount", "sum"),
                  avg_trade_size=("total_trade_amount", "mean"),
                  avg_stock_price=("stock_price", "mean"),
              )
              .reset_index()
        )
    save_visualization_csv(holiday_vs_nonholiday, "holiday_vs_nonholiday.csv")





