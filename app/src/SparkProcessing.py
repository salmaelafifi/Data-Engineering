from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from app.src.DataPipline import save_to_db


FINAL_CSV = "/app/final_data_set/FULL_STOCKS.csv"  

def spark_anaylsis():
    spark = SparkSession.builder \
        .appName("StockAnalysis") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Read the final CSV (after stream has been appended)
    df = spark.read.option("header", True).csv(FINAL_CSV)
    print("First 10 rows from Spark DataFrame:")
    df.show(10, truncate=False)

    
    print("\n====== Spark FUNCTIONS ======\n")

    print("1. Total trading volume for each stock ticker: ")
    q1 = df.groupBy("ticker").agg(F.sum("quantity").alias("total_volume"))
    q1.show()

    print("2. Average stock price by sector: ")
    q2 = df.groupBy("sector").agg(F.avg("price").alias("avg_price"))
    q2.show()

    print("3. Buy vs sell transactions on weekends: ")
    q3 = df.filter(F.col("day_of_week").isin("Saturday", "Sunday")) \
        .groupBy("trade_type").count()
    q3.show()

    print("4. Customers with more than 10 transactions: ")
    q4 = df.groupBy("customer_id").count().filter("count > 10")
    q4.show()

    print("5. Total trade amount per day of week, ordered highest to lowest: ")
    q5 = df.groupBy("day_of_week").agg(F.sum("trade_amount").alias("total_amount")) \
        .orderBy(F.desc("total_amount"))
    q5.show()


    print("\n====== Spark SQL ======\n")

    df.createOrReplaceTempView("stocks")

    print("1. Top 5 most traded tickers by total quantity: ")
    spark.sql("""
        SELECT ticker, SUM(quantity) AS total_quantity
        FROM stocks
        GROUP BY ticker
        ORDER BY total_quantity DESC
        LIMIT 5
    """).show()

    print("2. Average trade amount by customer account type: ")
    spark.sql("""
        SELECT account_type, AVG(trade_amount) AS avg_trade_amount
        FROM stocks
        GROUP BY account_type
    """).show()

    print("3. Transactions during holidays vs non-holidays: ")
    spark.sql("""
        SELECT is_holiday, COUNT(*) AS total_transactions
        FROM stocks
        GROUP BY is_holiday
    """).show()

    print("4. Sectors with highest total trading volume on weekends: ")
    spark.sql("""
        SELECT sector, SUM(quantity) AS weekend_volume
        FROM stocks
        WHERE day_of_week IN ('Saturday', 'Sunday')
        GROUP BY sector
        ORDER BY weekend_volume DESC
    """).show()

    print("5. Total buy vs sell amount for each liquidity tier: ")
    spark.sql("""
        SELECT liquidity_tier, trade_type, SUM(trade_amount) AS total_amount
        FROM stocks
        GROUP BY liquidity_tier, trade_type
        ORDER BY liquidity_tier ASC, trade_type ASC
    """).show()

    spark.stop()

# if __name__ == "__main__":
#     main()

def initialize_spark_session():
    spark = SparkSession.builder \
        .appName("M3_SPARK_APP_THE_EXPERTS") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    print("Spark session initialized successfully")
    spark.stop()


def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("M3_SPARK_APP_THE_EXPERTS") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    df = spark.read.option("header", True).csv(FINAL_CSV)

    # ===== Spark DataFrame operations =====
    q1 = df.groupBy("ticker").agg(F.sum("quantity").alias("total_volume"))
    save_to_db(q1.toPandas(), "analytics_total_volume")

    q2 = df.groupBy("sector").agg(F.avg("price").alias("avg_price"))
    save_to_db(q2.toPandas(), "analytics_avg_price_by_sector")

    q3 = df.filter(F.col("day_of_week").isin("Saturday", "Sunday")) \
           .groupBy("trade_type").count()
    save_to_db(q3.toPandas(), "analytics_weekend_trades")

    q4 = df.groupBy("customer_id").count().filter("count > 10")
    save_to_db(q4.toPandas(), "analytics_active_customers")

    q5 = df.groupBy("day_of_week") \
           .agg(F.sum("trade_amount").alias("total_amount")) \
           .orderBy(F.desc("total_amount"))
    save_to_db(q5.toPandas(), "analytics_trade_amount_by_day")

    # ===== Spark SQL =====
    df.createOrReplaceTempView("stocks")

    sql1 = spark.sql("""
        SELECT ticker, SUM(quantity) AS total_quantity
        FROM stocks
        GROUP BY ticker
        ORDER BY total_quantity DESC
        LIMIT 5
    """)
    save_to_db(sql1.toPandas(), "analytics_top_tickers")

    sql2 = spark.sql("""
        SELECT account_type, AVG(trade_amount) AS avg_trade_amount
        FROM stocks
        GROUP BY account_type
    """)
    save_to_db(sql2.toPandas(), "analytics_avg_trade_by_account")

    sql3 = spark.sql("""
        SELECT is_holiday, COUNT(*) AS total_transactions
        FROM stocks
        GROUP BY is_holiday
    """)
    save_to_db(sql3.toPandas(), "analytics_holiday_vs_nonholiday")

    sql4 = spark.sql("""
        SELECT sector, SUM(quantity) AS weekend_volume
        FROM stocks
        WHERE day_of_week IN ('Saturday', 'Sunday')
        GROUP BY sector
        ORDER BY weekend_volume DESC
    """)
    save_to_db(sql4.toPandas(), "analytics_weekend_volume_by_sector")

    sql5 = spark.sql("""
        SELECT liquidity_tier, trade_type, SUM(trade_amount) AS total_amount
        FROM stocks
        GROUP BY liquidity_tier, trade_type
    """)
    save_to_db(sql5.toPandas(), "analytics_liquidity_trade_amount")

    spark.stop()


