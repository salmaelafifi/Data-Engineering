# Dataset Description

This dataset contains detailed trading information for multiple stocks across different customers, including transaction details, stock characteristics, and temporal context. Below is a description of each column:

- **transaction_id**: A unique identifier for each transaction record.  
- **timestamp**: The date and time when the transaction occurred.  
- **customer_id**: A unique identifier for the customer executing the transaction.  
- **stock_ticker**: The symbol representing the stock involved in the transaction (e.g., STK001 to STK020).  
- **transaction_type**: The type of transaction, either a purchase (BUY) or a sale (SELL) of the stock.  
- **quantity**: The number of shares traded in the transaction.  
- **average_trade_size**: The average number of shares per trade for this transaction.  
- **stock_price**: The price of a single share at the time of the transaction.  
- **total_trade_amount**: The total monetary value of the transaction, calculated as `quantity * stock_price`.  
- **customer_account_type**: The category of the customer account, either Retail or Institutional.  
- **day_name**: The name of the day of the week when the transaction occurred (e.g., Monday, Tuesday).  
- **is_weekend**: A boolean indicator of whether the transaction occurred on a weekend (`True` or `False`).  
- **is_holiday**: A boolean indicator of whether the transaction occurred on a public holiday (`True` or `False`).  
- **stock_liquidity_tier**: A classification of the stock’s liquidity level (e.g., High, Medium, Low).  
- **stock_sector**: The sector to which the stock belongs, such as Energy, Consumer, Technology, or Finance.  
- **stock_industry**: The industry category of the stock, e.g., Utilities, Retail, Software, Asset Management, Oil & Gas, Hardware, Renewables, Pharmaceuticals.
