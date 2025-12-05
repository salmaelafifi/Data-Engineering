import pandas as pd
from sklearn.preprocessing import LabelEncoder
from Utils import *
import json

def encode_data(final_df: pd.DataFrame):
    df = final_df.copy()
    global_lookup = {}

    # =====================================================
    # 1. LABEL ENCODING → stock_ticker (ADD column, keep original)
    # =====================================================
    if 'stock_ticker' in df.columns:
        le = LabelEncoder()
        df['stock_ticker_encoded'] = le.fit_transform(df['stock_ticker'].astype(str))

        lookup_df = pd.DataFrame({
            'column_name': 'stock_ticker',
            'original_value': le.classes_,
            'encoded_value': range(len(le.classes_))
        })

        global_lookup['stock_ticker'] = lookup_df
        save_lookup_csv(lookup_df, 'lookup_stock_ticker.csv')

    # =====================================================
    # 2. ONE-HOT (k-1) → ADD COLUMNS, KEEP ORIGINAL
    # =====================================================
    one_hot_cols = [
        'transaction_type',
        'stock_sector',
        'stock_industry',
        'customer_account_type',
        'day_name'
    ]

    for col in one_hot_cols:
      if col in df.columns:

        original_values = df[col].astype(str)

        # ✅ FORCE 0/1 INSTEAD OF TRUE/FALSE
        dummies = pd.get_dummies(
            original_values,
            prefix=f"{col}_enc",
            drop_first=True
        ).astype(int)

        # ✅ ADD encoded columns (do NOT drop original)
        df = pd.concat([df, dummies], axis=1)

        lookup_rows = []

        encoded_categories = {
            c.replace(f"{col}_enc_", "") for c in dummies.columns
        }
        all_categories = set(original_values.unique())

        # ✅ Explicitly encoded categories
        for dummy_col in dummies.columns:
            original_val = dummy_col.replace(f"{col}_enc_", "")
            lookup_rows.append({
                'column_name': col,
                'original_value': original_val,
                'encoded_value': dummy_col
            })

        # ✅ Dropped baseline category → represented by ALL ZEROS
        dropped_category = list(all_categories - encoded_categories)
        if dropped_category:
            lookup_rows.append({
                'column_name': col,
                'original_value': dropped_category[0],
                'encoded_value': 'ALL_ZEROS (baseline)'
            })

        lookup_df = pd.DataFrame(lookup_rows)
        global_lookup[col] = lookup_df
        # lookup_df.to_csv(f'lookup_{col}.csv', index=False)
        save_lookup_csv(lookup_df, f'lookup_{col}.csv')

    # =====================================================
    # 3. BINARY → ADD encoded columns, KEEP ORIGINAL
    # =====================================================
    binary_cols = ['is_weekend', 'is_holiday']

    for col in binary_cols:
        if col in df.columns:
            encoded_col = f"{col}_enc"
            df[encoded_col] = df[col].astype(int)

            lookup_df = pd.DataFrame({
                'column_name': [col, col],
                'original_value': [0, 1],
                'encoded_value': [0, 1]
            })

            global_lookup[col] = lookup_df
            save_lookup_csv(lookup_df, f'lookup_{col}.csv')

    # =====================================================
    # ✅ FINAL OUTPUT
    # =====================================================
    save_csv(df, "FINAL_STOCKS.csv")
    with open("encoded_schema.json", "w") as f:
      json.dump(list(df.columns), f)

    return df, global_lookup


def transform_stream_batch(stream_df: pd.DataFrame,
                           lookup_folder: str = "app/lookup_tables",
                           final_csv_path: str = "app/clean_data_set/FINAL_STOCKS.csv",
                           schema_path: str = "app/encoded_schema.json"):
    """
    Transforms a streamed micro-batch using precomputed lookup tables
    and appends it safely to the same FINAL_STOCKS.csv.
    """

    # ============================
    # 1. LOAD FROZEN SCHEMA
    # ============================
    with open(schema_path, "r") as f:
        encoded_columns = json.load(f)

    # ============================
    # 2. LOAD LOOKUP TABLES
    # ============================
    lookup_tables = {}

    lookup_files = {
        'stock_ticker': 'lookup_stock_ticker.csv',
        'transaction_type': 'lookup_transaction_type.csv',
        'stock_sector': 'lookup_stock_sector.csv',
        'stock_industry': 'lookup_stock_industry.csv',
        'customer_account_type': 'lookup_customer_account_type.csv',
        'day_name': 'lookup_day_name.csv',
        'is_weekend': 'lookup_is_weekend.csv',
        'is_holiday': 'lookup_is_holiday.csv'
    }

    for key, file_name in lookup_files.items():
        path = os.path.join(lookup_folder, file_name)
        if os.path.exists(path):
            lookup_tables[key] = pd.read_csv(path)

    encoded_rows = []

    # ============================
    # 3. ENCODE EACH STREAM ROW
    # ============================
    for _, row in stream_df.iterrows():
        encoded_row = row.copy()

        # --------- LABEL: stock_ticker ---------
        if 'stock_ticker' in lookup_tables:
            lookup = lookup_tables['stock_ticker']
            mapping = dict(zip(lookup['original_value'], lookup['encoded_value']))
            encoded_row['stock_ticker_encoded'] = mapping.get(str(row['stock_ticker']), -1)

        # --------- ONE-HOT (k-1) ---------
        for col, lookup in lookup_tables.items():
            if col in ['stock_ticker', 'is_weekend', 'is_holiday']:
                continue

            if col in row.index:
                value = str(row[col])

                # initialize all encoded columns to 0
                for enc in lookup['encoded_value']:
                    if enc.startswith(f"{col}_enc"):
                        encoded_row[enc] = 0

                match = lookup[lookup['original_value'].astype(str) == value]

                if not match.empty:
                    enc_val = match.iloc[0]['encoded_value']
                    if 'ALL_ZEROS' not in enc_val:
                        encoded_row[enc_val] = 1

        # --------- BINARY ---------
        for col in ['is_weekend', 'is_holiday']:
            if col in row.index:
                encoded_row[f"{col}_enc"] = int(row[col])

        encoded_rows.append(encoded_row)

    encoded_stream_df = pd.DataFrame(encoded_rows)

    # ============================
    # 4. ENFORCE FROZEN SCHEMA
    # ============================
    encoded_stream_df = encoded_stream_df.reindex(columns=encoded_columns, fill_value=0)

    # ============================
    # 5. APPEND TO SAME FINAL CSV
    # ============================
    encoded_stream_df.to_csv(
        final_csv_path,
        mode="a",
        header=False,
        index=False
    )

    return encoded_stream_df
