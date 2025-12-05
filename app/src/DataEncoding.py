import pandas as pd
from sklearn.preprocessing import LabelEncoder
from Utils import *

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
        save_csv(lookup_df, 'lookup_stock_ticker.csv')

    # =====================================================
    # 2. ONE-HOT (k-1) → ADD COLUMNS, KEEP ORIGINAL
    # =====================================================
    one_hot_cols = [
        'transaction_type',
        'stock_sector',
        'stock_industry',
        'customer_account_type',
        'day'
    ]

    for col in one_hot_cols:
        if col in df.columns:

            original_values = df[col].astype(str)
            dummies = pd.get_dummies(
                original_values,
                prefix=f"{col}_enc",
                drop_first=True
            )

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
            save_csv(lookup_df, f'lookup_{col}.csv')

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
            save_csv(lookup_df, f'lookup_{col}.csv')

    # =====================================================
    # ✅ FINAL OUTPUT
    # =====================================================
    save_csv(df, "FINAL_STOCKS.csv")

    return df, global_lookup
