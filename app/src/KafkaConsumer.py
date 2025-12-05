import pandas as pd
from kafka import KafkaConsumer
import json
from DataEncoding import *

USER_ID = "55_1251"                 
TOPIC = f"{USER_ID}_Topic"        
FULL_OUTPUT_FILE = "/app/final_data_set/FULL_STOCKS.csv"


# def process_stream(row_dict):

#     df_row = pd.DataFrame([row_dict])

#     # -----------------------------------------
#     # Example encoding steps (COMMENTED OUT)
#     # -----------------------------------------
#     # df_row = your_encoding_function(df_row)
#     # df_row = encode_categorical(df_row)
#     # df_row = scale_numeric(df_row)
#     # df_row["is_weekend"] = df_row["date"].dt.dayofweek >= 5
#     # -----------------------------------------

#     return df_row


# def consumer_stream():
#     print("Loading milestone remaining 95% dataframe...")
#     full_df = pd.read_csv(FINAL_95_FILE)
#     print(f"Loaded {len(full_df)} rows from {FINAL_95_FILE}")

   
#     consumer = KafkaConsumer(
#         TOPIC,
#         bootstrap_servers="kafka:9092",
#         auto_offset_reset="latest",
#         value_deserializer=lambda x: json.loads(x.decode("utf-8"))
#     )

#     print(f"Listening for messages on topic '{TOPIC}'...")

#     for msg in consumer:
#         value = msg.value
#         print(f"Received: {value}")

        
#         if value == "EOS" or value == {"EOS": True}:
#             print("EOS received. Stopping consumer...")
#             break

       
#         processed_row = process_stream(value)

        
#         full_df = pd.concat([full_df, processed_row], ignore_index=True)

    
#     print(f"Saving final merged dataset to {FULL_OUTPUT_FILE}...")
#     full_df.to_csv(FULL_OUTPUT_FILE, index=False)

#     consumer.close()
#     print("Consumer closed.")
#     print("FULL_STOCKS.csv created successfully.")


def consumer_stream():
    print("Loading milestone remaining 95% dataframe...")


    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="kafka:9092",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    print(f"Listening for messages on topic '{TOPIC}'...")

    for msg in consumer:
        value = msg.value
        print(f"Received: {value}")

        if value == "EOS" or value == {"EOS": True}:
            print("EOS received. Stopping consumer...")
            break

        # Convert single message dict into a DataFrame
        stream_df = pd.DataFrame([value])

        # Transform using your frozen schema and lookups
        encoded_df = transform_stream_batch(stream_df)


    consumer.close()
    print("Consumer closed.")
    print("FULL_STOCKS.csv updated successfully.")

# if __name__ == "__main__":
#     consumer_stream()
