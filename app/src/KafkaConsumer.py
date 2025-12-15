import pandas as pd
from kafka import KafkaConsumer
import json
from DataEncoding import *

USER_ID = "55_1251"                 
TOPIC = f"{USER_ID}_Topic"        
FULL_OUTPUT_FILE = "/app/final_data_set/FULL_STOCKS.csv"

def consumer_stream():
    print("Loading milestone remaining 95% dataframe...")


    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
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

