import pandas as pd
from kafka import KafkaProducer
import json
import time

USER_ID = "55_1251"                 
TOPIC = f"{USER_ID}_Topic"
CSV_FILE = "/app/streaming_data/stream.csv"          
DELAY = 0.3                      

def create_producer():
    return KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def kafka_producer():
    producer = create_producer()
    csv_data = pd.read_csv(CSV_FILE)

    print(f"Sending records from {CSV_FILE} to Kafka topic '{TOPIC}'...")

    for _, row in csv_data.iterrows():
        message = row.to_dict()
        

        producer.send(TOPIC, value=message)
        print(f"Sent: {message}")

        time.sleep(DELAY)

    producer.send(TOPIC, value={"EOS": True})
    print("Sent EOS message.")

    producer.close()

if __name__ == "__main__":
    kafka_producer()
