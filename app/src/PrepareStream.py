import pandas as pd

INPUT_FINAL = "/app/final_data_set/final_dataset.csv"     
STREAM_CSV = "/app/streaming_data/stream.csv"               
REMAINING_95_CSV = "/app/streaming_data/remaining_95.csv"   

STREAM_FRACTION = 0.05
RANDOM_SEED = 42

def prepare_stream():
    df = pd.read_csv(INPUT_FINAL)

    stream_df = df.sample(frac=STREAM_FRACTION, random_state=RANDOM_SEED)
    stream_df.to_csv(STREAM_CSV, index=False)

   
    remaining_df = df.drop(stream_df.index).reset_index(drop=True)
    remaining_df.to_csv(REMAINING_95_CSV, index=False)


    print(f"5% stream data saved → {STREAM_CSV} ({len(stream_df)} rows)")
    print(f"95% remaining data saved → {REMAINING_95_CSV} ({len(remaining_df)} rows)")

# if __name__ == "__main__":
#     prepare()
