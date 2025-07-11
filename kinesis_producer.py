import boto3
import pandas as pd
import json
import time

# --- Configuration ---
STREAM_NAME = "amazon-reviews-stream"     # Make sure this exists in AWS Kinesis
REGION = "us-east-1"                     # Replace with your actual region
CSV_PATH = "amazon_reviews.csv"          # Your CSV file path

# --- Create Kinesis client ---
kinesis = boto3.client("kinesis", region_name=REGION)

# --- Read CSV ---
try:
    df = pd.read_csv(CSV_PATH)
    print(f"✅ Loaded {len(df)} reviews.")
except Exception as e:
    print(f"❌ Failed to load CSV: {e}")
    exit(1)

# --- Filter for reviewText ---
if "reviews.text" not in df.columns:
    print("❌ Column 'reviewText' not found in CSV.")
    exit(1)

# Drop NaNs
reviews = df["reviews.text"].dropna().tolist()

# --- Push to Kinesis ---
print("🚀 Starting to send reviews to Kinesis...")
for review in reviews:
    review = str(review).strip()
    if review == "":
        continue

    payload = json.dumps({"reviews.text": review})
    try:
        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=payload,
            PartitionKey="partitionKey"
        )
        print("✅ Pushed:", review[:80])  # Truncate long output
    except Exception as e:
        print(f"❌ Failed to push review: {e}")
    
    time.sleep(1)  # Adjust if you want faster/lower rate

print("🏁 Finished sending all reviews.")