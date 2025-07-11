import streamlit as st
import boto3
import json
import time

# --- Config ---
STREAM_NAME = 'amazon-reviews-stream'
REGION = 'us-east-1'
REFRESH_INTERVAL = 2  # seconds
MAX_REVIEWS = 5

# --- Kinesis Setup ---
client = boto3.client("kinesis", region_name=REGION)
shard_id = client.describe_stream(StreamName=STREAM_NAME)['StreamDescription']['Shards'][0]['ShardId']
shard_iterator = client.get_shard_iterator(
    StreamName=STREAM_NAME,
    ShardId=shard_id,
    ShardIteratorType='LATEST'  # Only get newest reviews
)['ShardIterator']

# --- Streamlit UI ---
st.set_page_config(page_title="📡 Live Amazon Reviews", layout="centered")
st.title("📝 Real-Time Incoming Reviews from Kinesis")

review_display = st.empty()
review_buffer = []

while True:
    response = client.get_records(ShardIterator=shard_iterator, Limit=100)
    records = response['Records']
    shard_iterator = response['NextShardIterator']

    for record in records:
        try:
            data = json.loads(record['Data'])
            review = data.get("reviews.text", "").strip()
            if review:
                review_buffer.insert(0, review[:200])  # Show latest at top
                review_buffer = review_buffer[:MAX_REVIEWS]
        except Exception as e:
            st.error(f"❌ Error: {e}")

    with review_display.container():
        st.subheader("📥 Live Streamed Reviews")
        for idx, review in enumerate(review_buffer):
            st.markdown(f"**{idx+1}.** {review}")

    time.sleep(REFRESH_INTERVAL)