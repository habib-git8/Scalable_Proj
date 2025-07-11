import boto3
import json
import time
import nltk
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
from nltk.tokenize import word_tokenize
from textblob import TextBlob
import os

nltk.download('punkt')

# --- AWS Configuration ---
STREAM_NAME = 'amazon-reviews-stream'
REGION = 'us-east-1'
BUCKET_NAME = 'amazon-reviews-project24170658'  # 🔁 Replace this with your actual bucket name
S3_FOLDER = 'images/'

# --- Initialize clients ---
client = boto3.client('kinesis', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

# --- Get shard iterator ---
shard_id = client.describe_stream(StreamName=STREAM_NAME)['StreamDescription']['Shards'][0]['ShardId']
shard_iterator = client.get_shard_iterator(
    StreamName=STREAM_NAME,
    ShardId=shard_id,
    ShardIteratorType='TRIM_HORIZON'
)['ShardIterator']

review_data = []
word_counter = Counter()

print("📡 Streaming and generating dashboard PNGs...")

while True:
    try:
        response = client.get_records(ShardIterator=shard_iterator, Limit=100)
        shard_iterator = response['NextShardIterator']
        records = response['Records']

        for record in records:
            try:
                data = json.loads(record['Data'])
                review = data.get("reviews.text", "").strip()
                if review == "":
                    continue
                sentiment = TextBlob(review).sentiment.polarity
                review_data.append({"review_text": review, "sentiment_score": sentiment})
                tokens = word_tokenize(review.lower())
                words = [word for word in tokens if word.isalpha()]
                word_counter.update(words)
            except Exception as e:
                print(f"❌ Error parsing: {e}")

        if len(review_data) >= 50:
            df = pd.DataFrame(review_data)

            plt.figure(figsize=(14, 6))
            plt.subplot(1, 2, 1)
            top_words = word_counter.most_common(10)
            if top_words:
                words, counts = zip(*top_words)
                plt.bar(words, counts, color='skyblue')
                plt.title("Top 10 Words")
                plt.xticks(rotation=45)

            plt.subplot(1, 2, 2)
            plt.hist(df["sentiment_score"], bins=30, color='lightgreen', edgecolor='black')
            plt.title("Sentiment Score Distribution")
            plt.xlabel("Sentiment Polarity")
            plt.ylabel("Frequency")

            avg_sentiment = df["sentiment_score"].mean()
            plt.suptitle(f"📊 Dashboard Snapshot — Avg Sentiment: {avg_sentiment:.2f}", fontsize=14)
            plt.tight_layout(rect=[0, 0, 1, 0.95])

            timestamp = int(time.time())
            filename = f"dashboard_snapshot_{timestamp}.png"
            local_path = os.path.join("/tmp", filename)
            s3_key = f"{S3_FOLDER}{filename}"

            plt.savefig(local_path)
            print(f"✅ Saved to local file: {local_path}")
            plt.close()

            # Upload to S3
            try:
                s3.upload_file(local_path, BUCKET_NAME, s3_key)
                print(f"🚀 Uploaded to s3://{BUCKET_NAME}/{s3_key}")
            except Exception as e:
                print(f"❌ Failed to upload to S3: {e}")

            review_data.clear()
            word_counter.clear()

        time.sleep(1)

    except KeyboardInterrupt:
        print("🛑 Dashboard stopped.")
        break
