import streamlit as st 
import boto3
import json
import time
import nltk
import matplotlib.pyplot as plt
from collections import Counter
from nltk.tokenize import word_tokenize
from textblob import TextBlob

nltk.download('punkt')

# --- AWS Kinesis config ---
STREAM_NAME = 'amazon-reviews-stream'
REGION = 'us-east-1'
client = boto3.client('kinesis', region_name=REGION)

shard_id = client.describe_stream(StreamName=STREAM_NAME)['StreamDescription']['Shards'][0]['ShardId']
shard_iterator = client.get_shard_iterator(
    StreamName=STREAM_NAME,
    ShardId=shard_id,
    ShardIteratorType='TRIM_HORIZON'
)['ShardIterator']

# --- Streamlit config ---
st.set_page_config(page_title="📡 Real-Time Amazon Review Dashboard", layout="wide")
st.title("📡 Real-Time Amazon Reviews Dashboard")

placeholder = st.empty()

word_counts = Counter()
sentiment_scores = []

while True:
    response = client.get_records(ShardIterator=shard_iterator, Limit=100)
    records = response['Records']
    shard_iterator = response['NextShardIterator']

    for record in records:
        try:
            data = json.loads(record['Data'])
            review = data.get('reviews.text', '')
            if not review.strip():
                continue

            # Tokenize words
            tokens = word_tokenize(review.lower())
            words = [word for word in tokens if word.isalpha()]
            word_counts.update(words)

            # Sentiment score
            sentiment = TextBlob(review).sentiment.polarity
            sentiment_scores.append(sentiment)

        except Exception as e:
            st.error(f"❌ Error parsing record: {e}")

    top_words = word_counts.most_common(5)

    with placeholder.container():
        st.markdown("### 🔝 Top 5 Words")
        for word, count in top_words:
            st.write(f"**{word}**: {count}")

        col1, col2 = st.columns(2)

        with col1:
            if top_words:
                words, counts = zip(*top_words)
                fig, ax = plt.subplots()
                ax.bar(words, counts, color='skyblue')
                ax.set_title("Top 5 Word Frequencies")
                ax.set_ylabel("Frequency")
                st.pyplot(fig)

        with col2:
            if sentiment_scores:
                fig2, ax2 = plt.subplots()
                ax2.hist(sentiment_scores, bins=20, color='lightgreen', edgecolor='black')
                ax2.set_title("Sentiment Score Distribution")
                ax2.set_xlabel("Sentiment Polarity")
                ax2.set_ylabel("Frequency")
                st.pyplot(fig2)

        # Show average sentiment
        if sentiment_scores:
            avg_score = sum(sentiment_scores) / len(sentiment_scores)
            st.markdown(f"### 🧠 Average Sentiment Score: `{avg_score:.2f}`")

    # Reset counters for the next update cycle
    word_counts.clear()
    sentiment_scores.clear()
    time.sleep(10)