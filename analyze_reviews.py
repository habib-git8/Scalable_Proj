import pandas as pd
import re
from collections import defaultdict, Counter
from textblob import TextBlob
from multiprocessing import Pool, cpu_count
import time
import matplotlib.pyplot as plt
import boto3

# === Helper Functions ===
def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

def count_words_chunk(chunk):
    counts = defaultdict(int)
    for review in chunk:
        for word in tokenize(review):
            counts[word] += 1
    return counts

def analyze_sentiment_chunk(chunk):
    result = {"positive": 0, "negative": 0, "neutral": 0}
    for review in chunk:
        polarity = TextBlob(review).sentiment.polarity
        if polarity > 0:
            result["positive"] += 1
        elif polarity < 0:
            result["negative"] += 1
        else:
            result["neutral"] += 1
    return result

# === Main Program ===
if __name__ == "__main__":
    # Load data
    df = pd.read_csv("amazon_reviews.csv", low_memory=False)
    texts = df["reviews.text"].dropna().tolist()
    print(f"✅ Loaded {len(texts)} reviews.\n")

    # === 1️⃣ Serial word count
    start_serial = time.time()
    word_counts_serial = defaultdict(int)
    for review in texts:
        for word in tokenize(review):
            word_counts_serial[word] += 1
    end_serial = time.time()
    print(f"⏱️ Serial word count time: {end_serial - start_serial:.2f} seconds")

    # === 2️⃣ Parallel word count
    start_parallel = time.time()
    num_chunks = cpu_count()
    chunk_size = len(texts) // num_chunks
    chunks = [texts[i:i + chunk_size] for i in range(0, len(texts), chunk_size)]

    with Pool() as pool:
        word_results = pool.map(count_words_chunk, chunks)

    final_word_counts = defaultdict(int)
    for wc in word_results:
        for word, count in wc.items():
            final_word_counts[word] += count

    sorted_words = sorted(final_word_counts.items(), key=lambda x: x[1], reverse=True)
    pd.DataFrame(sorted_words, columns=["word", "count"]).to_csv("word_counts_parallel.csv", index=False)
    end_parallel = time.time()
    print(f"⚡ Parallel word count time: {end_parallel - start_parallel:.2f} seconds")

    # === 3️⃣ Parallel sentiment analysis
    start_sentiment = time.time()
    with Pool() as pool:
        sentiment_chunks = pool.map(analyze_sentiment_chunk, chunks)

    final_sentiment = Counter()
    for s in sentiment_chunks:
        final_sentiment.update(s)

    pd.DataFrame(list(final_sentiment.items()), columns=["sentiment", "count"]).to_csv("sentiment_summary.csv", index=False)
    end_sentiment = time.time()
    print(f"🧠 Sentiment analysis (parallel) time: {end_sentiment - start_sentiment:.2f} seconds")
    # === 4️⃣b Plot Sentiment Summary ===
    sentiments = list(final_sentiment.keys())
    counts = list(final_sentiment.values())

    plt.figure(figsize=(8, 5))
    plt.bar(sentiments, counts)
    plt.title("Sentiment Analysis Summary")
    plt.xlabel("Sentiment")
    plt.ylabel("Number of Reviews")
    plt.tight_layout()

    # Save to file
    sentiment_plot_path = "sentiment_summary_plot.png"
    plt.savefig(sentiment_plot_path)
    print(f"✅ Saved sentiment plot as {sentiment_plot_path}")

    # === Upload sentiment plot to S3
    bucket_name = "amazon-reviews-project24170658"
    sentiment_s3_key = "output/sentiment_summary_plot.png"
    s3 = boto3.client("s3")
    try:
        s3.upload_file(sentiment_plot_path, bucket_name, sentiment_s3_key)
        print(f"✅ Uploaded sentiment plot to s3://{bucket_name}/{sentiment_s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload sentiment plot to S3: {e}")


    # === 4️⃣ Plot Top 10 Words
    top_words = sorted_words[:10]
    words, counts = zip(*top_words)

    plt.figure(figsize=(12, 6))
    plt.bar(words, counts)
    plt.xticks(rotation=45, ha='right')
    plt.title("Top 10 Most Frequent Words in Reviews")
    plt.xlabel("Words")
    plt.ylabel("Frequency")
    plt.tight_layout()

    # Save to file
    plot_path = "top_words_plot.png"
    plt.savefig(plot_path)
    print(f"\n✅ Saved plot as {plot_path}")

    # === 5️⃣ Upload to S3
    bucket_name = "amazon-reviews-project24170658"  # ← your bucket
    s3_key = "output/top_words_plot.png"

    s3 = boto3.client("s3")
    try:
        s3.upload_file(plot_path, bucket_name, s3_key)
        print(f"✅ Uploaded plot to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")

    # === Summary
    print("\n🔁 Top 10 words:")
    for word, count in top_words[:10]:
        print(f"{word}: {count}")

    print("\n🧠 Sentiment Summary:")
    for sentiment, count in final_sentiment.items():
        print(f"{sentiment.capitalize()}: {count}")