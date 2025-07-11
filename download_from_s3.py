import boto3

# --- CONFIGURATION ---
bucket_name = "amazon-reviews-project24170658"        # 🔁 Replace with your actual bucket
s3_key = "input/amazon_reviews.csv"  # 🔁 Replace with actual object key
local_file = "amazon_reviews.csv"          # File to save locally on EC2

# --- S3 DOWNLOAD ---
s3 = boto3.client("s3")

try:
    s3.download_file(bucket_name, s3_key, local_file)
    print(f"✅ File downloaded to EC2: {local_file}")
except Exception as e:
    print(f"❌ Failed to download file: {e}")