import boto3

def upload_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, s3_key)
    print(f"✅ Uploaded {file_path} to s3://{bucket_name}/{s3_key}")

# Replace with your actual file
upload_to_s3("amazon_reviews.csv", "amazon-reviews-project24170658", "input/amazon_reviews.csv")
