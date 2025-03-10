import boto3
from io import StringIO
import pandas as pd
from datetime import datetime

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def upload_to_s3(df, bucket_name, object_key):
    """Upload DataFrame to S3 bucket."""
    try:
        log(f"Starting upload to S3 bucket {bucket_name}/{object_key}")
        log(f"DataFrame shape: {df.shape}")
        
        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        log("DataFrame converted to CSV successfully")
        
        # Create S3 client with explicit credentials
        log("Creating S3 client...")
        s3_client = boto3.client('s3')
        log("S3 client created successfully")
        
        # Upload to S3
        log("Attempting to upload to S3...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=csv_buffer.getvalue()
        )
        
        log(f"Successfully uploaded to s3://{bucket_name}/{object_key}")
        return f"s3://{bucket_name}/{object_key}"
    except Exception as e:
        log(f"Error uploading to S3: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def read_from_s3(bucket_name, object_key):
    """Read a CSV file from S3 bucket into a DataFrame."""
    try:
        log(f"Reading data from s3://{bucket_name}/{object_key}")
        
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        
        # Read the CSV content from the response
        return pd.read_csv(response['Body'])
    except Exception as e:
        log(f"Error reading from S3: {e}")
        raise
