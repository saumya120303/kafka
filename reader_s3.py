import pandas as pd
from kafka import KafkaProducer
import json
import time
import boto3
from io import StringIO
import os
from datetime import datetime

def read_csv_from_s3(bucket_name, file_key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),  
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),  
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')  
    )
    # Get the CSV file from S3 and load it into a pandas DataFrame
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    return df

def read_and_push(bucket_name, file_key):
    df = read_csv_from_s3(bucket_name, file_key)
    # Sorting by week_number
    df.sort_values(by='week_number', inplace=True)
    
    # Add timestamp column
    # df['timestamp'] = datetime.now().isoformat()
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for index, row in df.iterrows():
        message = {
            'msisdn': row['msisdn'],
            'week_number': row['week_number'],
            'revenue_usd': row['revenue_usd']
            # 'timestamp': row['timestamp']
        }
        producer.send('rev1_s3', value=message)
        producer.flush()
        print(f"Sent: {message}")
        # time.sleep(interval)  # Pause for the specified interval

if __name__ == "__main__":
   # interval = 2  # Interval in seconds; this can be configured as needed
    bucket_name = 'revenue12'  # Name of the S3 bucket
    file_key = 'rev1.csv'  # Correct key of the file in the S3 bucket (without URL)
    read_and_push(bucket_name, file_key)
