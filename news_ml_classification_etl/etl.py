import json
from datetime import datetime, timedelta
import io
import logging 

import boto3
import pandas as pd

DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

schema = ['title', 'topic']
s3 = boto3.client('s3')
config = {}


def gather_news():
    paginator = s3.get_paginator('list_objects_v2') 
    bucket = config['FROM_S3_BUCKET']

    pages = paginator.paginate(Bucket=bucket, Prefix=DATE)
    for page in pages:
        if not 'Contents' in page:
            continue

        for obj in page['Contents']:
            # Check if the object is a JSON file
            if not obj['Key'].endswith('.json'):
                # Read the contents of the object and decode the JSON data
                json_data = json.loads(s3.get_object(Bucket=bucket,
                                                      Key=obj['Key'])['Body']
                                                      .read().decode('utf-8'))
                yield json_data

                
            
def transform_news(csv_buffer):
    data = dict.fromkeys(schema, list())
    for article in gather_news():
        for key in data.keys():
            data[key].append(article[key])

    return pd.DataFrame(data).to_csv(csv_buffer)

def main():
    bucket = config['TO_S3_BUCKET']
    file_name = f'{DATE}-news.csv'

    # Create a buffer to hold the transformed data
    csv_buffer = io.StringIO()
    transform_news(csv_buffer)

    # Upload the file to S3
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=file_name)


def lambda_handler(event, _):
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    config["FROM_S3_BUCKET"] = event["FROM_S3_BUCKET"]
    config["TO_S3_BUCKET"] = event["TO_S3_BUCKET"]

    main()
