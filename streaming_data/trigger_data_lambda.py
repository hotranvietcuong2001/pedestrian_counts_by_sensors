import json
import boto3
import base64
from datetime import datetime

BUCKET_NAME = 'vc-s3bucket-pedestrian-sensor'

def lambda_handler(event, context):
    # TODO implement
    
    for record in event["Records"]:
        data = record['kinesis']['data']
        data=base64.b64decode(data)
        parse_json = json.loads(data)
        json_object = json.dumps(parse_json, indent=4)
        
        now = datetime.now()
        s3_client = boto3.client('s3')
        s3_client.put_object(Body=json_object, Bucket=BUCKET_NAME, Key=f'data/raw/pedestrian_counts/{now}.json')
    return {
        'statusCode': 200,
        'body': 'Write S3 successfullly'
    }