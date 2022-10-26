"""Import library/framework"""
import requests
import json
import time
import boto3


kinesis_client = boto3.client('kinesis')
KINESIS_RETRY_COUNT = 10
KINESIS_RETRY_WAIT_IN_SEC = 0.1
KINESIS_STREAM_NAME = "vc-pedestrian-counts-stream"


"""Call API to get number of rows"""
metadata_url = 'https://data.melbourne.vic.gov.au/api/views/b2ak-trbp.json'

# request
meta_res = requests.get(metadata_url)
meta_data = meta_res.text
meta_json = json.loads(meta_data)
number_of_records = int(meta_json["columns"][0]["cachedContents"]["count"])

# number of records for putting data to kinesis stream
size_of_throughput = 1000

# read env file to get current row
with open('.env', 'r') as fi:
    curr = json.loads(fi.read())

# current row
curr_record = int(curr['current_record'])

# current file in kinesis stream
curr_file = int(curr['current_file'])



def send_to_stream(kinesis_records, retry_count):
    """
    This function help you put record on Kinesis Stream.
    
    :param kinesis_records: The list of records to send to Kinesis
    :param retry_count: The number of times to retry sending the records to Kinesis
    """
    
    put_response = kinesis_client.put_records(
        Records=kinesis_records,
        StreamName=KINESIS_STREAM_NAME
    )

    failed_count = put_response['FailedRecordCount']

    if failed_count > 0:
        if retry_count > 0:
            retry_kinesis_records = []
            for idx, record in enumerate(put_response['Records']):
                if 'ErrorCode' in record:
                    retry_kinesis_records.append(kinesis_records[idx])
            time.sleep(KINESIS_RETRY_WAIT_IN_SEC * (KINESIS_RETRY_COUNT - retry_count + 1))
            send_to_stream(retry_kinesis_records, retry_count - 1)
        else:
            print(f'Not able to put records after retries. Records = {put_response["Records"]}')

if __name__ == "__main__":
    while True:
        try:
            print(f'Get data at offset {curr_record}')
            url = f'https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json?$limit={size_of_throughput}&$offset={curr_record}&$order=date_time%20ASC&$where=date_time%20IS%20NOT%20NULL'
            print(url)
            res = requests.get(url)
            if res.text.rstrip() != '[]':
                parse_json = json.loads(res.text)
                json_object = json.dumps(parse_json)
                kinesis_records = [
                    {
                        'Data': json_object,
                        'PartitionKey': 'Testing'
                    }
                ]
    
                send_to_stream(kinesis_records, 5)
    
                curr_record = curr_record + len(parse_json)
                with open(".env", "w") as writer_curr_record:
                    curr_record_json = {"current_record": curr_record}
                    json.dump(curr_record_json, writer_curr_record)
                
                print('Success')
            else:
                time.sleep(10)
        except requests.HTTPError as e:
            print(f"[!] Exception caught: {e}")
            time.sleep(5)