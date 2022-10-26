from hooks.s3 import S3Hook


def chunks(lst, n):
    """
    It takes a list and a number, and returns a generator that yields chunks of the list of the given
    size
    
    :param lst: the list to be split
    :param n: the number of items in each chunk
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def upload_to_s3(bucket, target_name, local_file, aws_conn_id) -> None:
    """
    It uploads a local file to an S3 bucket
    
    :param bucket: The name of the S3 bucket
    :param target_name: The name of the file in S3
    :param local_file: The local file path to the file you want to upload to S3
    :param aws_conn_id: The Airflow connection ID for the AWS credentials
    """
    s3_hook = S3Hook(aws_conn_id)
    s3_hook.load_file(filename=local_file, key=target_name, bucket_name=bucket, replace=True)