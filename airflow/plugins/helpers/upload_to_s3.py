from airflow.hooks.S3_hook import S3Hook

def upload_to_s3(bucket, target_name, local_file) -> None:
    s3_hook = S3Hook('cuonghtv_aws_conn')
    s3_hook.load_file(filename=local_file, key=target_name, bucket_name=bucket, replace=True)