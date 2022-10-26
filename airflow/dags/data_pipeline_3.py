
"""Testing"""

"""Import"""
import os
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
    )

from airflow.providers.amazon.aws.sensors.emr import (
    EmrJobFlowSensor,
    EmrStepSensor
)

from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.postgres_operator import PostgresOperator

from operators.s3 import (
    S3DeleteObjectsOperator
)

from helpers.redshift import (
    map_files_for_upload,
)

from helpers.s3 import (
    upload_to_s3,
)

"""All about configurations"""

# for creating cluster EMR, running spark job and creating table on redshift
from configurations.dp_1_configuration import (
    SPARK_STEPS,
    JOB_FLOW_OVERRIDES,
    CREATE_ALL_TABLES
)

# path workdir
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# file's name and id
FILES= {'pedestrian_counts': 'b2ak-trbp', 'sensor_info': 'h57g-5234'}

# S3 bucket's name
BUCKET_NAME = 'vc-pedestrian-sensor-s3bucket'

# tables on Redshift and columns of it
TABLES = {"fact_top_10_by_day":["date_time", "sensor_id", "daily_counts"] ,
            "fact_top_10_by_month":["sensor_id", "monthly_counts", "date_time"],
            "fact_sensor_by_year":["sensor_id", "counts_2020","counts_2021", "counts_2022"],
            "dim_datetime":["date_time", "year","month", "date"],
            "dim_sensor_info":["sensor_id", "sensor_description","sensor_name",
                                "installation_date", "status", "note", "direction_1",
                                "direction_2", "latitude", "longitude"]}

# copy option to deal format parquet files on Redshift
copy_options = ["FORMAT AS PARQUET"]


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="data_pipeline_3",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['pedestrian_sensor', "testing"],
) as dag:

    delete_file_S3 = S3DeleteObjectsOperator(
        task_id=f"list_files_S3",
        aws_conn_id="cuonghtv_aws_conn",
        bucket=BUCKET_NAME,
        prefix=f"data/cleaned/",
    )

delete_file_S3





