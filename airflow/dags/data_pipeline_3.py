# for testing
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListPrefixesOperator


from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
    )

from airflow.providers.amazon.aws.sensors.emr import (
    EmrJobFlowSensor,
    EmrStepSensor
)

# from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from operators.s3_to_redshift import S3ToRedshiftOperator
from helpers.check_data_quality import (
    non_empty_check,
    unique_values_check,
)
from configurations.dp_2_configuration import (
    CREATE_ALL_TABLES
)


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
FILES= {'pedestrian_counts': 'b2ak-trbp', 'sensor_info': 'h57g-5234'}

BUCKET_NAME = 'vc-s3bucket-pedestrian-sensor'

TABLES = {"fact_top_10_by_day":["date_time", "sensor_id", "daily_counts"] ,
            "fact_top_10_by_month":["sensor_id", "monthly_counts", "date_time"],
            "fact_sensor_by_year":["sensor_id", "counts_2020","counts_2021", "counts_2022"],
            "dim_datetime":["date_time", "year","month", "date"],
            "dim_sensor_info":["sensor_id", "sensor_description","sensor_name",
                                "installation_date", "status", "note", "direction_1",
                                "direction_2", "latitude", "longitude"]}

copy_options = ["FORMAT AS PARQUET"]

def map_files_for_upload(filename:str):
    if filename.rsplit(".", 1)[-1] in ("parquet"):
        return filename
    raise AirflowSkipException(f"Skip upload: {filename}")



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
    # start_dp_pedestrian_sensor_daily = DummyOperator(task_id="start_dp_pedestrian_sensor_daily")
    with TaskGroup(group_id='s3_to_redshift') as s3_to_redshift:
        create_tables_on_redshift = PostgresOperator(
            task_id="create_tables_on_redshift",
            postgres_conn_id="cuonghtv_pg_redshift_conn",
            sql=CREATE_ALL_TABLES
        )

        start = DummyOperator(task_id="start")
        end = DummyOperator(task_id="end")

        for table_name, table_cols in TABLES.items():
            

            list_files_S3 = S3ListOperator(
                task_id=f"list_files_S3_{table_name}",
                aws_conn_id="cuonghtv_aws_conn",
                bucket=BUCKET_NAME,
                prefix=f"data/cleaned/{table_name}/",
                delimiter="/",
            )

            copy_S3_to_Redshift = S3ToRedshiftOperator.partial(
                task_id=f"copy_{table_name}_to_Redshift",
                schema="public",
                table=f"{table_name}",
                column_list=table_cols,
                s3_bucket=BUCKET_NAME,
                redshift_conn_id="cuonghtv_redshift_conn",
                aws_conn_id="cuonghtv_aws_conn",
                method="REPLACE",
                copy_options=copy_options
            ).expand(s3_key=list_files_S3.output.map(map_files_for_upload))

            start >> create_tables_on_redshift >> copy_S3_to_Redshift >> end


    # check_non_empty = PythonOperator(
    #     task_id=f"check_non_empty",
    #     python_callable=non_empty_check,
    #     provide_context=True,
    #     params={"postgres_conn_id": "cuonghtv_redshift_conn","tables": TABLES.keys()}
    # )

    s3_to_redshift


    # finish_dp_pedestrian_sensor_daily = DummyOperator(task_id="finish_dp_pedestrian_sensor_daily")


