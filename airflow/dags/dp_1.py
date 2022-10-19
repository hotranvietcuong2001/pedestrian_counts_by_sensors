
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

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.S3_hook import S3Hook


from configurations.dp_1_configuration import (
    SPARK_STEPS,
    JOB_FLOW_OVERRIDES,
    CREATE_ALL_TABLES
)


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
FILES= {'pedestrian_counts': 'b2ak-trbp', 'sensor_info': 'h57g-5234'}

BUCKET_NAME = 'vc-s3bucket-pedestrian-sensor'

copy_options = ["FORMAT AS PARQUET"]

def upload_to_s3(bucket, target_name, local_file) -> None:
    s3_hook = S3Hook('cuonghtv_aws_conn')
    s3_hook.load_file(filename=local_file, key=target_name, bucket_name=bucket, replace=True)




default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="data_pipeline_1",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['pedestrian_sensor'],
) as dag:
    start_dp_pedestrian_sensor_daily = DummyOperator(task_id="start_dp_pedestrian_sensor_daily")
    

    with TaskGroup(group_id='download_to_local_and_upload_to_S3') as ingest_data_to_s3:

        start = DummyOperator(task_id="start")
        end = DummyOperator(task_id="end")

        for FTYPE, FNAME in FILES.items():

            download_dataset_task = BashOperator(
                task_id=f"download_{FTYPE}_dataset_task",
                bash_command=f"curl -sS https://data.melbourne.vic.gov.au/api/views/{FNAME}/rows.csv?accessType=DOWNLOAD > {path_to_local_home}/{FTYPE}.csv"
            )

            
            upload_to_s3_task = PythonOperator(
                task_id=f"{FTYPE}_upload_to_s3_task",
                python_callable=upload_to_s3,
                op_kwargs={
                    "bucket": BUCKET_NAME,
                    "target_name": f"data/raw/{FTYPE}.csv",
                    "local_file": f"{path_to_local_home}/{FTYPE}.csv",
                },
            )

            delete_dataset_task = BashOperator(
                task_id=f"delete_local_{FTYPE}_dataset_task",
                bash_command=f"rm {path_to_local_home}/{FTYPE}.csv"
            )

            start >> download_dataset_task >> upload_to_s3_task >> delete_dataset_task >> end
    
    

    with TaskGroup(group_id='ingest_data_and_create_cluster') as ingest_data_and_create_cluster:
        
        start = DummyOperator(task_id="start")
        
        create_emr_cluster = EmrCreateJobFlowOperator(
            task_id="create_emr_cluster",
            job_flow_overrides=JOB_FLOW_OVERRIDES,
            aws_conn_id="cuonghtv_aws_conn",
            emr_conn_id="cuonghtv_emr_conn",
        )
        
        check_create_job_flow = EmrJobFlowSensor(
            task_id='check_create_job_flow',
            job_flow_id=f"{create_emr_cluster.output}",
            aws_conn_id="cuonghtv_aws_conn",
            target_states=['RUNNING', 'WAITING']
        )

        end = DummyOperator(task_id="end")

        start >> ingest_data_to_s3 >> end
        start >> create_emr_cluster >> check_create_job_flow >> end
    

    with TaskGroup(group_id='pyspark_in_emr') as pyspark_in_emr:

        add_steps = EmrAddStepsOperator(
            task_id="add_steps",
            job_flow_id=f"{create_emr_cluster.output}",
            aws_conn_id="cuonghtv_aws_conn",
            steps=SPARK_STEPS,
            params={
                "BUCKET_NAME": BUCKET_NAME,
            },
        )

        last_step = len(SPARK_STEPS) - 1
        step_checker = EmrStepSensor(
            task_id="step_checker",
            job_flow_id=f"{create_emr_cluster.output}",
            step_id="{{ task_instance.xcom_pull(task_ids='pyspark_in_emr.add_steps', key='return_value')["+ str(last_step)+ "] }}",
            aws_conn_id="cuonghtv_aws_conn",
        )

        terminate_emr_cluster = EmrTerminateJobFlowOperator(
            task_id="terminate_emr_cluster",
            job_flow_id=f"{create_emr_cluster.output}",
            aws_conn_id="cuonghtv_aws_conn",
        )
        
        add_steps >> step_checker >> terminate_emr_cluster

    with TaskGroup(group_id='s3_to_redshift') as s3_to_redshift:
        create_tables_on_redshift = PostgresOperator(
            task_id="create_tables_on_redshift",
            postgres_conn_id="cuonghtv_pg_redshift_conn",
            sql=CREATE_ALL_TABLES
        )

        copy_fact_top_10_by_day = S3ToRedshiftOperator(
            task_id="copy_fact_top_10_by_day",
            schema="public",
            table="fact_top_10_by_day",
            column_list=[
                "date_time", "sensor_id", "daily_counts"],
            s3_bucket=BUCKET_NAME,
            s3_key="data/cleaned/fact_top_10_by_day/",
            redshift_conn_id="cuonghtv_redshift_conn",
            aws_conn_id="cuonghtv_aws_conn",
            copy_options=copy_options
        )

        copy_fact_top_10_by_month = S3ToRedshiftOperator(
            task_id="copy_fact_top_10_by_month",
            schema="public",
            table="fact_top_10_by_month",
            column_list=[
                "sensor_id", "monthly_counts", "date_time"],
            s3_bucket=BUCKET_NAME,
            s3_key="data/cleaned/fact_top_10_by_month/",
            redshift_conn_id="cuonghtv_redshift_conn",
            aws_conn_id="cuonghtv_aws_conn",
            copy_options=copy_options
        )

        copy_fact_sensor_by_year = S3ToRedshiftOperator(
            task_id="copy_fact_sensor_by_year",
            schema="public",
            table="fact_sensor_by_year",
            column_list=[
                "sensor_id", "counts_2020","counts_2021", "counts_2022"],
            s3_bucket=BUCKET_NAME,
            s3_key="data/cleaned/fact_sensor_by_year/",
            redshift_conn_id="cuonghtv_redshift_conn",
            aws_conn_id="cuonghtv_aws_conn",
            copy_options=copy_options
        )

        copy_dim_datetime = S3ToRedshiftOperator(
            task_id="copy_dim_datetime",
            schema="public",
            table="dim_datetime",
            column_list=[
                "date_time", "year","month", "date"],
            s3_bucket=BUCKET_NAME,
            s3_key="data/cleaned/dim_datetime/",
            redshift_conn_id="cuonghtv_redshift_conn",
            aws_conn_id="cuonghtv_aws_conn",
            method="REPLACE",
            copy_options=copy_options
        )

        copy_dim_sensor_info = S3ToRedshiftOperator(
            task_id="copy_dim_sensor_info",
            schema="public",
            table="dim_sensor_info",
            column_list=[
                "sensor_id", "sensor_description","sensor_name",
                "installation_date", "status", "note", "direction_1",
                "direction_2", "latitude", "longitude"],
            s3_bucket=BUCKET_NAME,
            s3_key="data/cleaned/dim_sensor_info/",
            redshift_conn_id="cuonghtv_redshift_conn",
            aws_conn_id="cuonghtv_aws_conn",
            copy_options=copy_options
        )

        create_tables_on_redshift >> [
            copy_fact_top_10_by_day, copy_fact_top_10_by_month, 
            copy_fact_sensor_by_year, copy_dim_datetime, copy_dim_sensor_info]


    finish_dp_pedestrian_sensor_daily = DummyOperator(task_id="finish_dp_pedestrian_sensor_daily")


start_dp_pedestrian_sensor_daily >> ingest_data_and_create_cluster >> pyspark_in_emr >> s3_to_redshift >> finish_dp_pedestrian_sensor_daily