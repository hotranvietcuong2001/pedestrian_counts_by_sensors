
"""Import"""
import os
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
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

from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor

from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.postgres_operator import PostgresOperator


from operators.s3 import (
    S3DeleteObjectsOperator
)

from helpers.redshift import map_files_for_upload

"""All about configurations"""

# for creating cluster EMR, running spark job and creating table on redshift
from configurations.dp_1_configuration import (
    SPARK_STEPS,
    JOB_FLOW_OVERRIDES,
    CREATE_ALL_TABLES
)

# path workdir
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

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
    dag_id="data_pipeline_1",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['pedestrian_sensor', "streaming_data"],
) as dag:

    start_dp_pedestrian_sensor_daily = DummyOperator(task_id="start_dp_pedestrian_sensor_daily")
    
    delete_old_file_S3 = S3DeleteObjectsOperator(
        task_id="delete_old_file_S3",
        aws_conn_id="cuonghtv_aws_conn",
        bucket=BUCKET_NAME,
        prefix=f"data/cleaned/",
    )

    with TaskGroup(group_id='pyspark_in_emr') as pyspark_in_emr:

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
        
        create_emr_cluster >> check_create_job_flow >> add_steps >> \
            step_checker >> terminate_emr_cluster


    with TaskGroup(group_id='s3_to_redshift') as s3_to_redshift:
        
        create_tables_on_redshift = PostgresOperator(
            task_id="create_tables_on_redshift",
            postgres_conn_id="cuonghtv_pg_redshift_conn",
            sql=CREATE_ALL_TABLES
        )

        check_available_redshift = RedshiftClusterSensor(
            task_id = "check_available_redshift",
            cluster_identifier = "vc-pedestrian-sensor-redshift-cluster",
            target_status = "available",
            aws_conn_id='cuonghtv_aws_conn'
        )

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

            create_tables_on_redshift >> check_available_redshift >> copy_S3_to_Redshift

    finish_dp_pedestrian_sensor_daily = DummyOperator(task_id="finish_dp_pedestrian_sensor_daily")


start_dp_pedestrian_sensor_daily >> delete_old_file_S3 >> pyspark_in_emr >> \
    s3_to_redshift >> finish_dp_pedestrian_sensor_daily