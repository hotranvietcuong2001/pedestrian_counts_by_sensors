import os
from airflow import DAG
from airflow.utils.dates import days_ago


# Our airflow operators
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator



from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# Take environmental variables into local variables
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

 
# Take environmental variables into local variables
FILES= {'pedestrian_counts': 'b2ak-trbp', 'sensor_info': 'h57g-5234'}
FILES_LIST = list(FILES.keys())
BUCKET_NAME = 'vc-s3bucket-pedestrian-sensor'


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


JOB_FLOW_OVERRIDES = {
    "Name": "vc-cluster-Pedestrian_Sensors",
    "ReleaseLabel": "emr-6.8.0",
    "LogUri":f"s3://{BUCKET_NAME}/emr-logs",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                },
            ],

        },
    ],
    "Instances": {
        "Ec2KeyName":"vc-aws",
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "AutoTerminationPolicy": { 
      "IdleTimeout": 3600
    },
    "JobFlowRole": "DataCamp_EC2_EMR_role",
    "ServiceRole": "DataCamp_EMR_Role",
    'Tags': [
        {
            'Key': 'for-use-with-amazon-emr-managed-policies',
            'Value': 'true'
        },
    ],
}


SPARK_STEPS = [
    {
        "Name": "Copy raw data from S3 to HDFS EMR Cluster",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/data/raw",
                "--dest=/data/raw",
            ],
        },
    },
    {
        "Name": "Run spark job - preprocess dataset",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master", 
                "yarn",
                "--deploy-mode",
                "cluster",
                "s3://{{ params.BUCKET_NAME }}/spark_jobs/process_data.py",
                "--input_pedestrian_counts",
                "/data/raw/pedestrian_counts.csv",
                "--input_sensor_info",
                "/data/raw/sensor_info.csv",
                "--output_top_10_by_day",
                "/data/cleaned/top_10_by_day/",
                "--output_top_10_by_month",
                "/data/cleaned/top_10_by_month/",
                "--output_sensor_by_year",
                "/data/cleaned/sensor_by_year/",
                "--output_dim_sensor_info",
                "/data/cleaned/dim_sensor_info/"
            ],
        },
    },
    {
        "Name": "Move preprocessed data from HDFS to S3",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/data/cleaned",
                "--dest=s3://{{ params.BUCKET_NAME }}/data/cleaned",
            ],
        },
    },
]

# SPARK_STEPS = [
#     {
#         "Name": "Move raw data from S3 to HDFS Cluster",
#         "ActionOnFailure": "TERMINATE_JOB_FLOW",
#         "HadoopJarStep": {
#             "Jar": "command-runner.jar",
#             "Args": [
#                 "s3-dist-cp",
#                 "--src=s3://{{ params.BUCKET_NAME }}/data/raw",
#                 "--dest=/data/raw",
#             ],
#         },
#     },
#     {
#         "Name": "Run spark job: preprocess dataset",
#         "ActionOnFailure": "TERMINATE_JOB_FLOW",
#         "HadoopJarStep": {
#             "Jar": "command-runner.jar",
#             "Args": [
#                 "spark-submit",
#                 "--master", 
#                 "yarn",
#                 "--deploy-mode",
#                 "cluster",
#                 "s3://{{ params.BUCKET_NAME }}/spark_jobs/test.py",
#                 "--input_covid_data",
#                 "/data/raw/covid_cases.csv",
#                 "--input_vaccination_data",
#                 "/data/raw/vaccination.csv",
#                 "--output_covid_data",
#                 "/data/cleaned/covid_cases",
#                 "--output_vaccination_data",
#                 "/data/cleaned/vaccination"
#             ],
#         },
#     },
#     {
#         "Name": "Move preprocessed data from HDFS to S3",
#         "ActionOnFailure": "TERMINATE_JOB_FLOW",
#         "HadoopJarStep": {
#             "Jar": "command-runner.jar",
#             "Args": [
#                 "s3-dist-cp",
#                 "--src=/data/cleaned",
#                 "--dest=s3://{{ params.BUCKET_NAME }}/data/cleaned",
#             ],
#         },
#     },
# ]

def upload_to_s3(bucket, target_name, local_file) -> None:
    s3_hook = S3Hook('cuonghtv_aws_conn')
    s3_hook.load_file(filename=local_file, key=target_name, bucket_name=bucket, replace=True)



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


    finish_dp_pedestrian_sensor_daily = DummyOperator(task_id="finish_dp_pedestrian_sensor_daily")

start_dp_pedestrian_sensor_daily >> ingest_data_and_create_cluster >> pyspark_in_emr >> finish_dp_pedestrian_sensor_daily
    # with TaskGroup(group_id='s3_to_redshift') as s3_to_redshift:

    #     covid_to_redshift = S3ToRedshiftOperator(
    #         schema="PUBLIC",
    #         s3_bucket=BUCKET_NAME,
    #         s3_key="/data/cleaned}/covid_cases/*.parquet",
    #         table="covid",
    #         copy_options=['parquet'],        
    #         aws_conn_id="cuonghtv_aws_conn",
    #         redshift_conn_id="cuonghtv_redshift_conn219",
    #         method="REPLACE",
    #         task_id="covid_to_redshift",
    #     )

    #     vaccination_to_redshift = S3ToRedshiftOperator(
    #         schema="PUBLIC",
    #         s3_bucket=BUCKET_NAME,
    #         s3_key="/data/cleaned}/vaccination/",
    #         table="vaccination",
    #         copy_options=['parquet'],        
    #         aws_conn_id="cuonghtv_aws_conn",
    #         redshift_conn_id="cuonghtv_redshift_conn",
    #         method="REPLACE",
    #         task_id="vaccination_to_redshift",
    #     )


