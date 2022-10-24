
BUCKET_NAME = 'vc-s3bucket-pedestrian-sensor'


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
        "ActionOnFailure": "CANCEL_AND_WAIT",
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
                "/data/raw/pedestrian_counts/",
                "--input_sensor_info",
                "/data/raw/sensor_info/",
                "--output_fact_top_10_by_day",
                "/data/cleaned/fact_top_10_by_day",
                "--output_fact_top_10_by_month",
                "/data/cleaned/fact_top_10_by_month",
                "--output_fact_sensor_by_year",
                "/data/cleaned/fact_sensor_by_year",
                "--output_dim_sensor_info",
                "/data/cleaned/dim_sensor_info",
                "--output_dim_datetime",
                "/data/cleaned/dim_datetime"
            ],
        },
    },
    {
        "Name": "Move preprocessed data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
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






# Date cua installation_date dang ra phai la date time type
# latitude, .. dang la INT
CREATE_ALL_TABLES = """
    
    DROP TABLE IF EXISTS fact_top_10_by_day, fact_top_10_by_month, fact_sensor_by_year,
                            dim_sensor_info, dim_datetime;
    
    CREATE TABLE dim_datetime (
        date_time               DATE        PRIMARY KEY,
        year                    INTEGER     NOT NULL,
        month                   INTEGER     NOT NULL,
        date                    INTEGER     NOT NULL
    );

    CREATE TABLE dim_sensor_info (
        sensor_id               TEXT PRIMARY KEY,
        sensor_description      TEXT,
        sensor_name             TEXT,
        installation_date       TEXT,
        status                  TEXT,
        note                    TEXT,
        direction_1             TEXT,
        direction_2             TEXT,
        latitude                TEXT,
        longitude               TEXT            
    );

    CREATE TABLE fact_top_10_by_day (
        top_10_by_day_id        BIGINT GENERATED ALWAYS AS IDENTITY,
        sensor_id               TEXT,
        date_time               DATE,
        daily_counts            BIGINT,
        
        CONSTRAINT fk_top_10_by_day_sensor_id
            FOREIGN KEY(sensor_id)
                REFERENCES dim_sensor_info(sensor_id),
        CONSTRAINT fk_top_10_by_day_date_time
            FOREIGN KEY(date_time)
                REFERENCES dim_datetime(date_time)
    );


    CREATE TABLE fact_top_10_by_month (
        top_10_by_month_id      BIGINT GENERATED ALWAYS AS IDENTITY,
        sensor_id               TEXT,
        date_time               DATE,
        monthly_counts          BIGINT,

        CONSTRAINT fk_top_10_by_month_sensor_id
            FOREIGN KEY(sensor_id)
                REFERENCES dim_sensor_info(sensor_id),
        
        CONSTRAINT fk_top_10_by_month_date_time
            FOREIGN KEY(date_time)
                REFERENCES dim_datetime(date_time)
    );

    CREATE TABLE fact_sensor_by_year (
        sensor_id               TEXT,
        counts_2020             BIGINT,
        counts_2021             BIGINT,
        counts_2022             BIGINT,

        CONSTRAINT fk_sensor_by_year_sensor_id
            FOREIGN KEY(sensor_id)
                REFERENCES dim_sensor_info(sensor_id)
    );
"""