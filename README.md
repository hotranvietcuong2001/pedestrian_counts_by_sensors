# pedestrian_sensor

## Data Sources
Data is collected from [City of Melbourne](https://data.melbourne.vic.gov.au/Transport)

We have two datasets. `Pedestrian Counting System - Monthly (counts per hour)` dataset contains hourly pedestrian counts since 2009 from pedestrian sensor devices located across the city. The data is updated on a monthly basis and can be used to determine variations in pedestrian activity throughout the day.
`Pedestrian Counting System - Sensor Locations` contains status, location and directional information for each pedestrian sensor device installed throughout the city. The sensor_id column can be used to merge the data with related datasets.

Data is saved in two folders `pedestrian_counts`, `sensor_info`

## Questions

We have to answer some questions:
- Top 10 (most pedestrians) locations by day
- Top 10 (most pedestrians) locations by month
- Which location has shown most decline due to lockdowns in last 2 years
- Which location has most growth in last year

## Tech Stack Used
- Infrastructure as code: Terraform
- Cloud: AWS
- Data Lake: AWS S3
- Data Warehouse: AWS Redshift
- Data Transformation: Spark (PySpark) run on AWS EMR
- Workflow Orchestration: Airflow
- Containerization: Docker
- Streaming Data: Kinesis

## Architecture
![pedestrian_counts drawio](https://user-images.githubusercontent.com/56772542/197745638-017cc916-a6c3-492f-b9b2-988eabfd9c84.png)


## Results
The result will be divided into 5 tables for easy querying later. 

- Fact Table: `fact_top_10_by_day`, `fact_top_10_by_month`, `fact_sensor_by_year`
- Dim Table: `dim_sensor_info`, `dim_datetime`

The detailed description of the tables is given below:
### fact_top_10_by_day
![image](https://user-images.githubusercontent.com/56772542/198072885-97d78608-843a-459f-9ab3-407da9b8f7cd.png)


### fact_top_10_by_month
![image](https://user-images.githubusercontent.com/56772542/198072970-34a9b02a-dd74-4ca1-aa66-0eb3f34aaf96.png)


### fact_sensor_by_year
![image](https://user-images.githubusercontent.com/56772542/198073117-196ac05d-0f82-4e0b-8922-3633c461ac67.png)


### dim_sensor_info
![image](https://user-images.githubusercontent.com/56772542/198072671-ce08c2dd-8d5c-4f57-b1db-8159417a9630.png)


### dim_datetime
![image](https://user-images.githubusercontent.com/56772542/198072177-e070672f-73a4-4598-9b85-59029fe75a12.png)


