# pedestrian_sensor

## Data Sources
Data is collected from [City of Melbourne](https://data.melbourne.vic.gov.au/Transport)

We have two datasets. `Pedestrian Counting System - Monthly (counts per hour)` dataset contains hourly pedestrian counts since 2009 from pedestrian sensor devices located across the city. The data is updated on a monthly basis and can be used to determine variations in pedestrian activity throughout the day.
`Pedestrian Counting System - Sensor Locations` contains status, location and directional information for each pedestrian sensor device installed throughout the city. The sensor_id column can be used to merge the data with related datasets.

Data is saved in two file `pedestrian_counts.csv`, `sensor_info.csv`

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

## Architecture
![pedestrian_counts drawio](https://user-images.githubusercontent.com/56772542/197561872-31d3fd1c-f7cd-4341-bf1a-de95174be3ce.png)



## Results
The result will be divided into 4 csv files for easy querying later. Those 4 files include `top_10_by_day.csv`, `top_10_by_month.csv`, `sensor_by_year.csv`, `dim_sensor_info.csv`

The detailed description of the tables is given below:
### top_10_by_day
![image](https://user-images.githubusercontent.com/56772542/193217025-8c1f24c0-8315-4c5e-a68f-6ec74a58f310.png)

### top_10_by_month
![image](https://user-images.githubusercontent.com/56772542/193217391-9deb2d37-3d2f-4b98-b9a0-f79fb528eacb.png)

### sensor_by_year
![image](https://user-images.githubusercontent.com/56772542/193218372-8ccd3958-3065-4f39-b641-8a03a7c1485e.png)

### dim_sensor_info
![image](https://user-images.githubusercontent.com/56772542/193218470-32723cac-8cfd-4b0f-ab79-a7185bda6a94.png)




## Setup Guides
