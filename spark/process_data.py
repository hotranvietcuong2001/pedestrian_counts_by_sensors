# Importing the libraries that we need to use in our code.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse



def preprocess_data(pedestrian_counts, sensor_df):
  """
  The function takes in the pedestrian counts dataframe and the sensor dataframe, and returns the
  preprocessed dataframes
  
  :param pedestrian_counts: The dataframe containing the pedestrian counts
  :param sensor_df: The dataframe containing the sensor data
  :return: two preprocessed dataframes
  """

  pedestrian_counts = pedestrian_counts \
                        .withColumn("Date_Time", F.to_date(F.col("Date_Time"), 'MMMM dd, yyyy hh:mm:ss a')) \
                        .withColumn('Month', F.date_format(F.col("Date_Time"), "M")) \
                        .withColumn('Hourly_Counts', F.col("Hourly_Counts").cast("Int")) \
                        .drop("Date_Time", "Day", "Time")

  return pedestrian_counts, sensor_df
        




def get_top_10_location_by_day(df):
  """
  It groups the dataframe by year, month, date, and sensor_id, sums the hourly counts, and then ranks
  the results by the sum of hourly counts. It then filters the results to only include the top 10
  results and renames the column
  
  :param df: the dataframe that we want to get the top 10 locations by day
  :return: A dataframe with the top 10 locations by day.
  """
  
  window = Window.partitionBy(["Year", "Month", "Mdate"]) \
                  .orderBy(F.col('sum(Hourly_Counts)').desc())

  return df.groupBy(["Year", "Month", "Mdate", "Sensor_ID"]).sum("Hourly_Counts")\
            .select('*', F.rank().over(window).alias('rank')) \
            .filter(F.col('rank') <= 10) \
            .withColumRenamed('sum(Hourly_Counts)', 'Hourly_Counts') \
            .drop('rank')



def get_top_10_location_by_month(df):
  """
  > It groups the data by year, month, and sensor ID, sums the hourly counts, and then selects the top
  10 sensors for each month
  
  :param df: the dataframe to be processed
  :return: A dataframe with the top 10 locations by month
  """
  n = 10

  window = Window.partitionBy(["Year", "Month"]) \
                  .orderBy(F.col('sum(Hourly_Counts)').desc())

  return df.groupBy(["Year", "Month", "Sensor_ID"]).sum("Hourly_Counts")\
            .select('*', F.rank().over(window).alias('rank')) \
            .filter(F.col('rank') <= 10) \
            .withColumnRenamed('sum(Hourly_Counts)', 'Hourly_Counts') \
            .drop('rank')



def get_sensor_by_year(df):
  """
  > The function takes a dataframe as input and returns a dataframe with the sum of hourly counts for
  each sensor by year
  
  :param df: the dataframe you want to pivot
  :return: A dataframe with the sum of hourly counts for each sensor for each year.
  """
  return df.filter(F.col("Year")>=2020) \
            .groupBy(["Sensor_ID"]).pivot("Year").sum("Hourly_Counts")



def get_amount_decline_last_2_years(df):
  """
  > We get the sensor data by year, then we create a new column called `amount_decline_last_2_years`
  which is the difference between the 2022 and 2020 columns
  
  :param df: the dataframe
  :return: A dataframe with the amount of decline in the last 2 years
  """
  return get_sensor_by_year(df) \
            .withColumn("amount_decline_last_2_years", F.col("2022")-F.col("2020")) \
            .orderBy(F.col("amount_decline_last_2_years").asc()) \
            .dropna(subset="amount_decline_last_2_years")
            



def get_amount_growth_last_years(df):
  """
  > It takes a dataframe, groups it by sensor, and then calculates the amount of growth in the last
  year
  
  :param df: the dataframe
  :return: A dataframe with the amount of sensors per year and the growth of the amount of sensors
  between 2021 and 2022.
  """
  return get_sensor_by_year(df) \
            .withColumn("amount_growth_last_year", F.col("2022")-F.col("2021")) \
            .orderBy(F.col("amount_growth_last_year").desc()) \
            .dropna(subset="amount_growth_last_year")





if __name__ == "__main__":
  # Parsing the arguments that are passed in the command line.
  parser = argparse.ArgumentParser()

  parser.add_argument('--input_pedestrian_counts', required=True)
  parser.add_argument('--input_sensor_info', required=True)
  parser.add_argument('--output_top_10_by_day', required=True)
  parser.add_argument('--output_top_10_by_month', required=True)
  parser.add_argument('--output_sensor_by_year', required=True)
  parser.add_argument('--output_dim_sensor_info', required=True)

  args = parser.parse_args()
  input_pedestrian_counts = args.input_pedestrian_counts
  input_sensor_info = args.input_sensor_info
  output_top_10_by_day = args.output_top_10_by_day
  output_top_10_by_month = args.output_top_10_by_month
  output_sensor_by_year = args.output_sensor_by_year
  output_dim_sensor_info = args.output_dim_sensor_info

  # Creating a SparkSession object that is used to create DataFrames and execute SQL queries.
  spark = SparkSession.builder \
                      .appName('Pedestrian Counts By Sensor') \
                      .getOrCreate()


  # Reading the csv files and creating dataframes.
  pedestrian_counts_df = spark.read.csv(input_pedestrian_counts, header=True, multiLine=True)
  input_sensor_info = spark.read.csv(input_sensor_info, header=True, multiLine=True)

  # preprocess dataframe
  pedestrian_counts_df, input_sensor_info = preprocess_data(pedestrian_counts_df, input_sensor_info)

  # main functions

  top_10_location_by_day = get_top_10_location_by_day(pedestrian_counts_df)
  top_10_location_by_day.write.parquet(output_top_10_by_day, mode='overwrite', header=True)

  
  top_10_location_by_month_2 = get_top_10_location_by_month(pedestrian_counts_df)
  top_10_location_by_month_2.write.parquet(output_top_10_by_month, mode='overwrite', header=True)
  
  
  sensor_by_year_df = get_sensor_by_year(pedestrian_counts_df)
  sensor_by_year_df.write.parquet(output_sensor_by_year, mode='overwrite', header=True)
  
  
  temp_df_1 = get_amount_decline_last_2_years(pedestrian_counts_df)
  print('Location has shown most decline due to lockdowns in last 2 years',temp_df_1.collect()[0][0])
  
  temp_df_2 = get_amount_growth_last_years(pedestrian_counts_df)
  print('Location has most def get_amount_growth_last_years(df):', temp_df_2.collect()[0][0])
  
  input_sensor_info.write.parquet(output_dim_sensor_info, mode='overwrite', header=True)


