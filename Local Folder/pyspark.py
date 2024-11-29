# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg

# # Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("AverageIndicators") \
#     .getOrCreate()

# # Sample data
# data = [
#     (101, 'Area1', '2021-Q1', 50.0),
#     (101, 'Area1', '2021-Q1', 70.0),
#     (101, 'Area2', '2021-Q1', 80.0),
#     (102, 'Area1', '2021-Q1', 60.0),
#     (102, 'Area2', '2021-Q2', 90.0),
#     (103, 'Area3', '2021-Q1', 40.0),
#     (103, 'Area3', '2021-Q2', 50.0),
#     (103, 'Area3', '2021-Q2', 70.0),
# ]

# columns = ["fn_indicator_id", "fn_geo_id", "time_period", "data_value"]

# # Create DataFrame
# df = spark.createDataFrame(data, columns)

# # Calculate the average
# avg_df = df.groupBy("fn_indicator_id", "fn_geo_id", "time_period") \
#            .agg(avg("data_value").alias("average_data_value"))

# # Order the results
# avg_df_ordered = avg_df.orderBy("fn_indicator_id", "fn_geo_id", "time_period")
# avg_df_ordered.show()

# # Stop SparkSession
# spark.stop()