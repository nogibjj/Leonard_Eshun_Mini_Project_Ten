from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("SamplePySparkProject").getOrCreate()

    # Load data from a CSV file
    data = spark.read.csv("data.csv", header=True, inferSchema=True)

    # Perform some basic data analysis
    average_age = data.agg(avg("data_value")).collect()[0][0]
    total_users = data.count()

    # # Filter the data
    # filtered_data = data.filter(col("country") == "USA")

    # # Group the data
    # grouped_data = data.groupBy("gender").agg(count("*").alias("count"))

    # Print results
    print("Average age:", average_age)
    print("Total users:", total_users)
    # print("Filtered data:")
    # filtered_data.show()
    # print("Grouped data:")
    # grouped_data.show()

    # Stop the SparkSession
    spark.stop()

