"""
Transform the extracted data 
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType,FloatType
import csv
from my_lib.util import log_tests

spark_dataframes = {}


def get_col_type(col_type:str):
    if col_type.lower() == "int":
        return IntegerType
    elif col_type.lower() == "string":
        return StringType
    elif col_type.lower() == "date":
        return DateType
    elif col_type.lower() == "float":
        return FloatType


def create_dataframe_with_schema (spark, table, columns):
    # create schema
    schema = StructType([ ])
    for column in columns:
        schema.fields.append(StructField(column.Keys[0], get_col_type(column.value[0])))
    
    # create dataframe
    spark_dataframes[table] = spark.createDataFrame([], schema)


# load the csv file and insert into a new sqlite3 database
def transform_n_load(
    local_dataset: str,
    new_data_tables: dict,
    new_lookup_tables: dict, #dict of dict - {name :{column:type},name :{column:type}}
    column_map: dict,
):
    """ "Transforms and Loads data into the local SQLite3 database"""

    # load the data from the csv
    reader = csv.reader(open("data/" + local_dataset, newline=""), delimiter=",")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("CreateDataFrameWithSchema").getOrCreate()
 
    # Create dataframes with schema
    for table, columns in new_data_tables.items():
        log_tests(f"Creating non-lookup table: {table}")
        create_dataframe_with_schema(spark, table, columns)

    for table, columns in new_lookup_tables.items():
        log_tests(f"Creating lookup table: {table}")
        create_dataframe_with_schema(spark, table, columns)

    log_tests("Tables created.")

    temp_list_of_tuples = {}

    # skip the first row
    log_tests("Skipping the first row...")
    next(reader)
    log_tests("Inserting table data...")
    for row in reader:
        first_for_loop_broken = False
        for table, columns in new_lookup_tables.items():
            # If the ID is not a number don't import it
            if not row[column_map[columns[0].key()]].isnumeric():
                first_for_loop_broken = True
                break  # Go to outer loop

            # exec_str = f"select count({columns[0]}) from {table} where {columns[0]} = {int(row[column_map[columns[0]]])}"
            # result = c.execute(exec_str).fetchone()[0]
            result = spark_dataframes[table].count(columns[0].key() == row[column_map[columns[0].key()]])
            if result == 0:
                data_values = [(row[column_map[col]]) for col in columns]
                temp_list_of_tuples[table].append(tuple(data_values))

        # Only load the data if all lookup information are there
        if not first_for_loop_broken:
            for table, columns in new_data_tables.items():
                data_values = [(row[column_map[col]]) for col in columns]
                temp_list_of_tuples[table].append(tuple(data_values))

    log_tests("Inserting table data completed")

    print(temp_list_of_tuples)

    return "Transform  and load Successful"
