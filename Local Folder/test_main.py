from my_lib.extract import extract
from my_lib.load import transform_n_load
from my_lib.util import log_tests
import os
import pyspark



from my_lib.crud import (
    read_data,
    read_all_data,
    save_data,
    delete_data,
    update_data,
    get_table_columns,
)

column_map = {
    "air_quality_id": 0,
    "indicator_id": 1,
    "indicator_name": 2,
    "measure": 3,
    "measure_info": 4,
    "geo_type_name": 5,
    "geo_id": 6,
    "geo_place_name": 7,
    "time_period": 8,
    "start_date": 9,
    "data_value": 10,
    "fn_geo_id": 6,
    "fn_indicator_id": 1,
}


# Test extract
def test_extract():
    log_tests("Extraction Test", header=True, new_log_file=True)
    log_tests("Removing existing CSV file exists")
    if os.path.exists("data/air_quality.csv"):
        os.remove("data/air_quality.csv")

    log_tests("Confirming that CSV file doesn't exists...")
    assert not os.path.exists("population_bar.png")
    log_tests("Test Successful")

    log_tests("Extracting data and saving...")
    extract(
        "https://data.cityofnewyork.us/resource/c3uy-2p5r.csv?$limit=200000",
        "air_quality.csv",
    )

    log_tests("Testing if CSV file exists...")
    assert os.path.exists("data/air_quality.csv")
    log_tests("Extraction Test Successful", last_in_group=True)
    print("Extraction Test Successful")


# Test transform and load
def test_transform_and_load():
    log_tests("Transform and Load Test", header=True)
    transform_n_load(
        local_dataset="air_quality.csv",
        database_name="air_quality.db",
        new_data_tables={
            "air_quality": [
                "air_quality_id",
                "fn_indicator_id",
                "fn_geo_id",
                "time_period",
                "start_date",
                "data_value",
            ],
        },
        new_lookup_tables={
            "indicator": ["indicator_id", "indicator_name", "measure", "measure_info"],
            "geo_data": ["geo_id", "geo_place_name", "geo_type_name"],
        },
        column_attributes={
            "air_quality_id": "INTEGER PRIMARY KEY",
            "indicator_id": "INTEGER PRIMARY KEY",
            "indicator_name": "TEXT",
            "measure": "TEXT",
            "measure_info": "TEXT",
            "geo_type_name": "TEXT",
            "geo_id": "INTEGER PRIMARY KEY",
            "geo_place_name": "TEXT",
            "time_period": "TEXT",
            "start_date": "TEXT",
            "data_value": "REAL",
            "fn_indicator_id": "INTEGER",
            "fn_geo_id": "INTEGER",
        },
        column_map={
            "air_quality_id": 0,
            "indicator_id": 1,
            "indicator_name": 2,
            "measure": 3,
            "measure_info": 4,
            "geo_type_name": 5,
            "geo_id": 6,
            "geo_place_name": 7,
            "time_period": 8,
            "start_date": 9,
            "data_value": 10,
            "fn_geo_id": 6,
            "fn_indicator_id": 1,
        },
    )
    log_tests("Transform and Load Test Successful", last_in_group=True)
    print("Transform and Load Test Successful")


# Test read data
def test_read_data():
    log_tests("One Record Reading Test", header=True)
    row = read_data("air_quality.db", "air_quality", 740885)
    data_value = 5
    log_tests("Asserting that row[0][data_value] == 16.4")
    assert row[0][data_value] == 16.4
    log_tests("Assert Successful")
    log_tests("One Record Reading Test Successful", last_in_group=True)
    print("One Record Reading Test Successful")


# Test read all data
def test_read_all_data():
    log_tests("All Records Reading Test", header=True)
    rows = read_all_data("air_quality.db", "air_quality")
    log_tests("Asserting that len(rows) == 18016")
    assert len(rows) == 18016
    log_tests("All Records Reading Test Successful", last_in_group=True)
    print("All Records Reading Test Successful")


# Test save data
def test_save_data():
    log_tests("Record Saving Test", header=True)

    log_tests("Asserting there's no record in geo_data with ID 100000")
    result = read_data("air_quality.db", "geo_data", 100000)
    assert result is None
    log_tests("Assert Successful")

    log_tests("Saving new record with ID 100000")
    save_data("air_quality.db", "geo_data", ["100000", "Lancaster", "UFO"])

    log_tests("Asserting there's now a record in geo_data with ID 100000")
    result = read_data("air_quality.db", "geo_data", 100000)
    assert len(result) == 1
    log_tests("Assert Successful")

    log_tests("Record Saving Test Successful", last_in_group=True)
    print("Record Saving Test Successful")


# Test update data
def test_update_data():
    log_tests("Record Update Test", header=True)

    log_tests("Asserting 'geo_place_name' in geo_data for row ID 100000 is 'Lancaster'")
    result = read_data("air_quality.db", "geo_data", 100000)
    assert result[0][1] == "Lancaster"
    log_tests("Assert Successful")

    log_tests("Updating 'geo_place_name' in geo_data for row ID 100000 is 'Duke'")
    update_data("air_quality.db", "geo_data", {"geo_place_name": "Duke"}, 100000)

    log_tests("Asserting 'geo_place_name' in geo_data for row ID 100000 is now 'Duke'")
    result = read_data("air_quality.db", "geo_data", 100000)
    assert result[0][1] == "Duke"
    log_tests("Assert Successful")

    log_tests("Record Update Test Successful", last_in_group=True)
    print("Record Update Test Successful")


# Test delete data
def test_delete_data():
    log_tests("Record Deletion Test", header=True)

    log_tests("Asserting there's a record in geo_data for row ID 100000")
    result = read_data("air_quality.db", "geo_data", 100000)
    assert len(result) == 1
    log_tests("Assert Successful")

    log_tests("Deleting record with ID 100000")
    print(delete_data("air_quality.db", "geo_data", 100000))

    log_tests("Asserting there's no record in geo_data with ID 100000")
    result = read_data("air_quality.db", "geo_data", 100000)
    assert result is None
    log_tests("Assert Successful")

    log_tests("Record Deletion Test Successful", last_in_group=True)
    print("Record Deletion Test Successful")


# Test read all column names
def test_get_table_columns():
    log_tests("Reading All Column Test", header=True)

    columns = get_table_columns("air_quality.db", "air_quality")

    log_tests("Asserting the air_quality table has six (6) columns")
    assert len(columns) == 6
    log_tests("Assert Successful")

    log_tests("Reading All Column Test Successful", last_in_group=True)
    print("Reading All Column Test Successful")


# Two addtional queries to meet requirements
def execute_two_addtional_queries():
    print("****************Data in Geo_Data*************************")
    rows = read_all_data("air_quality.db", "geo_data")
    print("The number of rows retrieved is: ", len(rows))
    print("****************Data in Indicator*************************")
    print(read_all_data("air_quality.db", "indicator"))


if __name__ == "__main__":
    # test_extract()
    # test_transform_and_load()
    # test_read_data()
    # test_read_all_data()
    # test_save_data()
    # test_update_data()
    # test_delete_data()
    # test_get_table_columns()
    # execute_two_addtional_queries()
    print(pyspark.__version__)