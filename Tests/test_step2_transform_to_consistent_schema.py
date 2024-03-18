from pyspark.sql import SparkSession
from PythonCode.step2_transform_to_consistent_schema import transform_to_consistent_schema, save_to_silver_parquet
import pytest
import os
import sys

# Define the spark_session fixture
@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .appName("TestImportAndConvert") \
        .getOrCreate()
    yield spark
    spark.stop()

# Write tests for transform_to_consistent_schema function
def test_transform_to_consistent_schema(spark_session):
    # Mock data for yellow and green dataframes
    yellow_data = [(1, "2021-01-01 00:00:00", "2021-01-01 00:10:00", 1, 2, 1, 10.0, 2.0, 12.0)]
    green_data = [(2, "2021-01-01 00:15:00", "2021-01-01 00:25:00", 2, 3, 2, 12.0, 3.0, 15.0)]
    yellow_schema = ["VendorId", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "passenger_count", "trip_distance", "tip_amount", "total_amount"]
    green_schema = ["VendorId", "lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID", "DOLocationID", "passenger_count", "trip_distance", "tip_amount", "total_amount"]

    # Create test dataframes
    yellow_df = spark_session.createDataFrame(yellow_data, yellow_schema)
    green_df = spark_session.createDataFrame(green_data, green_schema)

    # Execute the function
    consistent_df = transform_to_consistent_schema(yellow_df, green_df)

    # Check if columns are transformed correctly
    expected_columns = ["VendorId", "PickUpDateTime", "DropOffDateTime", "PickUpLocationId", "DropOffLocationId", "PassengerCount", "TripDistance", "TipAmount", "TotalAmount"]
    assert consistent_df.columns == expected_columns

def test_save_to_silver_parquet(spark_session):
    # Mock data for consistent dataframe
    consistent_data = [(1, "2021-01-01 00:00:00", "2021-01-01 00:10:00", 1, 2, 1, 10.0, 2.0, 12.0)]
    consistent_schema = ["VendorId", "PickUpDateTime", "DropOffDateTime", "PickUpLocationId", "DropOffLocationId", "PassengerCount", "TripDistance", "TipAmount", "TotalAmount"]
    consistent_df = spark_session.createDataFrame(consistent_data, consistent_schema)

    # Execute the function
    save_to_silver_parquet(consistent_df)

    # Check if parquet file is created
    assert "consistent_schema.parquet" in os.listdir("PipelineData/Silver/")

