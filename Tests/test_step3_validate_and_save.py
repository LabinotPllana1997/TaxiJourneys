from pyspark.sql import SparkSession
from PythonCode.step3_validate_and_save import validate_and_save_to_silver
import pytest
import os
import sys
import ipytest


# Define the spark_session fixture
@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .appName("TestImportAndConvert") \
        .getOrCreate()
    yield spark
    spark.stop()

# Write a test for validate_and_save_to_silver function
def test_validate_and_save_to_silver(spark_session):
    # Mock data for consistent dataframe
    consistent_data = [
        (1, "2021-01-01 00:00:00", "2021-01-01 00:10:00", 1, 2, None, 10.0, 2.0, 12.0),
        (2, "2021-01-01 00:15:00", "2021-01-01 00:25:00", 2, 3, 2, 12.0, 3.0, 15.0),
        (3, "2021-01-01 00:30:00", "2021-01-01 00:40:00", 3, 4, 0, 15.0, 4.0, 19.0)
    ]
    consistent_schema = ["VendorId", "PickUpDateTime", "DropOffDateTime", "PickUpLocationId", "DropOffLocationId", "PassengerCount", "TripDistance", "TipAmount", "TotalAmount"]
    consistent_df = spark_session.createDataFrame(consistent_data, consistent_schema)

    # Execute the function
    validate_and_save_to_silver(spark_session)

    # Check if parquet and csv files are created
    assert "valid_data.parquet" in os.listdir("PipelineData/Silver/")
    assert "invalid_data.csv" in os.listdir("PipelineData/Silver/")


# ipytest.run('-vv')