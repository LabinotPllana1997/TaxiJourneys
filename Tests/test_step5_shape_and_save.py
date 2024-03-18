from pyspark.sql import SparkSession
from PythonCode.step5_shape_and_save import shape_and_save_to_gold
import pytest
import os
import sys
import ipytest

# Define the spark_session fixture
@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .appName("TestShapeAndSave") \
        .getOrCreate()
    yield spark
    spark.stop()

# Write a test for shape_and_save_to_gold function
def test_shape_and_save_to_gold(spark_session):
    # Mock data for deduped dataframe
    deduped_data = [
        (1, "2021-01-01 00:00:00", "2021-01-01 00:10:00", 1, 2, 1, 10.0, 2.0, 12.0),
        (2, "2021-01-01 00:15:00", "2021-01-01 00:25:00", 2, 3, 2, 12.0, 3.0, 15.0),
        (3, "2021-01-01 00:30:00", "2021-01-01 00:40:00", 3, 4, 3, 15.0, 4.0, 19.0)
    ]
    deduped_schema = ["VendorId", "PickUpDateTime", "DropOffDateTime", "PickUpLocationId", "DropOffLocationId", "PassengerCount", "TripDistance", "TipAmount", "TotalAmount"]
    deduped_df = spark_session.createDataFrame(deduped_data, deduped_schema)

    # Execute the function
    shape_and_save_to_gold(spark_session)

    # Check if CSV files are created
    assert "Locations.csv" in os.listdir("PipelineData/Gold/")
    assert "Vendors.csv" in os.listdir("PipelineData/Gold/")