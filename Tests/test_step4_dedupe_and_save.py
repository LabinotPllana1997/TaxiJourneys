from pyspark.sql import SparkSession
from PythonCode.step4_dedupe_and_save import dedupe_and_save_to_silver
import pytest
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


# Define the spark_session fixture
@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .appName("TestDedupeAndSave") \
        .getOrCreate()
    yield spark
    spark.stop()

# Write a test for dedupe_and_save_to_silver function
def test_dedupe_and_save_to_silver(spark_session):
    # Mock data for valid dataframe
    valid_data = [
        (1, "2021-01-01 00:00:00", "2021-01-01 00:10:00", 1, 2, 1, 10.0, 2.0, 12.0),
        (1, "2021-01-01 00:00:00", "2021-01-01 00:10:00", 1, 2, 1, 10.0, 2.0, 12.0),
        (2, "2021-01-01 00:15:00", "2021-01-01 00:25:00", 2, 3, 2, 12.0, 3.0, 15.0),
        (3, "2021-01-01 00:30:00", "2021-01-01 00:40:00", 3, 4, 3, 15.0, 4.0, 19.0)
    ]
    valid_schema = ["VendorId", "PickUpDateTime", "DropOffDateTime", "PickUpLocationId", "DropOffLocationId", "PassengerCount", "TripDistance", "TipAmount", "TotalAmount"]
    valid_df = spark_session.createDataFrame(valid_data, valid_schema)

    # Execute the function
    dedupe_and_save_to_silver(spark_session)

    # Check if parquet file is created
    assert "deduped_data.parquet" in os.listdir("PipelineData/Silver/")