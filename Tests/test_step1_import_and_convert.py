from pyspark.sql import SparkSession
from PythonCode.step1_import_and_convert import import_and_convert_to_parquet
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

# Cell 3: Write a test for import_and_convert_to_parquet function
def test_import_and_convert_to_parquet(spark_session):
    # Test data paths
    parquet_output_path = "PipelineData/Bronze"

    # Execute the function
    import_and_convert_to_parquet(spark_session)

    # Check if parquet files are created
    assert "yellow_tripdata_2021-01.parquet" in os.listdir(parquet_output_path)
    assert "green_tripdata_2021-01.parquet" in os.listdir(parquet_output_path)