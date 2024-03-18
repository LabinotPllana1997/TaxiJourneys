# 1. Import the Yellow and Green data files, convert them to Parquet format and save them to the PipelineData/Bronze directory

from pyspark.sql import SparkSession

# Step 1: Import Yellow and Green data files, convert to Parquet, and save to Bronze directory
def import_and_convert_to_parquet(spark):
    yellow_df = spark.read.csv("RawData/yellow_tripdata_2021-01.csv", header=True, inferSchema=True)
    green_df = spark.read.csv("RawData/green_tripdata_2021-01.csv", header=True, inferSchema=True)

    # Convert to Parquet and save to Bronze directory
    yellow_df.write.parquet("PipelineData/Bronze/yellow_tripdata_2021-01.parquet", mode="overwrite")
    green_df.write.parquet("PipelineData/Bronze/green_tripdata_2021-01.parquet", mode="overwrite")

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("TaxiJourneys").getOrCreate()
#     import_and_convert_to_parquet(spark)
#     spark.stop()
