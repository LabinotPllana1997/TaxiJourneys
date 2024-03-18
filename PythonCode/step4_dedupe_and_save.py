# 4. Dedupe the data and save in Parquet format in the PipelineData/silver folder (Dedupe based on Pickup location, Pick up Time, Drop off time, drop off location and vendor)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def dedupe_and_save_to_silver(spark):
    valid_df = spark.read.parquet("PipelineData/Silver/valid_data.parquet")

    # Dedupe logic based on specified columns
    deduped_df = valid_df.dropDuplicates(subset=["PickUpLocationId", "PickUpDateTime", "DropOffDateTime", "DropOffLocationId", "VendorId"])

    # Save deduped data
    deduped_df.write.parquet("PipelineData/Silver/deduped_data.parquet", mode="overwrite")

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("TaxiJourneys").getOrCreate()
#     dedupe_and_save_to_silver(spark)
#     spark.stop()