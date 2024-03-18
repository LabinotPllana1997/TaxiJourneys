# 3. Validate the data and save the valid data in Parquet format and invalid data in csv format in the PipelineData/Silver directory.
# The validation rules mandate that all journeys must have at least 1 passenger. Journies with no passengers are not valid. All records should have a vendor id. 
# Journies with no Vendor Id should have their ID set to 999 before saving to the valid data set.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def validate_and_save_to_silver(spark):
    consistent_df = spark.read.parquet("PipelineData/Silver/consistent_schema.parquet")

    # Validation rules
    validated_df = consistent_df.withColumn(
        "VendorId",
        when((col("VendorId").isNull()) | (col("VendorId") == ""), 999).otherwise(col("VendorId"))
    )

    valid_df = validated_df.filter("PassengerCount >= 1")

    invalid_df = validated_df.filter("PassengerCount < 1")

    # Save valid and invalid data
    valid_df.write.parquet("PipelineData/Silver/valid_data.parquet", mode="overwrite")
    invalid_df.write.csv("PipelineData/Silver/invalid_data.csv", header=True, mode="overwrite")

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("TaxiJourneys").getOrCreate()
#     validate_and_save_to_silver(spark)
#     spark.stop()
