# 5. Shape the data to output csv files to the Gold folder containing the following:-
#     File 1  - Locations
#     The total fares by pickup location
#     The total tips by pickup location
#     The average distance by pickup location
#     The average distance by dropoff location   ?????    ->   This is a different groupBy Will not be able to output in the same file
#     File 2 - Vendors
#     The total fare by vendor
#     The total tips by vendor
#     The average fare by vendor
#     The average tips by vendor

# NB: The total_amount has been explicitly mentioned to be extracted to Silver and is subsequently used to represent the total fares (the fare_amount has not been used).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum

def shape_and_save_to_gold(spark):
    deduped_df = spark.read.parquet("PipelineData/Silver/deduped_data.parquet")

    # File 1 - Locations
    locations_df = deduped_df.groupBy("PickUpLocationId").agg(
        sum("TotalAmount").alias("TotalFares"),
        sum("TipAmount").alias("TotalTips"),
        avg("TripDistance").alias("AverageDistanceByPickup"),
        avg("TripDistance").alias("AverageDistanceByDropoff")
    )

    locations_df.write.csv("PipelineData/Gold/Locations.csv", header=True, mode="overwrite")

    # File 2 - Vendors
    vendors_df = deduped_df.groupBy("VendorId").agg(
        sum("TotalAmount").alias("TotalFareByVendor"),
        sum("TipAmount").alias("TotalTipsByVendor"),
        avg("TotalAmount").alias("AverageFareByVendor"),
        avg("TipAmount").alias("AverageTipsByVendor")
    )

    vendors_df.write.csv("PipelineData/Gold/Vendors.csv", header=True, mode="overwrite")

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("TaxiJourneys").getOrCreate()
#     shape_and_save_to_gold(spark)
#     spark.stop()

    