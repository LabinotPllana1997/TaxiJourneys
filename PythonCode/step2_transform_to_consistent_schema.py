# 2. Transform the datafiles to a consistant schema and save them in Parquet format to the PipelineData/Silver directory

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def transform_to_consistent_schema(yellow_df, green_df):
    # Select and rename columns for consistent schema
    yellow_transformed = yellow_df.selectExpr(
        "VendorID as VendorId",
        "tpep_pickup_datetime as PickUpDateTime",
        "tpep_dropoff_datetime as DropOffDateTime",
        "PULocationID as PickUpLocationId",
        "DOLocationID as DropOffLocationId",
        "passenger_count as PassengerCount",
        "trip_distance as TripDistance",
        "tip_amount as TipAmount",
        "total_amount as TotalAmount"
    )

    green_transformed = green_df.selectExpr(
        "VendorID as VendorId",
        "lpep_pickup_datetime as PickUpDateTime",
        "lpep_dropoff_datetime as DropOffDateTime",
        "PULocationID as PickUpLocationId",
        "DOLocationID as DropOffLocationId",
        "passenger_count as PassengerCount",
        "trip_distance as TripDistance",
        "tip_amount as TipAmount",
        "total_amount as TotalAmount"
    )

    # Union the transformed DataFrames
    consistent_df = yellow_transformed.union(green_transformed)

    return consistent_df

def save_to_silver_parquet(consistent_df):
    consistent_df.write.parquet("PipelineData/Silver/consistent_schema.parquet", mode="overwrite")

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("TaxiJourneys").getOrCreate()

#     yellow_df = spark.read.parquet("PipelineData/Bronze/yellow_tripdata_2021-01.parquet")
#     green_df = spark.read.parquet("PipelineData/Bronze/green_tripdata_2021-01.parquet")

#     consistent_df = transform_to_consistent_schema(yellow_df, green_df)
#     save_to_silver_parquet(consistent_df)

#     spark.stop()