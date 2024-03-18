from PythonCode.step1_import_and_convert import import_and_convert_to_parquet
from PythonCode.step2_transform_to_consistent_schema import transform_to_consistent_schema, save_to_silver_parquet
from PythonCode.step3_validate_and_save import validate_and_save_to_silver
from PythonCode.step4_dedupe_and_save import dedupe_and_save_to_silver
from PythonCode.step5_shape_and_save import shape_and_save_to_gold
from pyspark.sql import SparkSession

if __name__ =='__main__':

    spark = SparkSession.builder.appName("TaxiJourneys").getOrCreate()

    # Step 1
    import_and_convert_to_parquet(spark)

    # Step 2
    yellow_df = spark.read.parquet("PipelineData/Bronze/yellow_tripdata_2021-01.parquet")
    green_df = spark.read.parquet("PipelineData/Bronze/green_tripdata_2021-01.parquet")

    consistent_df = transform_to_consistent_schema(yellow_df, green_df)
    save_to_silver_parquet(consistent_df)

    # Step 3
    validate_and_save_to_silver(spark)

    # Step 4
    dedupe_and_save_to_silver(spark)

    # Step 5
    shape_and_save_to_gold(spark)