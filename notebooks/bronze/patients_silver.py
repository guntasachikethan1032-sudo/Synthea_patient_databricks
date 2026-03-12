from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="synthea_patient_data.silver.patients_data_from_bronze",
    comment="Cleaned and standarardized patient data",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)




def city_silver():

    df_bronze = spark.read.table("synthea_patient_data.bronze.patient")

    df_silver = df_bronze.select(
        F.col("BIRTHDATE").alias("birth_date"),
        F.col("PASSPORT").alias("passport"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )

    df_silver = df_silver.withColumn(
        "silver_processed_timestamp",F.current_timestamp()
    )
    return df_silver  
