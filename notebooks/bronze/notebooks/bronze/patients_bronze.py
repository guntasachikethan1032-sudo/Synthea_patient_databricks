from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2

SOURCE_PATH = "s3://sachi-healthcare-bucket03/RAW/patients.csv"

@dp.materialized_view(
    name="synthea_patient_data.bronze.patient",
    comment="Patient data raw processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def patient_bronze():
    df = spark.read.format("csv").option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE")  \
        .option("mergeSchema", "true") \
        .option("columnNameOfCorruptRecord","_corrupt_record").load(SOURCE_PATH)

    df = df.withColumn("ingest_datetime", current_timestamp())
    
    return df
