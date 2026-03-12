from pyspark.sql.functions import current_timestamp, input_file_name


SOURCE_PATH = "s3://sachi-healthcare-bucket03/RAW/medications.csv"

# Bronze table name in Databricks
BRONZE_TABLE = "synthea_patient_data.bronze.medications"

# REad raw data from AWS , CSV
df = (
    spark.read
    .format("csv")
    .option("header", "true")                  # first row has column names
    .option("inferSchema", "true")             # auto-detect datatypes
    .option("mode", "PERMISSIVE")              # don't fail on malformed rows
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .load(SOURCE_PATH)
)

# metadata columns

df_bronze = (
    df.withColumn("ingest_datetime", current_timestamp())   # when record was ingested
)

# we are adding the data to the existing data. ( Append , option in the mode)
(
    df_bronze.write
    .format("delta")
    .mode("append")                         # replace existing bronze data
    .option("mergeSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

# look the data


display(spark.read.table(BRONZE_TABLE))
