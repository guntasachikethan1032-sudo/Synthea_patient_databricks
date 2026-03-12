from pyspark.sql import functions as F
from delta.tables import DeltaTable

# we are reading the data from the bronze layer

df_bronze = spark.read.table("synthea_patient_data.bronze.medications")


# transformations into the silver  table

df_silver = (
    df_bronze
    .select(
        F.col("START").cast("timestamp").alias("med_start_timestamp"),
        F.col("STOP").cast("timestamp").alias("med_stop_timestamp"),
        F.col("PATIENT").alias("patient_id"),
        F.col("PAYER").alias("payer_id"),
        F.col("ENCOUNTER").alias("encounter_id"),
        F.col("CODE").alias("medication_code"),
        F.col("DESCRIPTION").alias("medication_description"),
        F.col("BASE_COST").cast("double").alias("base_cost"),
        F.col("PAYER_COVERAGE").cast("double").alias("payer_coverage"),
        F.col("DISPENSES").cast("int").alias("dispenses"),
        F.col("TOTALCOST").cast("double").alias("total_cost"),
        F.col("REASONCODE").alias("reason_code"),
        F.col("REASONDESCRIPTION").alias("reason_description"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )
    .withColumn("medication_description", F.trim(F.col("medication_description")))
    .withColumn("reason_description", F.trim(F.col("reason_description")))
    .withColumn("med_start_date", F.to_date("med_start_timestamp"))
    .withColumn("med_stop_date", F.to_date("med_stop_timestamp"))
    .withColumn(
        "is_active",
        F.when(F.col("med_stop_timestamp").isNull(), F.lit(1)).otherwise(F.lit(0))
    )
    .fillna({
        "payer_coverage": 0.0,
        "base_cost": 0.0,
        "dispenses": 0,
        "total_cost": 0.0
    })
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)

# remove duplicates from current batch
df_silver = df_silver.dropDuplicates([
    "patient_id", "encounter_id", "medication_code", "med_start_timestamp"
])

# basic quality filter
df_silver = df_silver.filter(
    (F.col("patient_id").isNotNull()) &
    (F.col("medication_code").isNotNull()) &
    (F.col("med_start_timestamp").isNotNull())
)


#creates a temp view
df_silver.createOrReplaceTempView("vw_medications_silver")


#create a target table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("synthea_patient_data.silver.medications_processed")


target_table = DeltaTable.forName(spark, "synthea_patient_data.silver.medications_processed")

(
    target_table.alias("t")
    .merge(
        df_silver.alias("s"),
        """
        t.patient_id = s.patient_id
        AND t.encounter_id = s.encounter_id
        AND t.medication_code = s.medication_code
        AND t.med_start_timestamp = s.med_start_timestamp
        """
    )
    .whenMatchedUpdate(set={
        "med_stop_timestamp": "s.med_stop_timestamp",
        "payer_id": "s.payer_id",
        "medication_description": "s.medication_description",
        "base_cost": "s.base_cost",
        "payer_coverage": "s.payer_coverage",
        "dispenses": "s.dispenses",
        "total_cost": "s.total_cost",
        "reason_code": "s.reason_code",
        "reason_description": "s.reason_description",
        "bronze_ingest_timestamp": "s.bronze_ingest_timestamp",
        "med_start_date": "s.med_start_date",
        "med_stop_date": "s.med_stop_date",
        "is_active": "s.is_active",
        "silver_processed_timestamp": "s.silver_processed_timestamp"
    })
    .whenNotMatchedInsert(values={
        "med_start_timestamp": "s.med_start_timestamp",
        "med_stop_timestamp": "s.med_stop_timestamp",
        "patient_id": "s.patient_id",
        "payer_id": "s.payer_id",
        "encounter_id": "s.encounter_id",
        "medication_code": "s.medication_code",
        "medication_description": "s.medication_description",
        "base_cost": "s.base_cost",
        "payer_coverage": "s.payer_coverage",
        "dispenses": "s.dispenses",
        "total_cost": "s.total_cost",
        "reason_code": "s.reason_code",
        "reason_description": "s.reason_description",
        "bronze_ingest_timestamp": "s.bronze_ingest_timestamp",
        "med_start_date": "s.med_start_date",
        "med_stop_date": "s.med_stop_date",
        "is_active": "s.is_active",
        "silver_processed_timestamp": "s.silver_processed_timestamp"
    })
    .execute()
)



display(spark.read.table("synthea_patient_data.silver.medications_processed")) 







