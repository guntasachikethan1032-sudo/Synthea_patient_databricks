<img width="811" height="257" alt="image" src="https://github.com/user-attachments/assets/81cc23da-dce2-4b40-af61-4a9a5eee31fd" />


# Synthea_patient_databricks
I have used this repository to showcase my learnings from the past week. 

**Project Summary:**

This project demonstrates a healthcare data pipeline built using the Synthea dataset to simulate real-world healthcare engineering workflows. My goal was to gain practical experience with Databricks, the Unity catalog, PySpark, the Medallion architecture, and cloud-based data ingestion, and to learn how to leverage these in a production support role focused on Azure Data Factory (ADF) and Databricks.

Initially, I planned to build the project fully in the Azure ecosystem by ingesting data into Azure Storage and processing it in Azure Databricks. However, due to compute limitations in the free edition of Azure Databricks, I adapted the architecture and used AWS S3 as the raw storage layer, then connected it to Databricks Lakeflow for ingestion and transformation.

This project helped me understand how modern data engineering pipelines are designed, built, and optimized across cloud platforms.


**Objectives Achieved**


--> Ingested from cloud object storage --> / Understood external connections and explored marketplace.

--> loaded into a Bronze layer for raw storage-->/ Worked with and learned Read/ Write properties and various options.EX: "mode" = "append"  

--> transformed and standardized in a Silver layer--> Wide and narrow transformations, filtering, sorting, using window functions, partitioning, JOINS. 

--> queried and aggregated into a Gold layer for analytics use cases



**Key Learning Outcomes through my project and preparation**

Through this project and my preparation, I gained a hands-on understanding of these main architectural and technical concepts, which are very crucial for a production support position. 


Spark Architecture

Lakehouse data pipeline design

Medallion Architecture (Bronze → Silver → Gold data layers)

Databricks workspace organization (catalog, schema, notebooks, pipelines)

Cross-cloud data integration (AWS S3 with Databricks)

Spark execution fundamentals (DAG, partitions, lazy execution)

Wide vs Narrow transformations

Unity Catalog

Delta Lake/table

Databricks notebooks in ADF-style orchestration workflows

Databricks' capabilities that help us deal with large datasets


**Technical learnings:**

Data Ingestion using Lakeflow Connect.

Reading cloud data using PySpark (spark.read)

Loading CSV data from AWS S3

Schema inference and ingestion metadata

Data Transformation

PySpark DataFrame operations

Data cleaning, filtering, and column transformations

Bronze → Silver data processing

Data Analytics

Spark SQL queries for Gold-layer datasets

Aggregations and analytics-ready tables

Spark Optimization

Broadcast joins

Repartition / Coalesce

Caching



