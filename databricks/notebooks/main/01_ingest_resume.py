# Databricks notebook source
# DBTITLE 1,Bronze Layer - Raw Resume File Ingestion
# Purpose: Detect and catalog resume files uploaded to Volumes
# This is the BRONZE layer - we just track what files exist, no parsing yet

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import re

# Namespace and path configuration
CATALOG = "career_copilot_dev"
SCHEMA = "main"
VOLUME_BASE_PATH = "/Volumes/career_copilot_dev/common/filestore-common/ResumeData"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_raw_resumes"

print(f"Scanning for resume files in: {VOLUME_BASE_PATH}")
print(f"Target table: {BRONZE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Create Bronze Table Schema
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
  file_id STRING NOT NULL,
  user_id STRING NOT NULL,
  file_path STRING NOT NULL,
  file_name STRING NOT NULL,
  file_type STRING,
  file_size_bytes BIGINT,
  ingestion_timestamp TIMESTAMP NOT NULL,
  processing_status STRING NOT NULL,
  last_updated TIMESTAMP NOT NULL,
  error_message STRING
) USING DELTA
""")

print("Bronze table created/verified")

# COMMAND ----------

# DBTITLE 1,Scan Volume for Resume Files
try:
    all_files = dbutils.fs.ls(VOLUME_BASE_PATH)
    resume_files = []

    for item in all_files:
        if item.isDir():
            user_files = dbutils.fs.ls(item.path)
            for file_item in user_files:
                if not file_item.isDir():
                    match = re.search(r'/user_([a-f0-9\-]+)/', file_item.path)
                    if match:
                        user_id = match.group(1)
                        resume_files.append({
                            'file_path': file_item.path,
                            'file_name': file_item.name,
                            'file_size_bytes': file_item.size,
                            'user_id': user_id
                        })

    print(f"Found {len(resume_files)} resume files")

    if resume_files:
        schema = StructType([
            StructField("file_path", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("file_size_bytes", LongType(), True),
            StructField("user_id", StringType(), False)
        ])
        files_df = spark.createDataFrame(resume_files, schema=schema)
        display(files_df)
    else:
        print("No files found in Volume")
        files_df = None

except Exception as e:
    print(f"Error scanning Volume: {e}")
    files_df = None

# COMMAND ----------

# DBTITLE 1,Process and Insert New Files into Bronze
if files_df is not None and files_df.count() > 0:
    new_files_df = (
        files_df
        .withColumn("file_id", F.sha2(F.col("file_path"), 256))
        .withColumn(
            "file_type",
            F.when(F.lower(F.col("file_name")).endswith(".pdf"), "pdf")
            .when(F.lower(F.col("file_name")).endswith(".docx"), "docx")
            .when(F.lower(F.col("file_name")).endswith(".txt"), "txt")
            .otherwise("unknown")
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("processing_status", F.lit("pending"))
        .withColumn("last_updated", F.current_timestamp())
        .withColumn("error_message", F.lit(None).cast("string"))
    )

    existing_df = spark.table(BRONZE_TABLE)
    existing_file_ids = {row.file_id for row in existing_df.select("file_id").collect()}
    truly_new_files = new_files_df.filter(~F.col("file_id").isin(existing_file_ids))

    new_count = truly_new_files.count()
    if new_count > 0:
        truly_new_files.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
        print(f"Ingested {new_count} new files into bronze table")
        display(truly_new_files.select("user_id", "file_name", "file_type", "file_size_bytes", "processing_status"))
    else:
        print("No new files to ingest (all files already tracked)")
else:
    print("No files to process")

print("Bronze ingestion complete")
