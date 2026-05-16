# Databricks notebook source
# DBTITLE 1,Bronze Layer - Raw Resume File Ingestion
# Purpose: Detect and catalog resume files uploaded to Volumes
# This is the BRONZE layer - we just track what files exist, no parsing yet

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
import os
from datetime import datetime

# Configuration
VOLUME_BASE_PATH = "/Volumes/career_copilot_dev/common/filestore-common/ResumeData"
BRONZE_TABLE = "career_copilot_dev.main.bronze_raw_resumes"

print(f"🔍 Scanning for resume files in: {VOLUME_BASE_PATH}")
print(f"📊 Target table: {BRONZE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Create Bronze Table Schema
# Create bronze table to track uploaded resume files
# This table stores file metadata and processing status

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
  file_id STRING NOT NULL,              -- Unique identifier for the file (hash of file_path)
  user_id STRING NOT NULL,              -- Extracted from Volume path
  file_path STRING NOT NULL,            -- Full path in Volume
  file_name STRING NOT NULL,            -- Original filename
  file_type STRING,                     -- pdf, docx, txt
  file_size_bytes BIGINT,               -- File size
  ingestion_timestamp TIMESTAMP NOT NULL, -- When we first detected this file
  processing_status STRING NOT NULL,    -- pending, processing, completed, failed
  last_updated TIMESTAMP NOT NULL,      -- Last status update
  error_message STRING,                 -- Error details if failed
  CONSTRAINT bronze_raw_resumes_pk PRIMARY KEY (file_id)
) USING DELTA
""")

print("✅ Bronze table created/verified")

# COMMAND ----------

# DBTITLE 1,Scan Volume for Resume Files
# Recursively list all files in the ResumeData directory
# Each user has their own subfolder: user_{uuid}/

import re

try:
    # List all files recursively using dbutils
    all_files = dbutils.fs.ls(VOLUME_BASE_PATH)
    
    resume_files = []
    
    # Recursively scan user directories
    for item in all_files:
        if item.isDir():
            # This is a user directory (e.g., user_635b7e6f-ebef-4f6e-ad2c-cbd6f04c951d/)
            user_files = dbutils.fs.ls(item.path)
            for file_item in user_files:
                if not file_item.isDir():
                    # Extract user_id from path
                    match = re.search(r'/user_([a-f0-9\-]+)/', file_item.path)
                    if match:
                        user_id = match.group(1)
                        resume_files.append({
                            'file_path': file_item.path,
                            'file_name': file_item.name,
                            'file_size_bytes': file_item.size,
                            'user_id': user_id
                        })
    
    print(f"📝 Found {len(resume_files)} resume files")
    
    # Create DataFrame from discovered files
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
        print("⚠️ No files found in Volume")
        files_df = None
        
except Exception as e:
    print(f"❌ Error scanning Volume: {e}")
    import traceback
    traceback.print_exc()
    files_df = None

# COMMAND ----------

# DBTITLE 1,Process and Insert New Files into Bronze
# Add file metadata and insert into bronze table
# Only insert files that aren't already tracked (idempotency)

if files_df is not None and files_df.count() > 0:
    # Add metadata columns
    new_files_df = (
        files_df
        .withColumn("file_id", F.sha2(F.col("file_path"), 256))  # Unique ID from file path
        .withColumn("file_type", 
                    F.when(F.lower(F.col("file_name")).endswith(".pdf"), "pdf")
                     .when(F.lower(F.col("file_name")).endswith(".docx"), "docx")
                     .when(F.lower(F.col("file_name")).endswith(".txt"), "txt")
                     .otherwise("unknown"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("processing_status", F.lit("pending"))
        .withColumn("last_updated", F.current_timestamp())
        .withColumn("error_message", F.lit(None).cast("string"))
    )
    
    # Read existing bronze table
    existing_df = spark.table(BRONZE_TABLE)
    existing_file_ids = {row.file_id for row in existing_df.select("file_id").collect()}
    
    # Filter to only new files (not already in bronze)
    truly_new_files = new_files_df.filter(~F.col("file_id").isin(existing_file_ids))
    
    new_count = truly_new_files.count()
    
    if new_count > 0:
        # Insert new files into bronze table
        truly_new_files.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
        print(f"✅ Ingested {new_count} new files into bronze table")
        display(truly_new_files.select("user_id", "file_name", "file_type", "file_size_bytes", "processing_status"))
    else:
        print("✅ No new files to ingest (all files already tracked)")
else:
    print("ℹ️ No files to process")

print("\n🎯 Bronze ingestion complete!")

# COMMAND ----------


