# Databricks notebook source
# Purpose: Ingest parsed resume payloads into bronze.raw_resumes

from pyspark.sql import functions as F

input_path = dbutils.widgets.get("input_path")

raw_df = spark.read.json(input_path)
(
    raw_df
    .withColumn("uploaded_at", F.current_timestamp())
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("bronze.raw_resumes")
)

print("Ingestion complete")
