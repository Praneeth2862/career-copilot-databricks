# Databricks notebook source
# Purpose: Extract normalized skills from raw resume text into silver.resume_skills

from pyspark.sql import functions as F

SKILL_REGEX = "(python|sql|excel|statistics|pandas|numpy|power bi|tableau|machine learning|data structures|algorithms|api design|product sense|a/b testing)"

raw = spark.table("bronze.raw_resumes")

skills = (
    raw
    .withColumn("skill_raw", F.explode(F.split(F.lower(F.col("raw_text")), "[^a-z0-9/+ ]+")))
    .withColumn("skill_raw", F.trim(F.col("skill_raw")))
    .filter(F.col("skill_raw").rlike(SKILL_REGEX))
    .withColumn("normalized_skill", F.regexp_replace(F.col("skill_raw"), " ", "_"))
    .select("user_id", "normalized_skill")
    .dropDuplicates(["user_id", "normalized_skill"])
    .withColumn("confidence", F.lit(0.8))
    .withColumn("processed_at", F.current_timestamp())
)

(
    skills
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("silver.resume_skills")
)

print("Silver skill build complete")
