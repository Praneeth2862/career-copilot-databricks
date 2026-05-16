# Databricks notebook source
# Purpose: Build gold user_skill_profile and fit_features

from pyspark.sql import functions as F

CATALOG = "career_copilot_dev"
SCHEMA = "main"
SILVER_RESUME_SKILLS = f"{CATALOG}.{SCHEMA}.silver_resume_skills"
GOLD_USER_SKILL_PROFILE = f"{CATALOG}.{SCHEMA}.gold_user_skill_profile"
GOLD_FIT_FEATURES = f"{CATALOG}.{SCHEMA}.gold_fit_features"

user_skills = (
    spark.table(SILVER_RESUME_SKILLS)
    .groupBy("user_id")
    .agg(F.collect_set("normalized_skill").alias("skills"))
)

role_default = "data_analyst"

profile = (
    user_skills
    .withColumn("role", F.lit(role_default))
    .withColumn("updated_at", F.current_timestamp())
)

(
    profile
    .select("user_id", "role", "skills", "updated_at")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_USER_SKILL_PROFILE)
)

# Placeholder deterministic score for SQL-serving integration
fit = (
    profile
    .withColumn("score", F.least(F.lit(100), F.size("skills") * F.lit(10)))
    .withColumn("missing_critical_skills", F.array())
    .withColumn("updated_at", F.current_timestamp())
)

(
    fit
    .select("user_id", "role", "score", "missing_critical_skills", "updated_at")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_FIT_FEATURES)
)

print("Gold features refreshed")
