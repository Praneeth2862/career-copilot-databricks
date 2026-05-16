# Databricks notebook source
# Reusable Silver-layer helper functions

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# COMMAND ----------

DEFAULT_SKILL_REGEX = (
    r"(python|sql|excel|statistics|pandas|numpy|power bi|tableau|machine learning|"
    r"data structures|algorithms|api design|product sense|a/b testing|spark|pyspark|"
    r"scala|java|javascript|react|nodejs|aws|azure|gcp|docker|kubernetes|git|jenkins|"
    r"ci/cd|rest api|graphql|mongodb|postgresql|mysql|redis|kafka|airflow|dbt|looker|"
    r"metabase|r programming|scikit-learn|tensorflow|pytorch|nlp|deep learning|"
    r"computer vision|data engineering|etl|data warehousing|snowflake|redshift|"
    r"bigquery|hadoop|hive|presto|linux|bash|shell scripting|agile|scrum|jira|confluence)"
)


PII_REGEX_EMAIL = r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)"
PII_REGEX_PHONE = r"(\+?\d[\d\-\s]{8,}\d)"
PII_REGEX_URL = r"(https?://\S+|www\.\S+)"


# COMMAND ----------


def clean_resume_text(df: DataFrame, text_col: str = "raw_text") -> DataFrame:
    """Normalize text for downstream extraction while preserving source columns."""
    clean_col = F.lower(F.col(text_col))
    clean_col = F.regexp_replace(clean_col, r"[\r\n\t]+", " ")
    clean_col = F.regexp_replace(clean_col, r"\s+", " ")
    clean_col = F.regexp_replace(clean_col, r"\u00a0", " ")
    clean_col = F.trim(clean_col)

    return (
        df
        .withColumn("clean_text", clean_col)
        .withColumn("token_count", F.size(F.split(F.col("clean_text"), r"\s+")))
        .withColumn("text_quality_flag", F.when(F.col("token_count") < 30, F.lit("low_text")).otherwise(F.lit("ok")))
    )


# COMMAND ----------


def redact_pii(df: DataFrame, text_col: str = "clean_text") -> DataFrame:
    """Redact common direct identifiers from text used in downstream modeling."""
    redacted = F.regexp_replace(F.col(text_col), PII_REGEX_EMAIL, "<EMAIL>")
    redacted = F.regexp_replace(redacted, PII_REGEX_PHONE, "<PHONE>")
    redacted = F.regexp_replace(redacted, PII_REGEX_URL, "<URL>")

    return (
        df
        .withColumn("redacted_text", redacted)
        .withColumn("has_email", F.col(text_col).rlike(PII_REGEX_EMAIL))
        .withColumn("has_phone", F.col(text_col).rlike(PII_REGEX_PHONE))
        .withColumn("has_url", F.col(text_col).rlike(PII_REGEX_URL))
    )


# COMMAND ----------


def extract_sections(df: DataFrame, text_col: str = "clean_text") -> DataFrame:
    """Create lightweight section flags (v1 heuristic) for experience, projects, education, skills."""
    return (
        df
        .withColumn("has_skills_section", F.col(text_col).rlike(r"\bskills?\b"))
        .withColumn("has_experience_section", F.col(text_col).rlike(r"\b(experience|employment|work history)\b"))
        .withColumn("has_projects_section", F.col(text_col).rlike(r"\bprojects?\b"))
        .withColumn("has_education_section", F.col(text_col).rlike(r"\beducation\b"))
    )


# COMMAND ----------


def extract_skills(df: DataFrame, text_col: str = "clean_text", skill_regex: str = DEFAULT_SKILL_REGEX) -> DataFrame:
    """Extract skill tokens from normalized text."""
    return (
        df
        .select("file_id", "user_id", text_col)
        .withColumn("skill_raw", F.explode(F.split(F.col(text_col), r"[^a-z0-9/+ ]+")))
        .withColumn("skill_raw", F.trim(F.col("skill_raw")))
        .filter(F.col("skill_raw").rlike(skill_regex))
        .withColumn("normalized_skill", F.regexp_replace(F.col("skill_raw"), " ", "_"))
        .dropDuplicates(["user_id", "normalized_skill"])
        .withColumn("confidence", F.lit(0.8))
        .withColumn("processed_at", F.current_timestamp())
    )


# COMMAND ----------


def build_skill_evidence(df: DataFrame, skills_df: DataFrame, text_col: str = "clean_text") -> DataFrame:
    """Build evidence-level rows to support explainability in Gold scoring."""
    return (
        skills_df.alias("s")
        .join(df.select("file_id", "user_id", text_col).alias("d"), on=["file_id", "user_id"], how="left")
        .withColumn(
            "evidence_snippet",
            F.substring(F.col(text_col), 1, 220)
        )
        .withColumn("source_section", F.lit("unknown"))
        .select(
            "file_id",
            "user_id",
            "normalized_skill",
            "source_section",
            "evidence_snippet",
            "confidence",
            "processed_at"
        )
    )


# COMMAND ----------


def extract_experience_features(df: DataFrame, text_col: str = "clean_text") -> DataFrame:
    """Create first-pass experience features from resume text."""
    years_pattern = r"(\d+)\+?\s*(years|yrs)"

    return (
        df
        .withColumn("experience_years_est", F.regexp_extract(F.col(text_col), years_pattern, 1).cast("int"))
        .withColumn("has_internship", F.col(text_col).rlike(r"\bintern(ship)?\b"))
        .withColumn("has_freelance", F.col(text_col).rlike(r"\bfreelance\b"))
        .withColumn("feature_computed_at", F.current_timestamp())
        .select("file_id", "user_id", "experience_years_est", "has_internship", "has_freelance", "feature_computed_at")
    )


# COMMAND ----------


def build_quality_metrics(df: DataFrame) -> DataFrame:
    """Create quality metrics used for quarantine decisions."""
    return (
        df
        .withColumn("is_empty_text", F.col("clean_text").isNull() | (F.length(F.col("clean_text")) == 0))
        .withColumn("is_too_short", F.col("token_count") < 30)
        .withColumn("quality_score",
                    F.when(F.col("is_empty_text"), F.lit(0))
                     .when(F.col("is_too_short"), F.lit(40))
                     .otherwise(F.lit(90)))
    )


# COMMAND ----------


def split_quarantine(df: DataFrame):
    """Return (good_df, quarantine_df) based on quality thresholds."""
    quarantine_df = (
        df
        .filter(F.col("is_empty_text") | F.col("is_too_short"))
        .withColumn(
            "quarantine_reason",
            F.when(F.col("is_empty_text"), F.lit("empty_text"))
             .when(F.col("is_too_short"), F.lit("text_too_short"))
             .otherwise(F.lit("quality_check_failed"))
        )
    )

    good_df = df.filter(~(F.col("is_empty_text") | F.col("is_too_short")))
    return good_df, quarantine_df
