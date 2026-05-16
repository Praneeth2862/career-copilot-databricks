# Databricks notebook source
# DBTITLE 1,Silver Layer - Orchestration Notebook
# Purpose: Extract text and run standardized Silver cleanup/extraction pipeline

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../helpers/silver_helper

# COMMAND ----------

print("Starting Silver layer processing...")

# Namespace and path configuration
CATALOG = "career_copilot_dev"
SCHEMA = "main"
VOLUME_BASE_PATH = "/Volumes/career_copilot_dev/common/filestore-common/ResumeData"
BRONZE_RAW_RESUMES = f"{CATALOG}.{SCHEMA}.bronze_raw_resumes"
SILVER_RESUME_TEXT = f"{CATALOG}.{SCHEMA}.silver_resume_text"
SILVER_RESUME_SKILLS = f"{CATALOG}.{SCHEMA}.silver_resume_skills"
SILVER_RESUME_TEXT_CLEAN = f"{CATALOG}.{SCHEMA}.silver_resume_text_clean"
SILVER_RESUME_SECTIONS = f"{CATALOG}.{SCHEMA}.silver_resume_sections"
SILVER_RESUME_ENTITIES = f"{CATALOG}.{SCHEMA}.silver_resume_entities"
SILVER_SKILL_EVIDENCE = f"{CATALOG}.{SCHEMA}.silver_resume_skill_evidence"
SILVER_EXPERIENCE_FEATURES = f"{CATALOG}.{SCHEMA}.silver_resume_experience_features"
SILVER_RESUME_QUALITY = f"{CATALOG}.{SCHEMA}.silver_resume_quality"
SILVER_PII_REDACTED_TEXT = f"{CATALOG}.{SCHEMA}.silver_pii_redacted_text"
SILVER_QUARANTINE = f"{CATALOG}.{SCHEMA}.silver_quarantine_resume_records"

# COMMAND ----------

# DBTITLE 1,Ensure Core Silver Tables Exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_RESUME_TEXT} (
  file_id STRING NOT NULL,
  user_id STRING NOT NULL,
  raw_text STRING,
  text_length INT,
  extraction_method STRING,
  extraction_status STRING,
  error_message STRING,
  extracted_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_RESUME_SKILLS} (
  file_id STRING,
  user_id STRING NOT NULL,
  normalized_skill STRING NOT NULL,
  confidence DOUBLE,
  processed_at TIMESTAMP
) USING DELTA
""")

print("Verified core Silver tables")

# COMMAND ----------

# DBTITLE 1,Install Text Extraction Libraries
%pip install PyPDF2 python-docx --quiet

# COMMAND ----------

# DBTITLE 1,Extract Raw Text From Pending Bronze Files
import PyPDF2
import docx
import io
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

bronze_df = spark.table(BRONZE_RAW_RESUMES).filter(F.col("processing_status") == "pending")
pending_count = bronze_df.count()

if pending_count == 0:
    print("No pending files to process")
else:
    print(f"Processing {pending_count} files...")

    binary_df = (
        spark.read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(VOLUME_BASE_PATH)
    )

    files_to_process = bronze_df.join(
        binary_df,
        bronze_df.file_path == binary_df.path,
        "inner"
    ).select(
        bronze_df.file_id,
        bronze_df.user_id,
        bronze_df.file_type,
        binary_df.content.alias("file_content")
    )

    matched_count = files_to_process.count()
    print(f"Found {matched_count} matching file(s) to process")

    results_schema = StructType([
        StructField("file_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("raw_text", StringType(), True),
        StructField("text_length", IntegerType(), True),
        StructField("extraction_method", StringType(), True),
        StructField("extraction_status", StringType(), True),
        StructField("error_message", StringType(), True)
    ])

    results = []
    for row in files_to_process.collect():
        try:
            file_bytes = bytes(row.file_content)

            if row.file_type == "pdf":
                pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_bytes))
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
                text = text.strip()
                status = "success"
                error = None

            elif row.file_type == "docx":
                doc = docx.Document(io.BytesIO(file_bytes))
                text = "\n".join([para.text for para in doc.paragraphs]).strip()
                status = "success"
                error = None

            elif row.file_type == "txt":
                text = file_bytes.decode("utf-8", errors="ignore").strip()
                status = "success"
                error = None

            else:
                text = ""
                status = "failed"
                error = f"Unsupported file type: {row.file_type}"

            results.append((
                row.file_id,
                row.user_id,
                text,
                len(text),
                f"PyPDF2/{row.file_type}",
                status,
                error
            ))

        except Exception as e:
            results.append((
                row.file_id,
                row.user_id,
                "",
                0,
                f"PyPDF2/{row.file_type}",
                "failed",
                str(e)
            ))

    if results:
        extracted_df = spark.createDataFrame(results, schema=results_schema).withColumn("extracted_at", F.current_timestamp())
        extracted_df.write.format("delta").mode("append").saveAsTable(SILVER_RESUME_TEXT)

        for row in extracted_df.collect():
            next_status = "completed" if row.extraction_status == "success" else "failed"
            safe_err = (row.error_message or "").replace("'", "''")
            spark.sql(f"""
                UPDATE {BRONZE_RAW_RESUMES}
                SET processing_status = '{next_status}',
                    last_updated = CURRENT_TIMESTAMP(),
                    error_message = '{safe_err}'
                WHERE file_id = '{row.file_id}'
            """)

        print(f"Extracted text from {len(results)} file(s)")

# COMMAND ----------

# DBTITLE 1,Run Helper-Based Silver Transformations
success_df = spark.table(SILVER_RESUME_TEXT).filter(F.col("extraction_status") == "success")
success_count = success_df.count()

if success_count == 0:
    print("No successful text records to transform")
else:
    print(f"Running helper pipeline on {success_count} record(s)")

    cleaned_df = clean_resume_text(success_df, text_col="raw_text")
    section_df = extract_sections(cleaned_df, text_col="clean_text")
    redacted_df = redact_pii(cleaned_df, text_col="clean_text")
    quality_df = build_quality_metrics(cleaned_df)
    good_df, quarantine_df = split_quarantine(quality_df)

    skills_df = extract_skills(good_df, text_col="clean_text")
    evidence_df = build_skill_evidence(good_df, skills_df, text_col="clean_text")
    exp_features_df = extract_experience_features(good_df, text_col="clean_text")

    # Minimal entity table from skill outputs (v1)
    entities_df = (
        skills_df
        .select("file_id", "user_id", F.lit("skill").alias("entity_type"), F.col("normalized_skill").alias("entity_value"), "confidence", "processed_at")
    )

    # Write Silver outputs
    (
        cleaned_df
        .select("file_id", "user_id", "clean_text", "token_count", "text_quality_flag", F.current_timestamp().alias("cleaned_at"))
        .write.format("delta").mode("append").saveAsTable(SILVER_RESUME_TEXT_CLEAN)
    )

    (
        section_df
        .select(
            "file_id", "user_id", "has_skills_section", "has_experience_section",
            "has_projects_section", "has_education_section", F.current_timestamp().alias("processed_at")
        )
        .write.format("delta").mode("append").saveAsTable(SILVER_RESUME_SECTIONS)
    )

    entities_df.write.format("delta").mode("append").saveAsTable(SILVER_RESUME_ENTITIES)
    skills_df.write.format("delta").mode("append").saveAsTable(SILVER_RESUME_SKILLS)
    evidence_df.write.format("delta").mode("append").saveAsTable(SILVER_SKILL_EVIDENCE)
    exp_features_df.write.format("delta").mode("append").saveAsTable(SILVER_EXPERIENCE_FEATURES)

    (
        quality_df
        .select("file_id", "user_id", "is_empty_text", "is_too_short", "quality_score", F.current_timestamp().alias("quality_checked_at"))
        .write.format("delta").mode("append").saveAsTable(SILVER_RESUME_QUALITY)
    )

    (
        redacted_df
        .select("file_id", "user_id", "redacted_text", "has_email", "has_phone", "has_url", F.current_timestamp().alias("redacted_at"))
        .write.format("delta").mode("append").saveAsTable(SILVER_PII_REDACTED_TEXT)
    )

    if quarantine_df.count() > 0:
        (
            quarantine_df
            .select("file_id", "user_id", "quarantine_reason", "quality_score", F.current_timestamp().alias("quarantined_at"))
            .write.format("delta").mode("append").saveAsTable(SILVER_QUARANTINE)
        )

    print("Silver helper pipeline completed")
    print(f"Valid skill records: {skills_df.count()}")
    print(f"Quarantined records: {quarantine_df.count()}")
