# Databricks notebook source
# DBTITLE 1,Silver Layer - Text & Skill Extraction
# Purpose: Extract text from resume files and identify skills
# Step 1: Read PDF/DOCX files and extract raw text
# Step 2: Parse skills from extracted text

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

print("🚀 Starting Silver layer processing...")
print("📋 Step 1: Text Extraction")
print("📋 Step 2: Skill Extraction")

# COMMAND ----------

# DBTITLE 1,Create Silver Text Table
# Create table to store extracted resume text

spark.sql("""
CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_text (
  file_id STRING NOT NULL,
  user_id STRING NOT NULL,
  raw_text STRING,
  text_length INT,
  extraction_method STRING,
  extraction_status STRING,
  error_message STRING,
  extracted_at TIMESTAMP,
  CONSTRAINT silver_resume_text_pk PRIMARY KEY (file_id)
) USING DELTA
""")

print("✅ Silver text table created/verified")

# COMMAND ----------

# DBTITLE 1,Install Text Extraction Libraries
# Install libraries for reading PDF and DOCX files
%pip install PyPDF2 python-docx --quiet

# COMMAND ----------

# DBTITLE 1,Extract Text from Resume Files
# Extract text from PDF/DOCX/TXT files in Volumes
import PyPDF2
import docx
import io
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

print("\n📝 Extracting text from resume files...")

# Read pending files from bronze
bronze_df = spark.table("career_copilot_dev.main.bronze_raw_resumes").filter(F.col("processing_status") == "pending")

if bronze_df.count() == 0:
    print("ℹ️ No pending files to process")
else:
    print(f"📄 Processing {bronze_df.count()} files...")
    
    # Read files as binary RECURSIVELY to include subdirectories
    binary_df = (
        spark.read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")  # KEY: Read subdirectories!
        .load("/Volumes/career_copilot_dev/common/filestore-common/ResumeData")
    )
    
    # Join with bronze metadata
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
    
    print(f"✅ Found {files_to_process.count()} matching file(s) to process")
    
    # Define schema for results DataFrame
    results_schema = StructType([
        StructField("file_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("raw_text", StringType(), True),
        StructField("text_length", IntegerType(), True),
        StructField("extraction_method", StringType(), True),
        StructField("extraction_status", StringType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    # Process files locally (collect to driver for text extraction)
    results = []
    for row in files_to_process.collect():
        try:
            file_bytes = bytes(row.file_content)
            
            if row.file_type == "pdf":
                # Extract text from PDF
                pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_bytes))
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
                text = text.strip()
                status = "success"
                error = None
                
            elif row.file_type == "docx":
                # Extract text from DOCX
                doc = docx.Document(io.BytesIO(file_bytes))
                text = "\n".join([para.text for para in doc.paragraphs])
                text = text.strip()
                status = "success"
                error = None
                
            elif row.file_type == "txt":
                # Plain text file
                text = file_bytes.decode('utf-8', errors='ignore').strip()
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
        # Create DataFrame with explicit schema
        extracted_df = spark.createDataFrame(results, schema=results_schema).withColumn("extracted_at", F.current_timestamp())
        
        # Show sample
        print("\n📝 Sample extracted text:")
        display(extracted_df.select(
            "user_id", 
            "text_length", 
            "extraction_status",
            F.substring(F.col("raw_text"), 1, 200).alias("text_preview")
        ))
        
        # Save to silver text table
        extracted_df.write.format("delta").mode("append").saveAsTable("career_copilot_dev.main.silver_resume_text")
        
        # Update bronze status
        for row in extracted_df.collect():
            spark.sql(f"""
                UPDATE career_copilot_dev.main.bronze_raw_resumes
                SET processing_status = 'completed', last_updated = CURRENT_TIMESTAMP()
                WHERE file_id = '{row.file_id}'
            """)
        
        print(f"\n✅ Extracted text from {len(results)} file(s)")

# COMMAND ----------

# DBTITLE 1,Create Silver Skills Table
# Create table to store extracted skills

spark.sql("""
CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_skills (
  user_id STRING NOT NULL,
  normalized_skill STRING NOT NULL,
  confidence DOUBLE,
  processed_at TIMESTAMP,
  CONSTRAINT silver_resume_skills_pk PRIMARY KEY (user_id, normalized_skill)
) USING DELTA
""")

print("✅ Silver skills table created/verified")

# COMMAND ----------

# DBTITLE 1,Extract Skills from Resume Text
# Extract and normalize skills from resume text

print("\n🎯 Extracting skills from resume text...")

# Extended skill regex with common technical skills
SKILL_REGEX = "(python|sql|excel|statistics|pandas|numpy|power bi|tableau|machine learning|data structures|algorithms|api design|product sense|a/b testing|spark|pyspark|scala|java|javascript|react|nodejs|aws|azure|gcp|docker|kubernetes|git|jenkins|ci/cd|rest api|graphql|mongodb|postgresql|mysql|redis|kafka|airflow|dbt|looker|metabase|r programming|scikit-learn|tensorflow|pytorch|nlp|deep learning|computer vision|data engineering|etl|data warehousing|snowflake|redshift|bigquery|hadoop|hive|presto|linux|bash|shell scripting|agile|scrum|jira|confluence)"

# Read successfully extracted text
text_df = spark.table("career_copilot_dev.main.silver_resume_text").filter(F.col("extraction_status") == "success")

if text_df.count() == 0:
    print("⚠️ No successfully extracted text to process")
else:
    print(f"📝 Processing {text_df.count()} resume(s)...")
    
    # Extract skills using regex matching
    skills_df = (
        text_df
        .select("user_id", "raw_text")
        # Split text into words and convert to lowercase
        .withColumn("skill_raw", F.explode(F.split(F.lower(F.col("raw_text")), "[^a-z0-9/+ ]+")))
        .withColumn("skill_raw", F.trim(F.col("skill_raw")))
        # Filter to only known skills
        .filter(F.col("skill_raw").rlike(SKILL_REGEX))
        # Normalize skill names (replace spaces with underscores)
        .withColumn("normalized_skill", F.regexp_replace(F.col("skill_raw"), " ", "_"))
        .select("user_id", "normalized_skill")
        # Remove duplicates per user
        .dropDuplicates(["user_id", "normalized_skill"])
        # Add metadata
        .withColumn("confidence", F.lit(0.8))
        .withColumn("processed_at", F.current_timestamp())
    )
    
    skill_count = skills_df.count()
    
    if skill_count > 0:
        # Show extracted skills
        print(f"\n✅ Found {skill_count} unique skills")
        display(skills_df.groupBy("user_id").agg(
            F.count("normalized_skill").alias("skill_count"),
            F.collect_list("normalized_skill").alias("skills")
        ))
        
        # Save to silver skills table (merge to avoid duplicates)
        skills_df.write.format("delta").mode("append").saveAsTable("career_copilot_dev.main.silver_resume_skills")
        
        print(f"\n✅ Saved {skill_count} skills to silver table")
    else:
        print("⚠️ No skills found matching the regex pattern")

print("\n🎯 Silver skill extraction complete!")
