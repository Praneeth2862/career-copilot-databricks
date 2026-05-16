-- Canonical namespace config
-- Catalog: career_copilot_dev
-- Schema: main
-- Naming pattern: <layer>_<entity>

-- Bronze
CREATE TABLE IF NOT EXISTS career_copilot_dev.main.bronze_raw_resumes (
  file_id STRING,
  user_id STRING,
  file_path STRING,
  file_name STRING,
  file_type STRING,
  file_size_bytes BIGINT,
  ingestion_timestamp TIMESTAMP,
  processing_status STRING,
  last_updated TIMESTAMP,
  error_message STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.bronze_raw_job_posts (
  role STRING,
  source STRING,
  title STRING,
  description STRING,
  ingested_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.bronze_raw_user_events (
  user_id STRING,
  event_name STRING,
  payload STRING,
  event_ts TIMESTAMP
) USING DELTA;

-- Silver
CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_text (
  file_id STRING,
  user_id STRING,
  raw_text STRING,
  text_length INT,
  extraction_method STRING,
  extraction_status STRING,
  error_message STRING,
  extracted_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_skills (
  user_id STRING,
  normalized_skill STRING,
  confidence DOUBLE,
  processed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_role_skill_demand (
  role STRING,
  skill STRING,
  importance DOUBLE,
  tier STRING,
  processed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_text_clean (
  file_id STRING,
  user_id STRING,
  clean_text STRING,
  token_count INT,
  text_quality_flag STRING,
  cleaned_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_sections (
  file_id STRING,
  user_id STRING,
  has_skills_section BOOLEAN,
  has_experience_section BOOLEAN,
  has_projects_section BOOLEAN,
  has_education_section BOOLEAN,
  processed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_entities (
  file_id STRING,
  user_id STRING,
  entity_type STRING,
  entity_value STRING,
  confidence DOUBLE,
  processed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_skill_evidence (
  file_id STRING,
  user_id STRING,
  normalized_skill STRING,
  source_section STRING,
  evidence_snippet STRING,
  confidence DOUBLE,
  processed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_experience_features (
  file_id STRING,
  user_id STRING,
  experience_years_est INT,
  has_internship BOOLEAN,
  has_freelance BOOLEAN,
  feature_computed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_resume_quality (
  file_id STRING,
  user_id STRING,
  is_empty_text BOOLEAN,
  is_too_short BOOLEAN,
  quality_score INT,
  quality_checked_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_pii_redacted_text (
  file_id STRING,
  user_id STRING,
  redacted_text STRING,
  has_email BOOLEAN,
  has_phone BOOLEAN,
  has_url BOOLEAN,
  redacted_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.silver_quarantine_resume_records (
  file_id STRING,
  user_id STRING,
  quarantine_reason STRING,
  quality_score INT,
  quarantined_at TIMESTAMP
) USING DELTA;

-- Gold
CREATE TABLE IF NOT EXISTS career_copilot_dev.main.gold_user_skill_profile (
  user_id STRING,
  role STRING,
  skills ARRAY<STRING>,
  updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.gold_fit_features (
  user_id STRING,
  role STRING,
  score INT,
  missing_critical_skills ARRAY<STRING>,
  updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS career_copilot_dev.main.gold_progress_metrics (
  user_id STRING,
  completion_percent INT,
  streak_days INT,
  updated_at TIMESTAMP
) USING DELTA;
