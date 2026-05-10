-- Bronze
CREATE TABLE IF NOT EXISTS bronze.raw_resumes (
  user_id STRING,
  file_name STRING,
  raw_text STRING,
  uploaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.raw_job_posts (
  role STRING,
  source STRING,
  title STRING,
  description STRING,
  ingested_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.raw_user_events (
  user_id STRING,
  event_name STRING,
  payload STRING,
  event_ts TIMESTAMP
) USING DELTA;

-- Silver
CREATE TABLE IF NOT EXISTS silver.resume_skills (
  user_id STRING,
  normalized_skill STRING,
  confidence DOUBLE,
  processed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.role_skill_demand (
  role STRING,
  skill STRING,
  importance DOUBLE,
  tier STRING,
  processed_at TIMESTAMP
) USING DELTA;

-- Gold
CREATE TABLE IF NOT EXISTS gold.user_skill_profile (
  user_id STRING,
  role STRING,
  skills ARRAY<STRING>,
  updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.fit_features (
  user_id STRING,
  role STRING,
  score INT,
  missing_critical_skills ARRAY<STRING>,
  updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.progress_metrics (
  user_id STRING,
  completion_percent INT,
  streak_days INT,
  updated_at TIMESTAMP
) USING DELTA;
