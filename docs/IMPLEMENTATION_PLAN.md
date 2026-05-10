# Career Copilot Databricks Repo - Implementation Plan

## Goal
Deliver a production-grade Lakehouse pipeline (Bronze/Silver/Gold) powering skill profiles, fit features, and progress metrics for the app.

## Current State
- SQL DDL template exists
- 3 notebooks exist for ingestion and feature build
- Workflow JSON exists
- Data contracts and quality rules exist

## Phase Plan

### Phase 1 - Workspace Bootstrap (Days 1-2)
1. Create catalogs/schemas (`bronze`, `silver`, `gold`) in Databricks workspace.
2. Apply `databricks/sql/01_create_tables.sql`.
3. Import notebooks into workspace repo path.
4. Run notebooks manually in sequence to validate baseline execution.

Definition of Done:
- all required tables exist
- notebooks run without errors on sample input

### Phase 2 - Ingestion Reliability (Days 3-4)
1. Define canonical resume payload format at ingestion boundary.
2. Add schema checks before Bronze writes.
3. Add idempotency key strategy for duplicate uploads.
4. Set ingestion status table for run tracking.

Definition of Done:
- duplicate ingestion controlled
- malformed payloads rejected with clear reason

### Phase 3 - Silver/Gold Quality (Week 2)
1. Improve skill extraction normalization and synonym mapping.
2. Add quality gates based on `data_contracts/quality_rules`.
3. Build `gold.user_skill_profile`, `gold.fit_features`, `gold.progress_metrics` with deterministic logic.
4. Add run metadata columns (`run_id`, `processed_at`).

Definition of Done:
- quality checks pass for each pipeline run
- gold outputs are query-ready and stable

### Phase 4 - Workflow Automation (Week 3)
1. Convert notebook sequence into scheduled Databricks job.
2. Add retry and timeout policies.
3. Add failure alerts (email/webhook).
4. Add weekly full refresh + daily incremental strategy.

Definition of Done:
- scheduled runs execute automatically
- failures alert with actionable context

### Phase 5 - Governance and Access Controls (Week 4)
1. Define role model: data_engineer, app_service, analyst.
2. Restrict Bronze raw text access.
3. Grant app role read access only to approved Gold tables.
4. Enable and validate audit logging.
5. Add retention + delete workflow design for user-level data.

Definition of Done:
- least-privilege access enforced
- audit trail available for access and job runs

## Priority Backlog (Execution Order)
1. Apply DDL and run notebooks manually
2. Add ingestion/schema validation
3. Add data quality checks
4. Automate jobs and alerts
5. Lock down governance permissions

## Run Order for Daily Pipeline
1. `01_ingest_resume.py`
2. `02_build_silver_skills.py`
3. `03_build_gold_features.py`

## Branching Workflow
- Base branch: `develop`
- Feature branches: `feature/<pipeline-change>`
- PR target: `develop`

## PR Checklist
- SQL/notebook changes tested on sample run
- quality rule impact documented
- table contract changes documented
- no workspace-specific secrets committed

## Handoff Prompt for New Chat Session
"Use `docs/IMPLEMENTATION_PLAN.md` as source of truth. Start with Phase 1 bootstrap in Databricks and validate each phase before moving to the next."
