# Career Copilot Databricks Repo

Lakehouse pipelines, contracts, and workflows for AI Career Copilot.

## Contains
- `databricks/sql`: Bronze/Silver/Gold table DDL
- `databricks/notebooks`: ingestion and transformation notebooks
- `databricks/workflows`: Databricks job JSON definitions
- `data_contracts/`: schemas and quality rules
- `docs/`: architecture/product references

## Suggested Apply Order
1. Run SQL DDL: `databricks/sql/01_create_tables.sql`
2. Import notebooks from `databricks/notebooks/`
3. Create workflow from `databricks/workflows/career_copilot_job.json`
4. Wire upstream payload path for resume ingestion

## Governance Baseline
- Bronze raw data restricted
- Silver/Gold role-based access
- Audit logs enabled at workspace/account level
