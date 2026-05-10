# Architecture

## Layers
- App Layer: Next.js frontend + FastAPI backend
- Data Layer: Databricks Bronze/Silver/Gold tables
- Intelligence Layer: Deterministic fit/gap/roadmap/project recommender

## Security Baseline
- PII minimization
- Secrets in environment variables
- Role-based table permissions in Databricks
- Audit logs for inference and upload events
