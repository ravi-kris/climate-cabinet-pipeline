# Climate Cabinet Technical Mapping Project

State-level campaign finance medallion pipeline and analytics interface aligned to the technical mapping document.

## What this implements

- Bronze ingestion from local CSV files, OpenFEC API, or hybrid mode.
- Silver standardization, reconciliation checks, and exception capture.
- SCD Type 2 donor-candidate relationship history.
- Gold aggregations for district competitiveness and legislative brief generation.
- Airflow DAG orchestration with retry-friendly stage separation.
- dbt project scaffold for Silver/Gold modeling.
- Streamlit dashboard for contribution mix, trends, and top donors.
- Terraform starter for data lake and IAM baseline.
- GitHub Actions CI for tests and smoke pipeline run.
- Docker and docker-compose stack for pipeline, Airflow, dbt, and dashboard.

## Repository layout

- `src/climate_cabinet_pipeline/`: pipeline package, ingestion, and CLI
- `data/sources/`: source campaign finance sample data
- `data/bronze/`, `data/silver/`, `data/gold/`: medallion outputs
- `dashboard/app.py`: visualization interface
- `dags/campaign_finance_pipeline.py`: Airflow DAG
- `dbt/`: dbt project, staging and marts models
- `infra/terraform/`: Terraform baseline
- `docker/`: Dockerfiles and dbt profile
- `tests/`: automated tests

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev,viz]'
```

## Run pipeline

Run all stages (file mode):

```bash
climate-cabinet-pipeline --stage all --ingestion-mode file
```

Run by stage:

```bash
climate-cabinet-pipeline --stage bronze --ingestion-mode file
climate-cabinet-pipeline --stage silver
climate-cabinet-pipeline --stage scd2
climate-cabinet-pipeline --stage gold
```

## API ingestion (OpenFEC + optional state CSV URLs)

API-only Bronze ingestion:

```bash
export OPENFEC_API_KEY='<your_key>'
climate-cabinet-pipeline \
  --stage bronze \
  --ingestion-mode api \
  --openfec-cycle 2026 \
  --openfec-pages 3 \
  --openfec-per-page 100 \
  --openfec-state CA \
  --persist-api-extracts
```

Hybrid mode (local files + API):

```bash
export OPENFEC_API_KEY='<your_key>'
climate-cabinet-pipeline \
  --stage all \
  --ingestion-mode hybrid \
  --openfec-cycle 2026 \
  --state-csv-url 'https://example-state-portal.gov/campaign_finance_export.csv' \
  --persist-api-extracts
```

Notes:
- `OPENFEC_API_KEY` can be passed via env var or `--openfec-api-key`.
- `--state-csv-url` can be repeated for multiple URLs.
- `--persist-api-extracts` writes API pulls into `data/sources/` for auditability.

## Outputs produced

- Bronze: `data/bronze/contributions_bronze.parquet`
- Silver:
  - `data/silver/contributions_silver.parquet`
  - `data/silver/quality_report.json`
  - `data/silver/validation_exceptions.csv`
  - `data/silver/donor_candidate_scd2.parquet`
- Gold:
  - `data/gold/district_metrics.parquet`
  - `data/gold/donor_breakdown.parquet`
  - `data/gold/contribution_time_series.parquet`
  - `data/gold/top_donors.parquet`

## Launch dashboard

```bash
streamlit run dashboard/app.py
```

## Docker compose stack

Copy env template:

```bash
cp .env.example .env
```

Run one-off pipeline container:

```bash
docker compose --profile pipeline run --rm pipeline
```

Run dashboard container:

```bash
docker compose --profile dashboard up dashboard
```

Run Airflow (webserver + scheduler + postgres):

```bash
docker compose up airflow-init airflow-webserver airflow-scheduler
```

Airflow UI: `http://localhost:8080` (username: `airflow`, password: `airflow`).

Run dbt parse/debug container:

```bash
docker compose --profile dbt run --rm dbt parse
docker compose --profile dbt run --rm dbt debug
```

## Airflow orchestration

The DAG in `dags/campaign_finance_pipeline.py` orchestrates:

1. Bronze ingestion (file/api/hybrid based on env)
2. Silver transform + quality checks
3. SCD2 history update
4. Gold aggregations

## dbt mapping

- `dbt/models/staging/stg_contributions.sql` normalizes and validates source rows.
- `dbt/models/marts/fct_district_metrics.sql` produces district-level KPI aggregates.

## Terraform baseline

`infra/terraform/` provisions:

- Encrypted versioned S3 bucket for raw Bronze landing
- IAM execution role for pipeline runtime

## Run tests

```bash
pytest -q
```
