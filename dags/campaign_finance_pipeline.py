from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "climate-cabinet",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="campaign_finance_medallion_pipeline",
    default_args=DEFAULT_ARGS,
    description="State campaign finance bronze/silver/gold pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["climate-cabinet", "finance", "medallion"],
) as dag:
    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            "climate-cabinet-pipeline --stage bronze "
            "--source-dir data/sources --bronze-dir data/bronze "
            "--ingestion-mode ${BRONZE_INGESTION_MODE:-file} "
            "--openfec-pages ${OPENFEC_PAGES:-1} "
            "--openfec-per-page ${OPENFEC_PER_PAGE:-100} "
            "--openfec-cycle ${OPENFEC_CYCLE:-2026} "
            "${OPENFEC_STATE:+--openfec-state $OPENFEC_STATE} "
            "${STATE_CSV_URL:+--state-csv-url $STATE_CSV_URL} "
            "--persist-api-extracts"
        ),
    )

    silver = BashOperator(
        task_id="silver_transformation",
        bash_command="climate-cabinet-pipeline --stage silver --bronze-dir data/bronze --silver-dir data/silver",
    )

    scd2 = BashOperator(
        task_id="scd2_relationship_history",
        bash_command="climate-cabinet-pipeline --stage scd2 --silver-dir data/silver",
    )

    gold = BashOperator(
        task_id="gold_aggregations",
        bash_command="climate-cabinet-pipeline --stage gold --silver-dir data/silver --gold-dir data/gold",
    )

    bronze >> silver >> scd2 >> gold
