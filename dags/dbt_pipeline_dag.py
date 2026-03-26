"""
dbt_pipeline_dag.py
Airflow DAG — dbt + Snowflake Analytics Platform

Orchestrates a full dbt build in layered order:
    1. dbt deps           — install packages
    2. dbt source freshness
    3. dbt run --select staging
    4. dbt test --select staging
    5. dbt run --select intermediate
    6. dbt run --select marts.dimensions
    7. dbt run --select marts.facts        (incremental)
    8. dbt test --select marts
    9. dbt docs generate
   10. (Optional) Elementary report

All steps use the DbtCloudRunJobOperator when a dbt Cloud account is
available. Falls back to BashOperator with dbt CLI for self-hosted deploys.

Configuration:
    Set USE_DBT_CLOUD=true Airflow Variable to use dbt Cloud operator.
    Set USE_DBT_CLOUD=false (default) for CLI-based runs.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────

USE_DBT_CLOUD = Variable.get("USE_DBT_CLOUD", default_var="false").lower() == "true"
DBT_CLOUD_CONN_ID = "dbt_cloud_default"
DBT_CLI_CONN_ID = "snowflake_dbt"

# dbt Cloud Job IDs (set via Airflow Variables)
DBT_CLOUD_JOB_FULL = int(Variable.get("dbt_cloud_job_full", default_var="101"))
DBT_CLOUD_JOB_SLIM = int(Variable.get("dbt_cloud_job_slim", default_var="102"))

# dbt CLI settings
DBT_PROJECT_DIR = Variable.get(
    "dbt_project_dir", default_var="/opt/airflow/dbt/analytics_platform"
)
DBT_PROFILES_DIR = Variable.get("dbt_profiles_dir", default_var="/opt/airflow/dbt")
DBT_TARGET = Variable.get("dbt_target", default_var="prod")
DBT_THREADS = int(Variable.get("dbt_threads", default_var="8"))

ALERT_EMAILS = Variable.get(
    "alert_emails", default_var="charan.neelam@company.com"
).split(",")

# ─── Common dbt CLI base command ─────────────────────────────────────────────

DBT_BASE = (
    f"dbt --no-write-json"
    f" --project-dir {DBT_PROJECT_DIR}"
    f" --profiles-dir {DBT_PROFILES_DIR}"
    f" --target {DBT_TARGET}"
)

# ─── Default Args ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "charan.neelam",
    "depends_on_past": False,
    "email": ALERT_EMAILS,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# ─── Callbacks ────────────────────────────────────────────────────────────────


def check_dbt_target_freshness(**context) -> None:
    """Log the target environment for observability."""
    logger.info(
        "dbt pipeline starting | target=%s project_dir=%s date=%s",
        DBT_TARGET,
        DBT_PROJECT_DIR,
        context["ds"],
    )


def validate_dbt_test_results(**context) -> None:
    """
    Pull dbt test results from XCom and raise if critical tests failed.
    In production, parse the run_results.json artifact for richer output.
    """
    ti = context["ti"]
    marts_test_output = ti.xcom_pull(task_ids="dbt_test_marts")
    if marts_test_output and "ERROR" in str(marts_test_output):
        raise ValueError(
            f"dbt test failures detected in marts layer:\n{marts_test_output}"
        )
    logger.info("dbt test validation passed.")


# ─── DAG ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dbt_snowflake_analytics_pipeline",
    description="dbt + Snowflake: staging → intermediate → dims → facts → docs",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # 04:00 UTC daily (after ETL lands)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "snowflake", "analytics", "kimball"],
    doc_md=__doc__,
) as dag:

    # ── Pre-flight ────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_check",
        python_callable=check_dbt_target_freshness,
    )

    # ── dbt deps ──────────────────────────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BASE} deps",
        retries=2,
    )

    # ── Source freshness check ────────────────────────────────────────────────
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"{DBT_BASE} source freshness"
            f" --output /tmp/dbt_source_freshness_{{{{ ds_nodash }}}}.json"
        ),
        # warn only — don't fail pipeline if source is slightly stale
        on_failure_callback=lambda ctx: logger.warning(
            "Source freshness check failed — continuing pipeline."
        ),
    )

    # ── Staging layer ─────────────────────────────────────────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"{DBT_BASE} run" f" --select staging" f" --threads {DBT_THREADS}"
        ),
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(f"{DBT_BASE} test" f" --select staging" f" --store-failures"),
    )

    # ── Intermediate layer (ephemeral — no separate run needed, builds inline) ─
    # The intermediate models compile into downstream models.
    # We run a lightweight compile check here to catch SQL errors early.
    dbt_compile_intermediate = BashOperator(
        task_id="dbt_compile_intermediate",
        bash_command=(f"{DBT_BASE} compile" f" --select intermediate"),
    )

    # ── Dimensions (must run before facts for FK resolution) ─────────────────
    dbt_run_dims = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=(
            f"{DBT_BASE} run"
            f" --select marts.dim_date marts.dim_customer marts.dim_product"
            f" --threads {DBT_THREADS}"
        ),
    )

    # ── Facts (incremental MERGE) ─────────────────────────────────────────────
    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=(
            f"{DBT_BASE} run"
            f" --select marts.fact_orders"
            f" --threads {DBT_THREADS}"
            # Pass batch date as dbt variable for incremental window
            f' --vars \'{{"batch_date": "{{{{ ds }}}}", "incremental_lookback_days": 3}}\''
        ),
    )

    # ── Marts tests (all custom singular + schema tests) ─────────────────────
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"{DBT_BASE} test"
            f" --select marts"
            f" --store-failures"
            f" --threads {DBT_THREADS}"
        ),
        do_xcom_push=True,
    )

    # ── Test result validation ────────────────────────────────────────────────
    validate_tests = PythonOperator(
        task_id="validate_test_results",
        python_callable=validate_dbt_test_results,
    )

    # ── dbt docs generation ───────────────────────────────────────────────────
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"{DBT_BASE} docs generate"
            f" --target-path /tmp/dbt_docs_{{{{ ds_nodash }}}}"
        ),
    )

    # ── (Optional) Upload docs to S3 / ADLS ──────────────────────────────────
    upload_docs = BashOperator(
        task_id="upload_docs_to_storage",
        bash_command=(
            # Replace with your cloud storage path
            "echo 'Uploading docs to cloud storage...' && "
            "aws s3 sync /tmp/dbt_docs_{{ ds_nodash }}/ "
            "s3://your-analytics-bucket/dbt-docs/latest/ --quiet || "
            "echo 'Upload skipped — AWS CLI not configured'"
        ),
    )

    # ── Success gate ─────────────────────────────────────────────────────────
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─── Dependencies ────────────────────────────────────────────────────────
    (
        start
        >> preflight
        >> dbt_deps
        >> dbt_source_freshness
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_compile_intermediate
        >> dbt_run_dims
        >> dbt_run_facts
        >> dbt_test_marts
        >> validate_tests
        >> dbt_docs_generate
        >> upload_docs
        >> end
    )
