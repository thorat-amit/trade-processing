"""Composer/Airflow DAG that supervises the trade ETL.

Each run ensures the streaming Dataflow job is up (relaunch via Flex Template if
not), runs the idempotent expiry sweep, health-checks the job, and writes audit
rows. Failures email the team; Cloud Monitoring alerts cover it too.

Config is read at task time (Jinja or Variable.get() inside callables); the few
parse-time values come from env vars, so the scheduler doesn't hit the metadata
DB on every parse.
"""
from __future__ import annotations

import datetime as dt
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "")
SNOWFLAKE_CONN_ID = os.environ.get("SNOWFLAKE_CONN_ID", "snowflake_default")
DEFAULT_JOB_NAME = "trade-etl-streaming"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [ALERT_EMAIL] if ALERT_EMAIL else [],
    "email_on_failure": bool(ALERT_EMAIL),
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
}


def _dataflow_client():
    from googleapiclient.discovery import build

    return build("dataflow", "v1b3", cache_discovery=False)


def _list_active_jobs(df, project_id, region):
    return (
        df.projects().locations().jobs()
        .list(projectId=project_id, location=region, filter="ACTIVE")
        .execute().get("jobs", [])
    )


def _is_streaming_job_running(**_):
    project_id = Variable.get("gcp_project_id")
    region = Variable.get("gcp_region", default_var="us-central1")
    job_name = Variable.get("dataflow_job_name", default_var=DEFAULT_JOB_NAME)

    for job in _list_active_jobs(_dataflow_client(), project_id, region):
        if job.get("name") == job_name and job.get("currentState") in (
            "JOB_STATE_RUNNING", "JOB_STATE_PENDING",
        ):
            return "streaming_job_already_running"
    return "start_streaming_job"


def _health_check(**context):
    project_id = Variable.get("gcp_project_id")
    region = Variable.get("gcp_region", default_var="us-central1")
    job_name = Variable.get("dataflow_job_name", default_var=DEFAULT_JOB_NAME)

    running = [
        j for j in _list_active_jobs(_dataflow_client(), project_id, region)
        if j.get("name") == job_name and j.get("currentState") == "JOB_STATE_RUNNING"
    ]
    if not running:
        raise RuntimeError(f"streaming job '{job_name}' is not RUNNING")
    context["ti"].log.info("health check ok: %s is RUNNING", job_name)


with DAG(
    dag_id="trade_etl_orchestration",
    description="Supervise streaming trade ETL, run expiry sweep, monitor health",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",  # hourly: keep the streaming job supervised + statuses fresh
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["trade", "etl", "streaming", "snowflake"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_running = BranchPythonOperator(
        task_id="check_streaming_job",
        python_callable=_is_streaming_job_running,
    )

    already_running = EmptyOperator(task_id="streaming_job_already_running")

    # body/location/project_id are templated, so the worker renders the Variables
    # at runtime. Worker runs as the least-privilege SA in the private subnet.
    start_streaming = DataflowStartFlexTemplateOperator(
        task_id="start_streaming_job",
        project_id="{{ var.value.gcp_project_id }}",
        location="{{ var.value.get('gcp_region', 'us-central1') }}",
        body={
            "launchParameter": {
                "jobName": "{{ var.value.get('dataflow_job_name', '" + DEFAULT_JOB_NAME + "') }}",
                "containerSpecGcsPath": "{{ var.value.dataflow_flex_template_gcs_path }}",
                "parameters": {
                    "input_subscription": "{{ var.value.pubsub_input_subscription }}",
                    "snowflake_secret": "{{ var.value.snowflake_secret_resource }}",
                },
                "environment": {
                    "tempLocation": "{{ var.value.dataflow_temp_location }}",
                    "maxWorkers": 5,
                    "numWorkers": 1,
                    "autoscalingAlgorithm": "THROUGHPUT_BASED",
                    "enableStreamingEngine": True,
                    "serviceAccountEmail": "{{ var.value.dataflow_sa_email }}",
                    "subnetwork": "{{ var.value.dataflow_subnet }}",
                    "ipConfiguration": "WORKER_IP_PRIVATE",
                },
            }
        },
    )

    join_after_launch = EmptyOperator(
        task_id="join_after_launch",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Rule 4 (expiry). Idempotent - only flips already-matured ACTIVE rows -
    # so running it on every hourly cycle is safe and keeps statuses fresh.
    expiry_sweep = SQLExecuteQueryOperator(
        task_id="expiry_sweep",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="sql/expiry_sweep.sql",
    )

    audit_expiry = SQLExecuteQueryOperator(
        task_id="audit_expiry",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            INSERT INTO PIPELINE_AUDIT (run_id, task_id, event, detail)
            VALUES ('{{ run_id }}', 'expiry_sweep', 'EXPIRY_SWEEP',
                    'expiry sweep executed');
        """,
    )

    health_check = PythonOperator(task_id="health_check", python_callable=_health_check)

    audit_health = SQLExecuteQueryOperator(
        task_id="audit_health",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            INSERT INTO PIPELINE_AUDIT (run_id, task_id, event, detail)
            VALUES ('{{ run_id }}', 'health_check', 'HEALTH_CHECK',
                    'streaming job verified RUNNING');
        """,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> check_running >> [start_streaming, already_running]
    start_streaming >> join_after_launch
    already_running >> join_after_launch
    join_after_launch >> expiry_sweep >> audit_expiry >> health_check >> audit_health >> end
