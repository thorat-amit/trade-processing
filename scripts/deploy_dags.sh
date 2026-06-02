#!/usr/bin/env bash
# Upload the DAG + SQL to the Composer DAGs bucket and set required Airflow Variables.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?}"
REGION="${REGION:-us-central1}"
ENV="${ENV:-dev}"
COMPOSER_ENV="${COMPOSER_ENV:-trade-etl-composer-${ENV}}"
DAG_GCS_PREFIX="${DAG_GCS_PREFIX:?from terraform output dag_gcs_prefix}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo ">> Uploading DAG + SQL..."
gsutil cp "${ROOT}/orchestration/dags/trade_etl_dag.py" "${DAG_GCS_PREFIX}/"
gsutil -m cp -r "${ROOT}/orchestration/dags/sql" "${DAG_GCS_PREFIX}/"

echo ">> Setting Airflow Variables (edit values as needed)..."
set_var () {
  gcloud composer environments run "${COMPOSER_ENV}" --location "${REGION}" \
    variables set -- "$1" "$2"
}
# These Variables are consumed by the DAG at task-execution time (Jinja
# {{ var.value.X }} in operator fields, or Variable.get() inside callables).
set_var gcp_project_id "${PROJECT_ID}"
set_var gcp_region "${REGION}"
set_var dataflow_flex_template_gcs_path "${FLEX_TEMPLATE_PATH:?}"
set_var dataflow_temp_location "${DATAFLOW_TEMP_LOCATION:?}"
set_var pubsub_input_subscription "${SUBSCRIPTION:?}"
set_var snowflake_secret_resource "${SNOWFLAKE_SECRET:?}"
set_var dataflow_sa_email "${DATAFLOW_SA:?}"
set_var dataflow_subnet "${SUBNET:?}"
# Optional override; the DAG defaults this to "trade-etl-streaming".
if [[ -n "${DATAFLOW_JOB_NAME:-}" ]]; then set_var dataflow_job_name "${DATAFLOW_JOB_NAME}"; fi

# NOTE: ALERT_EMAIL and SNOWFLAKE_CONN_ID are consumed at DAG *parse* time, so
# they are provided as Composer ENVIRONMENT VARIABLES (set by Terraform's
# composer module: config.software_config.env_variables), NOT as Airflow
# Variables. To change them after deploy:
#   gcloud composer environments update "${COMPOSER_ENV}" --location "${REGION}" \
#     --update-env-variables ALERT_EMAIL=you@example.com

echo ">> Done. Set up the 'snowflake_default' Airflow Connection in the UI or via CLI."
