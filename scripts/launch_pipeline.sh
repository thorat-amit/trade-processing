#!/usr/bin/env bash
# Launch the streaming pipeline on Dataflow from the Flex Template.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?}"
REGION="${REGION:-us-central1}"
ENV="${ENV:-dev}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:?}"
DATAFLOW_BUCKET="${DATAFLOW_BUCKET:?}"
SUBSCRIPTION="${SUBSCRIPTION:?projects/<p>/subscriptions/<s>}"
SNOWFLAKE_SECRET="${SNOWFLAKE_SECRET:?projects/<p>/secrets/<n>/versions/latest}"
DATAFLOW_SA="${DATAFLOW_SA:?dataflow worker SA email}"
# Must match the DAG's expected job name (default "trade-etl-streaming") so the
# Composer health check recognizes a manually-launched job and never starts a duplicate.
JOB_NAME="${JOB_NAME:-trade-etl-streaming}"
SUBNET="${SUBNET:?subnet self link}"

TEMPLATE_PATH="gs://${ARTIFACTS_BUCKET}/flex-templates/trade-etl-${ENV}.json"

gcloud dataflow flex-template run "${JOB_NAME}" \
  --template-file-gcs-location "${TEMPLATE_PATH}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --service-account-email "${DATAFLOW_SA}" \
  --subnetwork "${SUBNET}" \
  --disable-public-ips \
  --temp-location "gs://${DATAFLOW_BUCKET}/temp" \
  --staging-location "gs://${DATAFLOW_BUCKET}/staging" \
  --enable-streaming-engine \
  --max-workers 5 \
  --num-workers 1 \
  --parameters "input_subscription=${SUBSCRIPTION},snowflake_secret=${SNOWFLAKE_SECRET}"
