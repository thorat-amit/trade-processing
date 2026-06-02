#!/usr/bin/env bash
# Build and publish the Dataflow Flex Template for the trade ETL pipeline.
# Prereqs: gcloud authenticated, APIs enabled (Terraform does this), Artifact Registry repo exists.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-us-central1}"
ENV="${ENV:-dev}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:?set ARTIFACTS_BUCKET (from terraform output artifacts_bucket)}"
REPO="${AR_REPO:-trade-etl}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/trade-etl-pipeline:${IMAGE_TAG}"
TEMPLATE_PATH="gs://${ARTIFACTS_BUCKET}/flex-templates/trade-etl-${ENV}.json"

echo ">> Ensuring Artifact Registry repo exists..."
gcloud artifacts repositories describe "${REPO}" --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "${REPO}" --repository-format=docker \
    --location="${REGION}" --project="${PROJECT_ID}" --description="Trade ETL images"

echo ">> Building Flex Template image ${IMAGE_URI} ..."
cd "$(dirname "$0")/../src/pipeline"

gcloud dataflow flex-template build "${TEMPLATE_PATH}" \
  --image-gcr-path "${IMAGE_URI}" \
  --sdk-language "PYTHON" \
  --flex-template-base-image "PYTHON3" \
  --metadata-file "metadata.json" \
  --py-path "." \
  --env "FLEX_TEMPLATE_PYTHON_PY_FILE=pipeline.py" \
  --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt" \
  --env "FLEX_TEMPLATE_PYTHON_SETUP_FILE=setup.py" \
  --project "${PROJECT_ID}"

echo ">> Flex Template published to: ${TEMPLATE_PATH}"
