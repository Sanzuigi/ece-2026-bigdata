#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export AWS_PAGER=""
export S3_ENDPOINT_URL="$(aws configure get endpoint_url --profile default)"
export LAB_BUCKET_NAME="${KUBERNETES_NAMESPACE:?Missing Onyxia namespace}"
region="$(aws configure get region --profile default)"
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv

kubectl -n "$KUBERNETES_NAMESPACE" create configmap datasets \
  --from-file=users.csv --from-file=orders.csv

kubectl -n "$KUBERNETES_NAMESPACE" create configmap s3-config \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="$region" \
  --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME"

credential_file=$(mktemp)
trap 'rm -f "$credential_file"' EXIT

aws configure export-credentials --profile default --format env-no-export \
  | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' \
  > "$credential_file"

kubectl -n "$KUBERNETES_NAMESPACE" create secret generic s3-credentials \
  --from-env-file="$credential_file"

kubectl -n "$KUBERNETES_NAMESPACE" apply \
  -f infrastructure/job-upload-bronze.yaml

kubectl -n "$KUBERNETES_NAMESPACE" wait \
  --for=condition=complete job/upload-bronze --timeout=120s

kubectl -n "$KUBERNETES_NAMESPACE" logs job/upload-bronze
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
