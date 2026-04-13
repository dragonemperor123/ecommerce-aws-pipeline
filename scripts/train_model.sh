#!/usr/bin/env bash
# Train the recommendation model and upload to S3
set -euo pipefail

REGION="${REGION:-us-east-1}"
CURATED_BUCKET=$(jq -r '.EcomStorageStack.CuratedBucketName // empty' cdk-outputs.json 2>/dev/null || echo "")
MODEL_BUCKET=$(jq -r '.EcomStorageStack.ModelBucketName // empty' cdk-outputs.json 2>/dev/null || echo "")

if [[ -z "$CURATED_BUCKET" || -z "$MODEL_BUCKET" ]]; then
    echo "ERROR: Run deploy.sh first to generate cdk-outputs.json"
    exit 1
fi

echo "==> Training recommendation model"
python lambdas/recommendation_api/train.py \
    --interactions-path "s3://$CURATED_BUCKET/product_interactions/" \
    --output-s3 "s3://$MODEL_BUCKET/recommendation" \
    --factors 64 \
    --iterations 25

echo "==> Model training complete"
