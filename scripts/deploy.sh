#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# deploy.sh — Bootstrap and deploy the full ecommerce pipeline
# Usage: ./scripts/deploy.sh [--account 123456789] [--region us-east-1]
# ──────────────────────────────────────────────────────────────
set -euo pipefail

REGION="${REGION:-us-east-1}"
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"

echo "==> Deploying to account=$ACCOUNT region=$REGION"

# Install Python deps
pip install -r requirements.txt -q

# Bootstrap CDK (idempotent)
cd infrastructure
npm install -g aws-cdk 2>/dev/null || true
cdk bootstrap "aws://$ACCOUNT/$REGION" \
    --context account="$ACCOUNT" \
    --context region="$REGION"

# Deploy all stacks in dependency order
cdk deploy --all \
    --require-approval never \
    --context account="$ACCOUNT" \
    --context region="$REGION" \
    --outputs-file ../cdk-outputs.json

cd ..
echo "==> Deployment complete. Outputs written to cdk-outputs.json"

# Upload Glue script
RAW_BUCKET=$(jq -r '.EcomStorageStack.RawBucketName // empty' cdk-outputs.json)
PROCESSED_BUCKET=$(jq -r '.EcomStorageStack.ProcessedBucketName // empty' cdk-outputs.json)

if [[ -n "$PROCESSED_BUCKET" ]]; then
    aws s3 cp glue_jobs/raw_to_processed.py \
        "s3://$PROCESSED_BUCKET/glue-scripts/raw_to_processed.py"
    echo "==> Glue script uploaded to s3://$PROCESSED_BUCKET/glue-scripts/"
fi

echo "==> Done!"
