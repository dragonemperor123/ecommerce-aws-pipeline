#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# deploy.sh — Bootstrap and deploy the full ecommerce pipeline
#
# Usage:
#   ./scripts/deploy.sh [options]
#
# Options (all have defaults):
#   --account 123456789      AWS account ID (default: auto-detected)
#   --region  us-east-1      AWS region     (default: us-east-1)
#   --dataset-path D:/...    Path to Olist CSV files for model training
#                            (default: D:/Dataset_AWS)
#   --skip-training          Skip recommendation model training step
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REGION="${REGION:-eu-north-1}"
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
DATASET_PATH="${DATASET_PATH:-D:/Dataset_AWS}"
SKIP_TRAINING="${SKIP_TRAINING:-false}"

# Parse CLI flags
while [[ $# -gt 0 ]]; do
    case $1 in
        --account)       ACCOUNT="$2";       shift 2 ;;
        --region)        REGION="$2";        shift 2 ;;
        --dataset-path)  DATASET_PATH="$2";  shift 2 ;;
        --skip-training) SKIP_TRAINING=true; shift   ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "==> Deploying to account=$ACCOUNT region=$REGION"

# ── 1. Install Python dependencies (inside virtualenv) ───────────────────────
echo "==> Installing Python dependencies..."
if [[ ! -d ".venv" ]]; then
    python3 -m venv .venv
fi
source .venv/bin/activate
pip install -r requirements.txt -q

# ── 2. CDK bootstrap + deploy ────────────────────────────────────────────────
echo "==> Installing AWS CDK..."
sudo npm install -g aws-cdk

echo "==> Bootstrapping and deploying CDK stacks..."
cd infrastructure

cdk bootstrap "aws://$ACCOUNT/$REGION" \
    --context account="$ACCOUNT" \
    --context region="$REGION"

cdk deploy --all \
    --require-approval never \
    --context account="$ACCOUNT" \
    --context region="$REGION" \
    --outputs-file ../cdk-outputs.json

cd ..
echo "==> CDK deployment complete. Outputs written to cdk-outputs.json"

# ── 3. Read bucket names from stack outputs ───────────────────────────────────
PROCESSED_BUCKET=$(jq -r '.EcomStorageStack.ProcessedBucketName // empty' cdk-outputs.json)
MODEL_BUCKET=$(jq -r '.EcomStorageStack.ModelBucketName // empty' cdk-outputs.json)

# ── 4. Upload Glue ETL script ─────────────────────────────────────────────────
if [[ -n "$PROCESSED_BUCKET" ]]; then
    echo "==> Uploading Glue script to s3://$PROCESSED_BUCKET/glue-scripts/"
    aws s3 cp glue_jobs/raw_to_processed.py \
        "s3://$PROCESSED_BUCKET/glue-scripts/raw_to_processed.py"
fi

# ── 5. Seed DynamoDB products table from Olist catalog ───────────────────────
PRODUCTS_TABLE=$(jq -r '.EcomStorageStack.ProductsTableName // empty' cdk-outputs.json)

if [[ -n "$PRODUCTS_TABLE" ]]; then
    echo "==> Seeding products table '$PRODUCTS_TABLE' from Olist catalog ..."
    python scripts/seed_products.py \
        --table-name    "$PRODUCTS_TABLE" \
        --dataset-path  "$DATASET_PATH" \
        --region        "$REGION"
    echo "==> Products seeded."
fi

# ── 6. Train recommendation model from Olist dataset ─────────────────────────
if [[ "$SKIP_TRAINING" == "false" && -n "$MODEL_BUCKET" ]]; then
    echo "==> Training recommendation model from Olist dataset at $DATASET_PATH ..."
    python lambdas/recommendation_api/train.py \
        --dataset-path  "$DATASET_PATH" \
        --output-s3     "s3://$MODEL_BUCKET/recommendation" \
        --factors       64 \
        --iterations    20
    echo "==> Model trained and uploaded to s3://$MODEL_BUCKET/recommendation/"
else
    echo "==> Skipping model training (SKIP_TRAINING=$SKIP_TRAINING)"
fi

echo "==> Done!"
