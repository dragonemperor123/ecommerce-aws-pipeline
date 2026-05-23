#!/usr/bin/env bash
# Start the ecommerce event generator
set -euo pipefail

REGION="${REGION:-us-east-1}"
RATE="${RATE:-2.0}"
DURATION="${DURATION:-}"
DATASET_PATH="${DATASET_PATH:-D:/Dataset_AWS}"

DURATION_ARG=""
if [[ -n "$DURATION" ]]; then
    DURATION_ARG="--duration $DURATION"
fi

echo "==> Starting event generator at $RATE orders/sec (region=$REGION, dataset=$DATASET_PATH)"
python data_generator/generator.py \
    --region "$REGION" \
    --rate "$RATE" \
    --dataset-path "$DATASET_PATH" \
    $DURATION_ARG
