# Real-Time Ecommerce Data Pipeline on AWS

A production-grade, event-driven ecommerce data pipeline demonstrating real-time ingestion, serverless processing, a three-zone data lake, fraud detection, and ML-powered recommendations — all deployed with AWS CDK.

---

## Architecture

```
                          ┌─────────────────────────────────────────────────────┐
                          │                  Data Sources                        │
                          │  Orders  │  Clickstream  │  Inventory Updates        │
                          └────┬─────┴──────┬────────┴──────┬────────────────────┘
                               │            │               │
                    ┌──────────▼────────────▼───────────────▼──────────┐
                    │           Kinesis Data Streams                    │
                    │   ecom-orders  │  ecom-clickstream  │  ecom-inv  │
                    └──┬────────────┬──────────────────────┬───────────┘
                       │            │                      │
           ┌───────────▼──┐  ┌──────▼──────┐    ┌────────▼────────┐
           │ Order         │  │ Stream       │    │ Inventory       │
           │ Processor λ  │  │ Enricher λ   │    │ Alerter λ       │
           └──┬────────┬──┘  └──────┬───────┘    └────────┬────────┘
              │        │            │                      │
    ┌─────────▼──┐  ┌──▼───────┐  ┌▼──────────────┐  ┌───▼──────────┐
    │ DynamoDB   │  │ SNS/SQS  │  │ S3 Processed  │  │ SNS Low Stock│
    │ Orders     │  │ Fraud +  │  │ (Parquet)     │  │ Topic        │
    │ Sessions   │  │ Orders   │  └──────┬────────┘  └──────────────┘
    │ Products   │  └──┬───────┘         │
    └────────────┘     │           ┌─────▼──────────────┐
                  ┌────▼───────┐   │   AWS Glue ETL     │
                  │ Fraud      │   │   raw→processed    │
                  │ Detector λ │   │   →curated         │
                  └────────────┘   └──────┬─────────────┘
                                          │
                              ┌───────────▼──────────────┐
                              │   S3 Curated (Parquet)   │
                              │  daily_kpis / clv /      │
                              │  product_interactions    │
                              └───────────┬──────────────┘
                                          │
                              ┌───────────▼──────────────┐
                              │  Amazon Athena           │
                              │  SQL Analytics           │
                              └───────────┬──────────────┘
                                          │
                              ┌───────────▼──────────────┐
                              │  CloudWatch Dashboard    │
                              │  + Alarms                │
                              └──────────────────────────┘

              ┌──────────────────────────────────────────┐
              │  SageMaker ALS Recommendation Endpoint   │
              │  POST /recommendations → API Gateway λ  │
              └──────────────────────────────────────────┘
```

---

## Features

| Layer | What it does |
|---|---|
| **Kinesis Streams** | 3 streams: orders (2 shards), clickstream (4 shards), inventory (1 shard) |
| **Lambda Processors** | Order persist + fraud scoring, clickstream enrichment + S3 flush, inventory alerting |
| **S3 Data Lake** | Raw (GZIP JSON) → Processed (Parquet, partitioned) → Curated (aggregates) |
| **Glue ETL** | Hourly crawler + daily job: dedup, clean, CLV, product interaction matrix |
| **Athena** | SQL workgroup with saved queries: revenue KPIs, cohort retention, fraud analysis |
| **DynamoDB** | Orders (with customer GSI), Products, Sessions (TTL) — all PAY_PER_REQUEST |
| **Fraud Detection** | Rule-based scoring in Lambda + velocity checks (orders/hour, daily spend) |
| **SNS/SQS** | Fraud alerts, order events, low-inventory notifications with DLQs |
| **SageMaker** | ALS collaborative filtering model, real-time inference endpoint |
| **API Gateway** | REST API: `POST /recommendations` with throttling + CloudWatch logging |
| **CloudWatch** | Dashboard with 4 widgets, alarms on Lambda errors + Kinesis iterator age |
| **CDK (Python)** | 6 stacks, all IaC, deployable with one command |

---

## Project Structure

```
ecommerce-aws-pipeline/
├── infrastructure/          # CDK stacks
│   ├── app.py
│   └── lib/
│       ├── storage_stack.py      # S3 + DynamoDB
│       ├── ingestion_stack.py    # Kinesis + Firehose
│       ├── processing_stack.py   # Lambda + Glue + Athena
│       ├── ml_stack.py           # SageMaker + API Gateway
│       ├── notification_stack.py # SNS + SQS
│       └── monitoring_stack.py   # CloudWatch dashboard + alarms
├── lambdas/
│   ├── order_processor/     # ORDER_PLACED → DynamoDB + fraud scoring
│   ├── stream_enricher/     # Clickstream → S3 processed
│   ├── fraud_detector/      # Velocity fraud analysis (SQS trigger)
│   ├── inventory_alerter/   # Low-stock SNS alerts
│   └── recommendation_api/  # SageMaker inference + API Gateway
│       ├── handler.py       # Lambda handler
│       ├── inference.py     # SageMaker serving script
│       └── train.py         # ALS model training
├── glue_jobs/
│   ├── raw_to_processed.py  # Spark ETL: raw → processed → curated
│   └── athena_queries.sql   # Analytics query library
├── data_generator/
│   └── generator.py         # Realistic ecommerce event simulator
├── tests/
│   └── test_order_processor.py
├── scripts/
│   ├── deploy.sh
│   ├── run_generator.sh
│   └── train_model.sh
└── requirements.txt
```

---

## Quick Start

### Prerequisites
- AWS CLI configured (`aws configure`)
- Python 3.11+
- Node.js 18+ (for CDK)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Deploy infrastructure
```bash
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export REGION=us-east-1
./scripts/deploy.sh
```

### 3. Start the event generator
```bash
# 2 orders/sec (default)
./scripts/run_generator.sh

# Custom rate
RATE=5.0 ./scripts/run_generator.sh

# Run for 60 seconds then stop
DURATION=60 ./scripts/run_generator.sh
```

### 4. Watch events flow
- **CloudWatch Dashboard**: AWS Console → CloudWatch → Dashboards → `EcommerceDataPipeline`
- **DynamoDB**: Console → DynamoDB → Tables → `OrdersTable`
- **S3**: Console → S3 → your processed bucket → `clickstream/`

### 5. Train the recommendation model
```bash
# After enough orders have flowed through Glue ETL
./scripts/train_model.sh
```

### 6. Call the recommendations API
```bash
API_URL=$(jq -r '.EcomMLStack.RecommendationApiUrl' cdk-outputs.json)
curl -X POST "$API_URL/recommendations" \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "CUST-000001", "n": 5}'
```

---

## Key Design Decisions

**Why Kinesis over SQS for ingestion?** Kinesis preserves ordering within a shard, enables replay, and supports multiple independent consumers (Lambda + Firehose simultaneously).

**Why a three-zone data lake?** Raw zone preserves the source of truth. Processed zone (Parquet, partitioned) cuts Athena scan costs by 90%+. Curated zone serves pre-aggregated KPIs and ML training data.

**Why ALS for recommendations?** Implicit feedback (purchases, views) is the natural signal in ecommerce. ALS handles sparse matrices efficiently and scales to millions of items.

**Fraud detection is two-stage:** Fast rule-based scoring in the order processor (sub-ms, synchronous) catches obvious cases. Async velocity checks in a separate Lambda allow deeper analysis without blocking order flow.

---

## Running Tests

```bash
pytest tests/ -v
```

Tests use `moto` to mock AWS — no real AWS account needed for unit tests.

---

## Cost Estimate (light load)

| Service | Est. monthly cost |
|---|---|
| Kinesis (3 streams × 2 shards avg) | ~$5 |
| Lambda (1M invocations) | ~$2 |
| S3 (50 GB) | ~$1.15 |
| DynamoDB (PAY_PER_REQUEST, light) | ~$3 |
| Glue (2 DPU-hours/day) | ~$9 |
| SageMaker ml.t2.medium endpoint | ~$50 |
| API Gateway (1M requests) | ~$3.50 |
| **Total** | **~$75/month** |

> Tear down the SageMaker endpoint when not demoing to avoid the largest cost.
