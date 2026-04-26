"""
Order Processor Lambda
- Consumes ORDER_PLACED events from Kinesis
- Persists to DynamoDB
- Runs fraud scoring
- Publishes to SNS order-events topic
"""
import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

ORDERS_TABLE = os.environ["ORDERS_TABLE"]
FRAUD_TOPIC_ARN = os.environ["FRAUD_TOPIC_ARN"]
ORDER_EVENTS_TOPIC_ARN = os.environ["ORDER_EVENTS_TOPIC_ARN"]

orders_table = dynamodb.Table(ORDERS_TABLE)

# Simple rule-based fraud thresholds (SageMaker model handles deep scoring).
# Calibrated for Olist/Brazilian e-commerce data:
# - bank_transfer = boleto, a standard Brazilian payment — NOT suspicious
# - high_value threshold at 95th percentile of Olist order values (~R$500)
# - bronze threshold at ~75th percentile for low-tier customers
FRAUD_RULES = {
    "high_value_threshold": 500,
    "suspicious_payment": {"crypto"},
    "bronze_threshold": 200,
}


def score_fraud(order: dict) -> dict:
    flags = []
    score = 0.0

    if order.get("is_suspicious"):
        flags.append("known_risky_customer")
        score += 0.4

    if order["total"] > FRAUD_RULES["high_value_threshold"]:
        flags.append("high_value_order")
        score += 0.2

    if order.get("payment_method") in FRAUD_RULES["suspicious_payment"]:
        flags.append("unusual_payment_method")
        score += 0.15

    if order["customer_tier"] == "bronze" and order["total"] > FRAUD_RULES["bronze_threshold"]:
        flags.append("tier_amount_mismatch")
        score += 0.25

    # Multiple high-value items
    if len(order.get("items", [])) > 3 and order["total"] > 500:
        flags.append("bulk_purchase")
        score += 0.1

    return {"score": min(score, 1.0), "flags": flags, "is_fraud": score >= 0.5}


def process_order(order: dict) -> None:
    fraud = score_fraud(order)
    order["fraud_score"] = fraud["score"]
    order["fraud_flags"] = fraud["flags"]
    order["processed_at"] = datetime.now(timezone.utc).isoformat()

    # Write to DynamoDB
    try:
        orders_table.put_item(
            Item={
                "order_id": order["order_id"],
                "customer_id": order["customer_id"],
                "status": order["status"],
                "total": str(order["total"]),
                "fraud_score": str(round(fraud["score"], 4)),
                "fraud_flags": fraud["flags"],
                "payment_method": order["payment_method"],
                "item_count": len(order.get("items", [])),
                "created_at": order["created_at"],
                "processed_at": order["processed_at"],
                "ttl": int(time.time()) + 90 * 86400,  # 90-day TTL
            }
        )
    except ClientError as e:
        log.error("DynamoDB write failed for order %s: %s", order["order_id"], e)
        raise

    # Publish fraud alert if triggered
    if fraud["is_fraud"]:
        try:
            sns.publish(
                TopicArn=FRAUD_TOPIC_ARN,
                Message=json.dumps({
                    "order_id": order["order_id"],
                    "customer_id": order["customer_id"],
                    "total": order["total"],
                    "fraud_score": fraud["score"],
                    "fraud_flags": fraud["flags"],
                    "timestamp": order["processed_at"],
                }),
                Subject="Fraud Alert",
                MessageAttributes={
                    "event_type": {"DataType": "String", "StringValue": "FRAUD_ALERT"},
                },
            )
            log.warning("FRAUD ALERT published for order %s (score=%.2f)", order["order_id"], fraud["score"])
        except ClientError as e:
            log.error("Failed to publish fraud alert: %s", e)

    # Publish order event
    try:
        sns.publish(
            TopicArn=ORDER_EVENTS_TOPIC_ARN,
            Message=json.dumps({
                "order_id": order["order_id"],
                "customer_id": order["customer_id"],
                "status": order["status"],
                "total": order["total"],
                "timestamp": order["processed_at"],
            }),
            MessageAttributes={
                "event_type": {"DataType": "String", "StringValue": "ORDER_PLACED"},
                "status": {"DataType": "String", "StringValue": order["status"]},
            },
        )
    except ClientError as e:
        log.error("Failed to publish order event: %s", e)


def lambda_handler(event, context):
    records = event.get("Records", [])
    success = 0
    failed_item_ids = []

    for record in records:
        try:
            payload = json.loads(record["body"])
            process_order(payload)
            success += 1
        except Exception as e:
            log.error("Failed to process record %s: %s", record["messageId"], e)
            failed_item_ids.append({"itemIdentifier": record["messageId"]})

    log.info("Processed %d/%d order records", success, len(records))

    # Return failed items for retry (bisect_batch_on_error handles splitting)
    if failed_item_ids:
        return {"batchItemFailures": failed_item_ids}
