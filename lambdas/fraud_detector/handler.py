"""
Fraud Detector Lambda
- Triggered by SQS (subscribed to fraud SNS topic)
- Performs secondary deep-scoring using velocity checks
- Updates order record with final fraud decision
- Logs for audit trail
"""
import json
import logging
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

ORDERS_TABLE = os.environ["ORDERS_TABLE"]
FRAUD_TOPIC_ARN = os.environ["FRAUD_TOPIC_ARN"]

orders_table = dynamodb.Table(ORDERS_TABLE)

# Velocity thresholds — calibrated against Olist dataset distributions.
# Olist median order value ~R$120; 95th percentile ~R$500; top 1% >R$1000.
# Brazilian customers rarely place more than 1-2 orders per day.
MAX_ORDERS_PER_HOUR = 3
MAX_SPEND_PER_DAY   = 1500


def get_recent_orders(customer_id: str, hours: int = 1) -> list:
    """Query recent orders for a customer via GSI."""
    try:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        response = orders_table.query(
            IndexName="customer-index",
            KeyConditionExpression=Key("customer_id").eq(customer_id) & Key("created_at").gte(cutoff),
        )
        return response.get("Items", [])
    except ClientError as e:
        log.warning("Could not fetch recent orders for %s: %s", customer_id, e)
        return []


def velocity_check(customer_id: str, current_total: float) -> dict:
    recent = get_recent_orders(customer_id, hours=1)
    order_count = len(recent)
    spend_24h = sum(float(o.get("total", 0)) for o in get_recent_orders(customer_id, hours=24))

    flags = []
    velocity_score = 0.0

    if order_count >= MAX_ORDERS_PER_HOUR:
        flags.append(f"velocity_orders_{order_count}_in_1h")
        velocity_score += 0.4

    if spend_24h + current_total > MAX_SPEND_PER_DAY:
        flags.append(f"spend_limit_exceeded_{spend_24h + current_total:.0f}")
        velocity_score += 0.35

    return {"velocity_score": min(velocity_score, 1.0), "velocity_flags": flags}


def lambda_handler(event, context):
    for record in event.get("Records", []):
        try:
            # SQS message body contains the SNS notification
            body = json.loads(record["body"])
            alert = json.loads(body.get("Message", body) if isinstance(body, dict) else body)

            order_id = alert["order_id"]
            customer_id = alert["customer_id"]
            total = float(alert["total"])
            existing_score = float(alert.get("fraud_score", 0))

            velocity = velocity_check(customer_id, total)
            combined_score = min(existing_score + velocity["velocity_score"], 1.0)
            all_flags = alert.get("fraud_flags", []) + velocity["velocity_flags"]

            decision = "BLOCK" if combined_score >= 0.7 else "REVIEW" if combined_score >= 0.5 else "PASS"

            # Update order with final fraud decision
            try:
                orders_table.update_item(
                    Key={"order_id": order_id, "customer_id": customer_id},
                    UpdateExpression=(
                        "SET fraud_decision = :d, combined_fraud_score = :s, "
                        "all_fraud_flags = :f, fraud_reviewed_at = :t"
                    ),
                    ExpressionAttributeValues={
                        ":d": decision,
                        ":s": str(round(combined_score, 4)),
                        ":f": all_flags,
                        ":t": datetime.now(timezone.utc).isoformat(),
                    },
                )
            except ClientError as e:
                log.error("Failed to update fraud decision for %s: %s", order_id, e)

            log.info(
                "Fraud decision for order %s: %s (score=%.2f, flags=%s)",
                order_id, decision, combined_score, all_flags,
            )

        except Exception as e:
            log.error("Failed to process fraud record: %s", e)
            raise  # SQS will retry
