"""
Inventory Alerter Lambda
- Consumes INVENTORY_UPDATE events from Kinesis
- Publishes low-stock alerts to SNS
- Updates product table with current stock level
"""
import base64
import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

PRODUCTS_TABLE = os.environ["PRODUCTS_TABLE"]
LOW_INVENTORY_TOPIC_ARN = os.environ["LOW_INVENTORY_TOPIC_ARN"]

products_table = dynamodb.Table(PRODUCTS_TABLE)

LOW_STOCK_THRESHOLD = 10
CRITICAL_STOCK_THRESHOLD = 3


def process_inventory_event(event: dict) -> None:
    product_id = event["product_id"]
    current_stock = event["current_stock"]

    # Update product table
    try:
        products_table.update_item(
            Key={"product_id": product_id},
            UpdateExpression=(
                "SET stock_level = :s, last_updated = :t, "
                "product_name = if_not_exists(product_name, :n), "
                "category = if_not_exists(category, :c)"
            ),
            ExpressionAttributeValues={
                ":s": current_stock,
                ":t": datetime.now(timezone.utc).isoformat(),
                ":n": event.get("product_name", "Unknown"),
                ":c": event.get("category", "Unknown"),
            },
        )
    except ClientError as e:
        log.error("Failed to update product %s: %s", product_id, e)

    # Alert on low stock
    if current_stock <= LOW_STOCK_THRESHOLD:
        severity = "CRITICAL" if current_stock <= CRITICAL_STOCK_THRESHOLD else "WARNING"
        try:
            sns.publish(
                TopicArn=LOW_INVENTORY_TOPIC_ARN,
                Message=json.dumps({
                    "product_id": product_id,
                    "product_name": event.get("product_name"),
                    "category": event.get("category"),
                    "current_stock": current_stock,
                    "severity": severity,
                    "warehouse_id": event.get("warehouse_id"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }),
                Subject=f"Low Inventory Alert [{severity}]: {event.get('product_name', product_id)}",
                MessageAttributes={
                    "severity": {"DataType": "String", "StringValue": severity},
                    "category": {"DataType": "String", "StringValue": event.get("category", "Unknown")},
                },
            )
            log.warning(
                "LOW STOCK [%s]: product=%s stock=%d warehouse=%s",
                severity, product_id, current_stock, event.get("warehouse_id"),
            )
        except ClientError as e:
            log.error("Failed to publish inventory alert: %s", e)


def lambda_handler(event, context):
    records = event.get("Records", [])
    success = 0
    failed_item_ids = []

    for record in records:
        try:
            payload = json.loads(base64.b64decode(record["kinesis"]["data"]).decode("utf-8"))
            process_inventory_event(payload)
            success += 1
        except Exception as e:
            log.error("Failed to process inventory record %s: %s", record["kinesis"]["sequenceNumber"], e)
            failed_item_ids.append({"itemIdentifier": record["kinesis"]["sequenceNumber"]})

    log.info("Processed %d/%d inventory records", success, len(records))

    if failed_item_ids:
        return {"batchItemFailures": failed_item_ids}
