"""
Recommendation API Lambda
- Handles POST /recommendations requests from API Gateway
- Calls SageMaker endpoint for real-time inference
- Enriches response with product metadata from DynamoDB
"""
import json
import logging
import os
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

sagemaker_runtime = boto3.client("sagemaker-runtime")
dynamodb = boto3.resource("dynamodb")

SAGEMAKER_ENDPOINT = os.environ["SAGEMAKER_ENDPOINT"]
PRODUCTS_TABLE = os.environ["PRODUCTS_TABLE"]

products_table = dynamodb.Table(PRODUCTS_TABLE)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def get_product_details(product_ids: list) -> dict:
    if not product_ids:
        return {}
    try:
        keys = [{"product_id": pid} for pid in product_ids[:10]]
        response = dynamodb.meta.client.batch_get_item(
            RequestItems={PRODUCTS_TABLE: {"Keys": keys}}
        )
        items = response.get("Responses", {}).get(PRODUCTS_TABLE, [])
        return {item["product_id"]: item for item in items}
    except ClientError as e:
        log.warning("Could not fetch product details: %s", e)
        return {}


def call_sagemaker(payload: dict) -> list:
    try:
        response = sagemaker_runtime.invoke_endpoint(
            EndpointName=SAGEMAKER_ENDPOINT,
            ContentType="application/json",
            Body=json.dumps(payload),
        )
        result = json.loads(response["Body"].read().decode("utf-8"))
        return result.get("recommendations", [])
    except ClientError as e:
        log.error("SageMaker invocation failed: %s", e)
        raise


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON body"}),
            "headers": {"Content-Type": "application/json"},
        }

    customer_id = body.get("customer_id")
    context_product_id = body.get("product_id")
    n = min(int(body.get("n", 5)), 20)

    if not customer_id and not context_product_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Provide customer_id or product_id"}),
            "headers": {"Content-Type": "application/json"},
        }

    try:
        recommendations = call_sagemaker({
            "customer_id": customer_id,
            "product_id": context_product_id,
            "n": n,
        })
    except Exception as e:
        log.error("Recommendation inference failed: %s", e)
        return {
            "statusCode": 503,
            "body": json.dumps({"error": "Recommendation service unavailable"}),
            "headers": {"Content-Type": "application/json"},
        }

    product_details = get_product_details([r.get("product_id") for r in recommendations])

    enriched = []
    for rec in recommendations:
        pid = rec.get("product_id")
        details = product_details.get(pid, {})
        enriched.append({
            "product_id": pid,
            "score": rec.get("score"),
            "rank": rec.get("rank"),
            "name": details.get("product_name"),
            "category": details.get("category"),
            "price": details.get("price"),
            "stock_level": details.get("stock_level"),
        })

    return {
        "statusCode": 200,
        "body": json.dumps(
            {"customer_id": customer_id, "recommendations": enriched, "count": len(enriched)},
            cls=DecimalEncoder,
        ),
        "headers": {"Content-Type": "application/json"},
    }
