"""
Recommendation API Lambda
- Handles POST /recommendations requests from API Gateway
- Uses SageMaker endpoint when available, falls back to DynamoDB catalog scan
- Enriches response with product metadata from DynamoDB
"""
import json
import logging
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

SAGEMAKER_ENDPOINT = os.environ.get("SAGEMAKER_ENDPOINT", "")
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


def call_sagemaker(payload: dict, n: int) -> list:
    sagemaker_runtime = boto3.client("sagemaker-runtime")
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


def catalog_fallback(category_hint: str, n: int) -> list:
    """Return top-N products from DynamoDB, optionally filtered by category."""
    try:
        if category_hint:
            resp = products_table.scan(
                FilterExpression=Attr("category").eq(category_hint),
                Limit=n * 3,
            )
        else:
            resp = products_table.scan(Limit=n * 3)
        items = resp.get("Items", [])[:n]
        return [
            {
                "product_id": item["product_id"],
                "score": None,
                "rank": i + 1,
                "category": item.get("category"),
                "unit_price": float(item["unit_price"]) if item.get("unit_price") is not None else None,
                "stock_level": int(item["stock_level"]) if item.get("stock_level") is not None else None,
                "source": "catalog_fallback",
            }
            for i, item in enumerate(items)
        ]
    except ClientError as e:
        log.error("DynamoDB scan failed: %s", e)
        return []


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
    category_hint = body.get("category", "")
    n = min(int(body.get("n", 5)), 20)

    if not customer_id and not context_product_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Provide customer_id or product_id"}),
            "headers": {"Content-Type": "application/json"},
        }

    # Use SageMaker when endpoint is configured, otherwise use catalog fallback
    if SAGEMAKER_ENDPOINT:
        try:
            recommendations = call_sagemaker({
                "customer_id": customer_id,
                "product_id": context_product_id,
                "n": n,
            }, n)
        except Exception as e:
            log.warning("SageMaker unavailable, falling back to catalog: %s", e)
            recommendations = catalog_fallback(category_hint, n)
    else:
        log.info("No SageMaker endpoint configured — using catalog fallback")
        recommendations = catalog_fallback(category_hint, n)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "customer_id": customer_id,
                "recommendations": recommendations,
                "count": len(recommendations),
            },
            cls=DecimalEncoder,
        ),
        "headers": {"Content-Type": "application/json"},
    }
