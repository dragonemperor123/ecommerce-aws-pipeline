"""Tests for inventory alerter Lambda."""
import base64
import json
import os
import importlib
import pytest
from moto import mock_aws
import boto3


def make_kinesis_event(events: list) -> dict:
    return {
        "Records": [
            {
                "kinesis": {
                    "sequenceNumber": str(i),
                    "data": base64.b64encode(json.dumps(e).encode()).decode(),
                    "partitionKey": e.get("product_id", "p1"),
                }
            }
            for i, e in enumerate(events)
        ]
    }


def setup_aws():
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    table = ddb.create_table(
        TableName="products-test",
        KeySchema=[{"AttributeName": "product_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "product_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    sns = boto3.client("sns", region_name="us-east-1")
    topic_arn = sns.create_topic(Name="inventory-test")["TopicArn"]
    os.environ["PRODUCTS_TABLE"] = "products-test"
    os.environ["LOW_INVENTORY_TOPIC_ARN"] = topic_arn
    return table


@mock_aws
def test_normal_stock_update_no_alert():
    table = setup_aws()
    from lambdas.inventory_alerter import handler
    importlib.reload(handler)

    event = {
        "event_type": "INVENTORY_UPDATE", "event_id": "inv-001",
        "product_id": "PROD-0001", "product_name": "Test Widget",
        "category": "Electronics", "previous_stock": 100,
        "current_stock": 95, "delta": -5,
        "warehouse_id": "WH-01", "timestamp": "2026-04-13T10:00:00+00:00",
    }

    result = handler.lambda_handler(make_kinesis_event([event]), None)
    assert result is None

    item = table.get_item(Key={"product_id": "PROD-0001"}).get("Item")
    assert item["stock_level"] == 95


@mock_aws
def test_low_stock_triggers_warning_alert():
    table = setup_aws()
    from lambdas.inventory_alerter import handler
    importlib.reload(handler)

    event = {
        "event_type": "INVENTORY_UPDATE", "event_id": "inv-002",
        "product_id": "PROD-0002", "product_name": "Scarce Item",
        "category": "Books", "previous_stock": 15,
        "current_stock": 7, "delta": -8,
        "warehouse_id": "WH-02", "timestamp": "2026-04-13T10:00:00+00:00",
    }

    result = handler.lambda_handler(make_kinesis_event([event]), None)
    assert result is None

    item = table.get_item(Key={"product_id": "PROD-0002"}).get("Item")
    assert item["stock_level"] == 7


@mock_aws
def test_critical_stock_triggers_critical_alert():
    table = setup_aws()
    from lambdas.inventory_alerter import handler
    importlib.reload(handler)

    event = {
        "event_type": "INVENTORY_UPDATE", "event_id": "inv-003",
        "product_id": "PROD-0003", "product_name": "Critical Item",
        "category": "Toys", "previous_stock": 5,
        "current_stock": 2, "delta": -3,
        "warehouse_id": "WH-03", "timestamp": "2026-04-13T10:00:00+00:00",
    }

    result = handler.lambda_handler(make_kinesis_event([event]), None)
    assert result is None

    item = table.get_item(Key={"product_id": "PROD-0003"}).get("Item")
    assert item["stock_level"] == 2


@mock_aws
def test_zero_stock_is_critical():
    table = setup_aws()
    from lambdas.inventory_alerter import handler
    importlib.reload(handler)

    event = {
        "event_type": "INVENTORY_UPDATE", "event_id": "inv-004",
        "product_id": "PROD-0004", "product_name": "Out of Stock Item",
        "category": "Sports", "previous_stock": 1,
        "current_stock": 0, "delta": -1,
        "warehouse_id": "WH-01", "timestamp": "2026-04-13T10:00:00+00:00",
    }

    result = handler.lambda_handler(make_kinesis_event([event]), None)
    assert result is None
