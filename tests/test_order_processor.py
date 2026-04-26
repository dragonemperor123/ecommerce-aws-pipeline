"""
Unit tests for order processor Lambda.
Uses moto to mock AWS services.
"""
import json
import os
import pytest

import boto3
from moto import mock_aws


@pytest.fixture(autouse=True)
def aws_credentials():
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def dynamodb_table():
    with mock_aws():
        client = boto3.resource("dynamodb", region_name="us-east-1")
        table = client.create_table(
            TableName="orders-test",
            KeySchema=[
                {"AttributeName": "order_id", "KeyType": "HASH"},
                {"AttributeName": "customer_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "order_id", "AttributeType": "S"},
                {"AttributeName": "customer_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield table


@pytest.fixture
def sns_topics():
    with mock_aws():
        client = boto3.client("sns", region_name="us-east-1")
        fraud_topic = client.create_topic(Name="fraud-test")["TopicArn"]
        order_topic = client.create_topic(Name="orders-test")["TopicArn"]
        yield {"fraud": fraud_topic, "orders": order_topic}


def make_kinesis_event(order: dict) -> dict:
    return {
        "Records": [
            {
                "messageId": "msg-0001",
                "body": json.dumps(order),
                "eventSource": "aws:sqs",
            }
        ]
    }


@mock_aws
def test_normal_order_processed():
    os.environ["ORDERS_TABLE"] = "orders-test"
    os.environ["FRAUD_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:fraud-test"
    os.environ["ORDER_EVENTS_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:orders-test"

    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    table = ddb.create_table(
        TableName="orders-test",
        KeySchema=[
            {"AttributeName": "order_id", "KeyType": "HASH"},
            {"AttributeName": "customer_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "order_id", "AttributeType": "S"},
            {"AttributeName": "customer_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    sns = boto3.client("sns", region_name="us-east-1")
    fraud_arn = sns.create_topic(Name="fraud-test")["TopicArn"]
    order_arn = sns.create_topic(Name="orders-test")["TopicArn"]
    os.environ["FRAUD_TOPIC_ARN"] = fraud_arn
    os.environ["ORDER_EVENTS_TOPIC_ARN"] = order_arn

    import importlib
    from lambdas.order_processor import handler
    importlib.reload(handler)

    order = {
        "order_id": "ORD-TEST001",
        "customer_id": "cust-aaa",
        "customer_tier": "silver",
        "items": [{"product_id": "abc123", "quantity": 1, "unit_price": 49.90, "category": "health_beauty", "freight": 5.0}],
        "subtotal": 49.90,
        "discount": 0.0,
        "total": 49.90,
        "payment_method": "credit_card",
        "shipping_address": {"city": "sao paulo", "state": "SP", "country": "BR"},
        "status": "confirmed",
        "created_at": "2026-04-13T12:00:00+00:00",
        "is_suspicious": False,
    }

    result = handler.lambda_handler(make_kinesis_event(order), None)
    assert result is None

    item = table.get_item(Key={"order_id": "ORD-TEST001", "customer_id": "cust-aaa"})["Item"]
    assert item["order_id"] == "ORD-TEST001"
    assert float(item["fraud_score"]) < 0.5


@mock_aws
def test_suspicious_order_triggers_fraud_alert():
    os.environ["ORDERS_TABLE"] = "orders-test"

    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    table = ddb.create_table(
        TableName="orders-test",
        KeySchema=[
            {"AttributeName": "order_id", "KeyType": "HASH"},
            {"AttributeName": "customer_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "order_id", "AttributeType": "S"},
            {"AttributeName": "customer_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    sns = boto3.client("sns", region_name="us-east-1")
    os.environ["FRAUD_TOPIC_ARN"] = sns.create_topic(Name="fraud-test")["TopicArn"]
    os.environ["ORDER_EVENTS_TOPIC_ARN"] = sns.create_topic(Name="orders-test")["TopicArn"]

    import importlib
    from lambdas.order_processor import handler
    importlib.reload(handler)

    order = {
        "order_id": "ORD-FRAUD001",
        "customer_id": "cust-bbb",
        "customer_tier": "bronze",
        # total=600 > high_value_threshold(500); bronze tier > bronze_threshold(200); crypto payment
        "items": [
            {"product_id": "abc123", "quantity": 1, "unit_price": 300.0, "category": "computers_accessories", "freight": 10.0},
            {"product_id": "def456", "quantity": 1, "unit_price": 300.0, "category": "computers_accessories", "freight": 10.0},
        ],
        "subtotal": 600.0,
        "discount": 0.0,
        "total": 600.0,
        "payment_method": "crypto",
        "shipping_address": {"city": "rio de janeiro", "state": "RJ", "country": "BR"},
        "status": "confirmed",
        "created_at": "2026-04-13T12:00:00+00:00",
        "is_suspicious": True,
    }

    handler.lambda_handler(make_kinesis_event(order), None)

    item = table.get_item(Key={"order_id": "ORD-FRAUD001", "customer_id": "cust-bbb"})["Item"]
    assert float(item["fraud_score"]) >= 0.5


@mock_aws
def test_boleto_order_not_flagged():
    """bank_transfer (boleto) is Brazil's standard payment — must not raise fraud flags."""
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName="orders-test",
        KeySchema=[
            {"AttributeName": "order_id", "KeyType": "HASH"},
            {"AttributeName": "customer_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "order_id", "AttributeType": "S"},
            {"AttributeName": "customer_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    sns_client = boto3.client("sns", region_name="us-east-1")
    os.environ["FRAUD_TOPIC_ARN"]        = sns_client.create_topic(Name="fraud-test")["TopicArn"]
    os.environ["ORDER_EVENTS_TOPIC_ARN"] = sns_client.create_topic(Name="orders-test")["TopicArn"]
    os.environ["ORDERS_TABLE"]           = "orders-test"

    import importlib
    from lambdas.order_processor import handler
    importlib.reload(handler)

    order = {
        "order_id":    "ORD-BOLETO01",
        "customer_id": "cust-aaa",
        "customer_tier": "silver",
        "items": [{"product_id": "abc123", "quantity": 1, "unit_price": 80.0,
                   "category": "health_beauty", "freight": 8.0}],
        "subtotal": 80.0,
        "discount":  0.0,
        "total":    80.0,
        "payment_method": "bank_transfer",   # boleto
        "shipping_address": {"city": "sao paulo", "state": "SP", "country": "BR"},
        "status": "confirmed",
        "created_at": "2026-04-13T12:00:00+00:00",
        "is_suspicious": False,
    }

    handler.lambda_handler(make_kinesis_event(order), None)

    table = boto3.resource("dynamodb", region_name="us-east-1").Table("orders-test")
    item = table.get_item(Key={"order_id": "ORD-BOLETO01", "customer_id": "cust-aaa"})["Item"]
    assert float(item["fraud_score"]) < 0.5
    assert "unusual_payment_method" not in item.get("fraud_flags", [])
