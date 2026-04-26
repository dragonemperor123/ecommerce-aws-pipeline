"""Tests for stream enricher Lambda."""
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
                "messageId": f"msg-{i:04d}",
                "body": json.dumps(e),
                "eventSource": "aws:sqs",
            }
            for i, e in enumerate(events)
        ]
    }


def setup_aws(bucket_name="processed-test", table_name="sessions-test"):
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "session_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket_name)
    os.environ["SESSIONS_TABLE"] = table_name
    os.environ["PROCESSED_BUCKET"] = bucket_name


@mock_aws
def test_enricher_adds_derived_fields():
    setup_aws()
    from lambdas.stream_enricher import handler
    importlib.reload(handler)

    click = {
        "event_type": "PAGE_VIEW",
        "event_id": "evt-001",
        "session_id": "sess-abc",
        "customer_id": "CUST-000001",
        "product_id": "PROD-0001",
        "action": "view",
        "page": "/products/PROD-0001",
        "referrer": "google",
        "device": "desktop",
        "timestamp": "2026-04-13T14:30:00+00:00",
        "duration_ms": 5000,
    }

    result = handler.lambda_handler(make_kinesis_event([click]), None)
    assert result is None

    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    item = ddb.Table("sessions-test").get_item(Key={"session_id": "sess-abc"}).get("Item")
    assert item is not None
    assert item["event_count"] == 1


@mock_aws
def test_enricher_handles_anonymous_session():
    setup_aws()
    from lambdas.stream_enricher import handler
    importlib.reload(handler)

    click = {
        "event_type": "PAGE_VIEW",
        "event_id": "evt-002",
        "session_id": "sess-anon",
        "customer_id": None,
        "product_id": "PROD-0002",
        "action": "view",
        "page": "/products/PROD-0002",
        "referrer": "direct",
        "device": "mobile",
        "timestamp": "2026-04-13T20:00:00+00:00",
        "duration_ms": 1500,
    }

    result = handler.lambda_handler(make_kinesis_event([click]), None)
    assert result is None


@mock_aws
def test_enricher_batches_multiple_events():
    setup_aws()
    from lambdas.stream_enricher import handler
    importlib.reload(handler)

    clicks = [
        {
            "event_type": "PAGE_VIEW", "event_id": f"evt-{i:03d}",
            "session_id": f"sess-{i % 3}", "customer_id": "CUST-000001",
            "product_id": f"PROD-{i:04d}", "action": "view",
            "page": f"/products/PROD-{i:04d}", "referrer": "google",
            "device": "desktop", "timestamp": "2026-04-13T10:00:00+00:00",
            "duration_ms": 3000,
        }
        for i in range(10)
    ]

    result = handler.lambda_handler(make_kinesis_event(clicks), None)
    assert result is None
