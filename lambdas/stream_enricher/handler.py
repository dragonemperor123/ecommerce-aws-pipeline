"""
Stream Enricher Lambda
- Consumes clickstream events from Kinesis
- Updates session state in DynamoDB
- Writes enriched events to S3 (processed zone)
- Batches writes for efficiency
"""
import base64
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from io import StringIO

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

SESSIONS_TABLE = os.environ["SESSIONS_TABLE"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]

sessions_table = dynamodb.Table(SESSIONS_TABLE)
SESSION_TTL_SECONDS = 1800  # 30 minutes


def enrich_event(event: dict) -> dict:
    """Add derived fields to a click event."""
    ts = event.get("timestamp", datetime.now(timezone.utc).isoformat())
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return {
        **event,
        "hour_of_day": dt.hour,
        "day_of_week": dt.strftime("%A"),
        "is_weekend": dt.weekday() >= 5,
        "is_authenticated": event.get("customer_id") is not None,
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }


def update_session(session_id: str, event: dict) -> None:
    try:
        sessions_table.update_item(
            Key={"session_id": session_id},
            UpdateExpression=(
                "SET last_seen = :ts, event_count = if_not_exists(event_count, :zero) + :one, "
                "expires_at = :exp, customer_id = if_not_exists(customer_id, :cid)"
            ),
            ExpressionAttributeValues={
                ":ts": event.get("timestamp"),
                ":one": 1,
                ":zero": 0,
                ":exp": int(time.time()) + SESSION_TTL_SECONDS,
                ":cid": event.get("customer_id") or "anonymous",
            },
        )
    except ClientError as e:
        log.warning("Session update failed for %s: %s", session_id, e)


def flush_to_s3(enriched_events: list) -> None:
    if not enriched_events:
        return
    now = datetime.now(timezone.utc)
    key = (
        f"clickstream/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"batch-{int(time.time() * 1000)}.ndjson"
    )
    body = "\n".join(json.dumps(e) for e in enriched_events)
    try:
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        log.info("Flushed %d events to s3://%s/%s", len(enriched_events), PROCESSED_BUCKET, key)
    except ClientError as e:
        log.error("S3 flush failed: %s", e)
        raise


def lambda_handler(event, context):
    records = event.get("Records", [])
    enriched = []
    failed_item_ids = []

    for record in records:
        try:
            payload = json.loads(base64.b64decode(record["kinesis"]["data"]).decode("utf-8"))
            enriched_event = enrich_event(payload)
            update_session(payload["session_id"], payload)
            enriched.append(enriched_event)
        except Exception as e:
            log.error("Failed to enrich record %s: %s", record["kinesis"]["sequenceNumber"], e)
            failed_item_ids.append({"itemIdentifier": record["kinesis"]["sequenceNumber"]})

    flush_to_s3(enriched)
    log.info("Enriched %d/%d click records", len(enriched), len(records))

    if failed_item_ids:
        return {"batchItemFailures": failed_item_ids}
