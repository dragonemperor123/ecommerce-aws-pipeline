"""
Batch inference: generates top-N recommendations for every customer using
the trained ALS model and writes them to DynamoDB.

Run locally after training:
    python scripts/precompute_recommendations.py \
        --model-bucket ecomstoragestack-modelbucketb33d855b-zeoqq7kw1iqe \
        --products-table EcomStorageStack-ProductsTable241ADBFF-1FUSOMZ2YXLKL \
        --reco-table ecom-recommendations \
        --region eu-north-1
"""
import argparse
import io
import json
import logging
import tarfile
import tempfile
from decimal import Decimal

import boto3
import joblib
import numpy as np
from boto3.dynamodb.types import TypeSerializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

N_RECO = 20  # top-N to store per customer


def load_model(bucket: str, key: str = "recommendation/model.tar.gz") -> dict:
    log.info("Downloading model from s3://%s/%s", bucket, key)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    tar_bytes = obj["Body"].read()
    with tempfile.TemporaryDirectory() as tmpdir:
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
            tar.extractall(tmpdir)
        return {
            "model":               joblib.load(f"{tmpdir}/model.joblib"),
            "customer_idx":        joblib.load(f"{tmpdir}/customer_idx.joblib"),
            "reverse_product_idx": joblib.load(f"{tmpdir}/reverse_product_idx.joblib"),
        }


def load_product_meta(products_table_name: str, region: str) -> dict:
    """Load category + price for every product from DynamoDB."""
    log.info("Loading product metadata from DynamoDB...")
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(products_table_name)
    meta = {}
    kwargs = {"ProjectionExpression": "product_id, category, unit_price"}
    while True:
        resp = table.scan(**kwargs)
        for item in resp["Items"]:
            meta[item["product_id"]] = {
                "category":   item.get("category"),
                "unit_price": float(item["unit_price"]) if item.get("unit_price") is not None else None,
            }
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    log.info("Loaded metadata for %d products", len(meta))
    return meta


def batch_write(table, items: list):
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def run(model_bucket, products_table_name, reco_table_name, region):
    artifacts   = load_model(model_bucket)
    product_meta = load_product_meta(products_table_name, region)

    model            = artifacts["model"]
    customer_idx     = artifacts["customer_idx"]
    reverse_pid      = artifacts["reverse_product_idx"]

    dynamodb = boto3.resource("dynamodb", region_name=region)
    reco_table = dynamodb.Table(reco_table_name)

    customers   = list(customer_idx.items())
    total       = len(customers)
    batch_items = []
    written     = 0

    log.info("Generating recommendations for %d customers...", total)

    # Global popular items for cold-start enrichment
    norms     = np.linalg.norm(model.item_factors, axis=1)
    pop_ids   = np.argsort(norms)[::-1][:N_RECO * 2]
    pop_recs  = [reverse_pid[int(i)] for i in pop_ids if int(i) in reverse_pid][:N_RECO]

    for i, (customer_id, uid) in enumerate(customers):
        try:
            ids, scores = model.recommend(
                uid, model.user_factors[uid],
                N=N_RECO, filter_already_liked_items=True,
            )
            recs = []
            for iid, score in zip(ids, scores):
                pid  = reverse_pid.get(int(iid))
                if pid is None:
                    continue
                meta = product_meta.get(pid, {})
                recs.append({
                    "product_id": pid,
                    "score":      round(float(score), 4),
                    "category":   meta.get("category"),
                    "unit_price": meta.get("unit_price"),
                })
        except Exception as e:
            log.warning("Skipping %s: %s", customer_id, e)
            recs = []

        batch_items.append({
            "customer_id":     customer_id,
            "recommendations": recs,
            "popular_fallback": [
                {"product_id": pid, "score": 0.5, "category": product_meta.get(pid, {}).get("category"),
                 "unit_price": product_meta.get(pid, {}).get("unit_price")}
                for pid in pop_recs
            ],
        })

        if len(batch_items) >= 25:
            batch_write(reco_table, batch_items)
            written += len(batch_items)
            batch_items = []
            if written % 5000 == 0:
                log.info("Written %d / %d (%.1f%%)", written, total, 100 * written / total)

    if batch_items:
        batch_write(reco_table, batch_items)
        written += len(batch_items)

    log.info("Done — wrote recommendations for %d customers", written)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-bucket",    required=True)
    parser.add_argument("--products-table",  required=True)
    parser.add_argument("--reco-table",      required=True)
    parser.add_argument("--region",          default="eu-north-1")
    args = parser.parse_args()
    run(args.model_bucket, args.products_table, args.reco_table, args.region)
