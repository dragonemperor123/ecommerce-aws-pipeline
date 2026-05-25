"""
Recommendation API Lambda — ALS collaborative filtering

Flow:
  1. On first invocation, download the trained ALS model (implicit library)
     from S3 and cache it in /tmp for the lifetime of the Lambda container.
  2. Given a customer_id, call model.recommend() to get personalised
     product-level scores from the ALS latent factors.
  3. Enrich the scored product IDs with metadata (category, price) from DynamoDB.
  4. Cold-start (customer not in training data): fall back to globally popular
     items derived from ALS item-factor norms, filtered to mid-range prices.
  5. Additionally filter by price-affinity using the customer's order history
     so recommendations match the customer's real spending behaviour.
"""
import io
import json
import logging
import os
import random
import tarfile
import tempfile
from decimal import Decimal

import boto3
import joblib
import numpy as np
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

try:
    import anthropic as _anthropic
    _anthropic_available = True
except ImportError:
    _anthropic_available = False

log = logging.getLogger()
log.setLevel(logging.INFO)

dynamodb  = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

PRODUCTS_TABLE   = os.environ["PRODUCTS_TABLE"]
ORDERS_TABLE     = os.environ.get("ORDERS_TABLE", "")
MODEL_BUCKET     = os.environ.get("MODEL_BUCKET", "")
MODEL_KEY        = os.environ.get("MODEL_KEY", "recommendation/model.tar.gz")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

products_table = dynamodb.Table(PRODUCTS_TABLE)
orders_table   = dynamodb.Table(ORDERS_TABLE) if ORDERS_TABLE else None

CORS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
}

# Module-level cache — survives across warm invocations
_model_cache: dict | None = None


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


# ── Model loading ────────────────────────────────────────────────────────────

def load_model() -> dict:
    global _model_cache
    if _model_cache is not None:
        return _model_cache

    if not MODEL_BUCKET:
        log.warning("MODEL_BUCKET not set — ALS model unavailable")
        return {}

    log.info("Downloading ALS model from s3://%s/%s", MODEL_BUCKET, MODEL_KEY)
    try:
        obj = s3_client.get_object(Bucket=MODEL_BUCKET, Key=MODEL_KEY)
        tar_bytes = obj["Body"].read()

        with tempfile.TemporaryDirectory() as tmpdir:
            with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
                tar.extractall(tmpdir)

            # Load raw numpy factor arrays — no implicit/OpenMP dependency
            _model_cache = {
                "user_factors":        np.load(os.path.join(tmpdir, "user_factors.npy")),
                "item_factors":        np.load(os.path.join(tmpdir, "item_factors.npy")),
                "customer_idx":        joblib.load(os.path.join(tmpdir, "customer_idx.joblib")),
                "reverse_product_idx": joblib.load(os.path.join(tmpdir, "reverse_product_idx.joblib")),
            }
        log.info("ALS model loaded — %d customers, %d products",
                 len(_model_cache["customer_idx"]), len(_model_cache["item_factors"]))
        return _model_cache
    except Exception as e:
        log.error("Failed to load ALS model: %s", e)
        return {}


# ── Customer history ─────────────────────────────────────────────────────────

def get_customer_avg_spend(customer_id: str) -> float | None:
    if not orders_table:
        return None
    try:
        resp = orders_table.scan(
            FilterExpression=Attr("customer_id").eq(customer_id),
            ProjectionExpression="#t, item_count",
            ExpressionAttributeNames={"#t": "total"},
        )
        orders = resp.get("Items", [])
        if not orders:
            return None
        total_spend = sum(float(o.get("total", 0)) for o in orders)
        total_items = sum(int(o.get("item_count", 1)) for o in orders)
        return total_spend / max(total_items, 1)
    except ClientError as e:
        log.warning("Could not fetch orders for %s: %s", customer_id, e)
        return None


# ── DynamoDB enrichment ──────────────────────────────────────────────────────

def enrich_products(product_ids: list) -> dict:
    if not product_ids:
        return {}
    try:
        resp = dynamodb.meta.client.batch_get_item(
            RequestItems={PRODUCTS_TABLE: {"Keys": [{"product_id": pid} for pid in product_ids[:100]]}}
        )
        items = resp.get("Responses", {}).get(PRODUCTS_TABLE, [])
        return {item["product_id"]: item for item in items}
    except ClientError as e:
        log.warning("batch_get_item failed: %s", e)
        return {}


# ── ALS recommendations ──────────────────────────────────────────────────────

def als_recommend(customer_id: str, n: int, avg_spend: float | None, category_filter: str = "") -> tuple[list, str]:
    artifacts = load_model()
    if not artifacts:
        return [], "model_unavailable"

    user_factors = artifacts["user_factors"]
    item_factors = artifacts["item_factors"]
    customer_idx = artifacts["customer_idx"]
    reverse_pid  = artifacts["reverse_product_idx"]

    fetch_n = min(n * 8, 200)

    if customer_id in customer_idx:
        uid    = customer_idx[customer_id]
        # Pure numpy dot product — no implicit/OpenMP needed
        scores_all = item_factors @ user_factors[uid]
        top_idx    = np.argsort(scores_all)[::-1][:fetch_n]
        ids, scores = top_idx, scores_all[top_idx]
        strategy = "als_personalised"
    else:
        # Cold start: rank by item-factor norm (global popularity proxy)
        norms      = np.linalg.norm(item_factors, axis=1)
        top_idx    = np.argsort(norms)[::-1][:fetch_n]
        ids, scores = top_idx, norms[top_idx]
        strategy   = "als_popular"

    candidates = [
        {"product_id": reverse_pid[int(iid)], "raw_score": float(sc)}
        for iid, sc in zip(ids, scores)
        if int(iid) in reverse_pid
    ]

    # Enrich with DynamoDB metadata
    pid_list  = [c["product_id"] for c in candidates]
    meta      = enrich_products(pid_list)

    results = []
    for c in candidates:
        pid  = c["product_id"]
        item = meta.get(pid, {})
        price = float(item["unit_price"]) if item.get("unit_price") is not None else None

        # Category filter
        if category_filter:
            item_cat = (item.get("category") or "").lower()
            if category_filter not in item_cat:
                continue

        # Price-affinity filter: keep products within ±60% of avg spend
        if avg_spend and price is not None:
            if not (avg_spend * 0.4 <= price <= avg_spend * 1.6):
                continue

        results.append({
            "product_id":  pid,
            "score":       round(min(c["raw_score"], 0.99), 3),
            "rank":        len(results) + 1,
            "category":    item.get("category"),
            "unit_price":  price,
            "stock_level": int(item["stock_level"]) if item.get("stock_level") is not None else None,
            "source":      strategy,
        })

        if len(results) >= n:
            break

    # If price filter left too few results, relax to 3× range and fill
    if len(results) < n:
        for c in candidates:
            if len(results) >= n:
                break
            pid = c["product_id"]
            if any(r["product_id"] == pid for r in results):
                continue
            item = meta.get(pid, {})
            if category_filter:
                item_cat = (item.get("category") or "").lower()
                if category_filter not in item_cat:
                    continue
            price = float(item["unit_price"]) if item.get("unit_price") is not None else None
            if avg_spend and price is not None:
                if not (avg_spend * 0.2 <= price <= avg_spend * 3.0):
                    continue
            results.append({
                "product_id":  pid,
                "score":       round(min(c["raw_score"], 0.99), 3),
                "rank":        len(results) + 1,
                "category":    item.get("category"),
                "unit_price":  price,
                "stock_level": int(item["stock_level"]) if item.get("stock_level") is not None else None,
                "source":      strategy,
            })

    return results, strategy


# ── Claude explanation ───────────────────────────────────────────────────────

def generate_explanation(recommendations: list, avg_spend: float | None, strategy: str) -> str | None:
    if not ANTHROPIC_API_KEY or not _anthropic_available:
        return None
    try:
        categories = list({r["category"] for r in recommendations if r.get("category")})[:3]
        price_range = None
        prices = [r["unit_price"] for r in recommendations if r.get("unit_price")]
        if prices:
            price_range = f"R${min(prices):.0f}–R${max(prices):.0f}"

        context_parts = []
        if avg_spend:
            context_parts.append(f"average spend of R${avg_spend:.0f}/item")
        if categories:
            context_parts.append(f"interest in {', '.join(categories)}")
        if price_range:
            context_parts.append(f"price range {price_range}")

        if strategy == "catalog_fallback":
            prompt = "Write one friendly sentence (max 20 words) explaining that we're showing popular products while we learn their preferences."
        elif strategy == "als_popular":
            prompt = f"Write one friendly sentence (max 20 words) explaining we're showing trending products to a new customer. Context: {', '.join(context_parts) or 'new customer'}."
        else:
            prompt = f"Write one friendly sentence (max 20 words) explaining personalised product recommendations. Context: customer with {', '.join(context_parts) or 'purchase history'}."

        client = _anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=60,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.warning("Explanation generation failed: %s", e)
        return None


# ── Lambda handler ───────────────────────────────────────────────────────────

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON body"}), "headers": CORS}

    customer_id     = body.get("customer_id")
    n               = min(int(body.get("n", 5)), 20)
    category_filter = body.get("category", "").strip().lower().replace(" ", "_")

    if not customer_id:
        return {"statusCode": 400, "body": json.dumps({"error": "Provide customer_id"}), "headers": CORS}

    avg_spend     = get_customer_avg_spend(customer_id)
    recommendations, strategy = als_recommend(customer_id, n, avg_spend, category_filter)

    # Catalog fallback when model is unavailable or returned nothing
    if not recommendations:
        strategy = "catalog_fallback"
        try:
            resp = products_table.scan(Limit=n * 4)
            items = resp.get("Items", [])[:n]
            recommendations = [
                {
                    "product_id":  item["product_id"],
                    "score":       None,
                    "rank":        i + 1,
                    "category":    item.get("category"),
                    "unit_price":  float(item["unit_price"]) if item.get("unit_price") is not None else None,
                    "stock_level": int(item["stock_level"]) if item.get("stock_level") is not None else None,
                    "source":      "catalog_fallback",
                }
                for i, item in enumerate(items)
            ]
        except Exception as e:
            log.error("Catalog fallback failed: %s", e)

    for i, r in enumerate(recommendations):
        r["rank"] = i + 1

    explanation = generate_explanation(recommendations, avg_spend, strategy)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "customer_id":        customer_id,
                "recommendations":    recommendations,
                "count":              len(recommendations),
                "strategy":           strategy,
                "avg_spend_per_item": round(avg_spend, 2) if avg_spend else None,
                "explanation":        explanation,
            },
            cls=DecimalEncoder,
        ),
        "headers": CORS,
    }
