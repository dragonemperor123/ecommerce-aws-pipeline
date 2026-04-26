"""
Ecommerce Event Generator — powered by Brazilian Olist dataset
Replays real orders, clickstream, and inventory events
and pumps them into SQS queues.

Dataset path is controlled via --dataset-path (default: D:/Dataset_AWS)
"""
import json
import random
import time
import uuid
import argparse
import logging
import os
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional

import boto3
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_olist_data(dataset_path: str) -> dict:
    """Load and join Olist CSVs into lookup structures the generator needs."""
    log.info("Loading Olist dataset from %s ...", dataset_path)

    orders       = pd.read_csv(os.path.join(dataset_path, "olist_orders_dataset.csv"))
    order_items  = pd.read_csv(os.path.join(dataset_path, "olist_order_items_dataset.csv"))
    payments     = pd.read_csv(os.path.join(dataset_path, "olist_order_payments_dataset.csv"))
    customers    = pd.read_csv(os.path.join(dataset_path, "olist_customers_dataset.csv"))
    products     = pd.read_csv(os.path.join(dataset_path, "olist_products_dataset.csv"))
    sellers      = pd.read_csv(os.path.join(dataset_path, "olist_sellers_dataset.csv"))
    translations = pd.read_csv(os.path.join(dataset_path, "product_category_name_translation.csv"))

    # Translate product categories to English
    products = products.merge(translations, on="product_category_name", how="left")
    products["category"] = products["product_category_name_english"].fillna(
        products["product_category_name"].fillna("other")
    )

    # Build product lookup: product_id -> {product_id, category, price}
    # Use median price from order_items as the product's price
    product_prices = (
        order_items.groupby("product_id")["price"]
        .median()
        .reset_index()
        .rename(columns={"price": "unit_price"})
    )
    product_catalog = products[["product_id", "category"]].merge(product_prices, on="product_id", how="left")
    product_catalog["unit_price"] = product_catalog["unit_price"].fillna(50.0)
    product_catalog = product_catalog.dropna(subset=["product_id"])
    product_list = product_catalog.to_dict("records")

    # Build customer lookup: customer_id -> {customer_id, city, state, tier}
    # Assign tier based on purchase frequency
    purchase_counts = orders.groupby("customer_id").size().reset_index(name="order_count")
    customers = customers.merge(purchase_counts, on="customer_id", how="left")
    customers["order_count"] = customers["order_count"].fillna(1)

    def assign_tier(n):
        if n >= 5:   return "platinum"
        if n >= 3:   return "gold"
        if n >= 2:   return "silver"
        return "bronze"

    customers["tier"] = customers["order_count"].apply(assign_tier)
    customer_list = customers[["customer_id", "customer_unique_id", "customer_city",
                                "customer_state", "tier"]].to_dict("records")

    # Build payment method mapping: order_id -> payment_type
    # Keep only the primary payment (payment_sequential == 1)
    primary_payments = payments[payments["payment_sequential"] == 1][["order_id", "payment_type", "payment_value"]]

    # Build enriched orders: join orders + items + payments
    enriched_orders = (
        orders[["order_id", "customer_id", "order_status"]]
        .merge(primary_payments, on="order_id", how="left")
    )
    enriched_orders["payment_type"] = enriched_orders["payment_type"].fillna("credit_card")

    # Build order items grouped by order_id
    order_items_grouped = order_items.groupby("order_id").apply(
        lambda df: df[["product_id", "price", "freight_value"]].to_dict("records")
    ).to_dict()

    # Build seller warehouse lookup
    warehouse_ids = sellers["seller_id"].tolist()

    log.info(
        "Loaded %d products, %d customers, %d orders",
        len(product_list), len(customer_list), len(enriched_orders),
    )

    return {
        "products": product_list,
        "customers": customer_list,
        "enriched_orders": enriched_orders,
        "order_items_grouped": order_items_grouped,
        "warehouse_ids": warehouse_ids,
    }


# ---------------------------------------------------------------------------
# Event dataclasses
# ---------------------------------------------------------------------------

@dataclass
class OrderEvent:
    event_type: str
    event_id: str
    order_id: str
    customer_id: str
    customer_tier: str
    items: list
    subtotal: float
    discount: float
    total: float
    payment_method: str
    shipping_address: dict
    status: str
    created_at: str
    is_suspicious: bool


@dataclass
class ClickEvent:
    event_type: str
    event_id: str
    session_id: str
    customer_id: Optional[str]
    product_id: str
    action: str
    page: str
    referrer: str
    device: str
    timestamp: str
    duration_ms: int


@dataclass
class InventoryEvent:
    event_type: str
    event_id: str
    product_id: str
    category: str
    previous_stock: int
    current_stock: int
    delta: int
    warehouse_id: str
    timestamp: str


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------

# Payment method mapping from Olist types to pipeline-friendly names
PAYMENT_MAP = {
    "credit_card":  "credit_card",
    "boleto":       "bank_transfer",   # Brazilian bank slip — treat as bank transfer
    "voucher":      "paypal",          # Store credit / voucher
    "debit_card":   "debit_card",
    "not_defined":  "credit_card",
}

FRAUD_RULES = {
    "high_value_threshold": 500,
    "suspicious_payment": {"crypto"},   # boleto (bank_transfer) is legitimate in Brazil
    "bronze_threshold": 200,
}


def make_order_event(data: dict) -> Optional[OrderEvent]:
    """Build an OrderEvent by replaying a real Olist order."""
    orders_df    = data["enriched_orders"]
    items_map    = data["order_items_grouped"]
    customers    = data["customers"]
    products_map = {p["product_id"]: p for p in data["products"]}

    # Sample a random real order
    row = orders_df.sample(1).iloc[0]
    order_id  = row["order_id"]
    cust_id   = row["customer_id"]
    status    = row["order_status"]
    pay_type  = PAYMENT_MAP.get(row["payment_type"], "credit_card")

    raw_items = items_map.get(order_id, [])
    if not raw_items:
        return None

    items = []
    for item in raw_items:
        prod = products_map.get(item["product_id"])
        category = prod["category"] if prod else "other"
        items.append({
            "product_id": item["product_id"],
            "category": category,
            "quantity": 1,
            "unit_price": round(float(item["price"]), 2),
            "freight": round(float(item["freight_value"]), 2),
        })

    subtotal = round(sum(i["unit_price"] for i in items), 2)
    discount = round(subtotal * random.choice([0, 0, 0, 0.05, 0.10]), 2)
    total    = round(subtotal - discount, 2)

    # Customer tier lookup
    cust_record = next((c for c in customers if c["customer_id"] == cust_id), None)
    tier = cust_record["tier"] if cust_record else "bronze"
    city  = cust_record["customer_city"] if cust_record else "unknown"
    state = cust_record["customer_state"] if cust_record else "XX"

    is_suspicious = (
        total > FRAUD_RULES["high_value_threshold"]
        or pay_type in FRAUD_RULES["suspicious_payment"]
        or (tier == "bronze" and total > FRAUD_RULES["bronze_threshold"])
    )

    # Normalize order status to pipeline statuses
    status_map = {
        "delivered": "confirmed", "shipped": "confirmed",
        "approved": "confirmed",  "processing": "pending",
        "created": "pending",     "canceled": "failed",
        "unavailable": "failed",  "invoiced": "pending",
    }
    mapped_status = status_map.get(status, "confirmed")

    return OrderEvent(
        event_type="ORDER_PLACED",
        event_id=str(uuid.uuid4()),
        order_id=f"ORD-{uuid.uuid4().hex[:10].upper()}",
        customer_id=cust_id,
        customer_tier=tier,
        items=items,
        subtotal=subtotal,
        discount=discount,
        total=total,
        payment_method=pay_type,
        shipping_address={"city": city, "state": state, "country": "BR"},
        status=mapped_status,
        created_at=datetime.now(timezone.utc).isoformat(),
        is_suspicious=is_suspicious,
    )


def make_click_event(session_id: str, data: dict) -> ClickEvent:
    """Build a clickstream event using a real product from the catalog."""
    product = random.choice(data["products"])
    customer = random.choice(data["customers"]) if random.random() > 0.3 else None
    return ClickEvent(
        event_type="PAGE_VIEW",
        event_id=str(uuid.uuid4()),
        session_id=session_id,
        customer_id=customer["customer_id"] if customer else None,
        product_id=product["product_id"],
        action=random.choice(["view", "view", "view", "add_to_cart", "remove_from_cart", "wishlist"]),
        page=f"/products/{product['product_id']}",
        referrer=random.choice(["google", "facebook", "direct", "email", "instagram", ""]),
        device=random.choice(["desktop", "mobile", "tablet"]),
        timestamp=datetime.now(timezone.utc).isoformat(),
        duration_ms=random.randint(500, 60000),
    )


def make_inventory_event(data: dict) -> InventoryEvent:
    """Build an inventory event using a real product and seller as warehouse."""
    product = random.choice(data["products"])
    delta = random.choice([-5, -3, -1, -1, -1, 10, 20, 50])
    previous = random.randint(10, 300)
    current = max(0, previous + delta)
    warehouse = random.choice(data["warehouse_ids"]) if data["warehouse_ids"] else "WH-01"
    return InventoryEvent(
        event_type="INVENTORY_UPDATE",
        event_id=str(uuid.uuid4()),
        product_id=product["product_id"],
        category=product["category"],
        previous_stock=previous,
        current_stock=current,
        delta=delta,
        warehouse_id=warehouse,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class EcommerceGenerator:
    def __init__(self, region: str, orders_queue: str, click_queue: str,
                 inventory_queue: str, dataset_path: str):
        self.sqs = boto3.client("sqs", region_name=region)
        self.orders_queue    = orders_queue
        self.click_queue     = click_queue
        self.inventory_queue = inventory_queue
        self.data = load_olist_data(dataset_path)
        self.stats = {"orders": 0, "clicks": 0, "inventory": 0, "errors": 0}

    def _send(self, queue_url: str, event: dict):
        try:
            self.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(event),
            )
        except Exception as e:
            log.error("Failed to send message to %s: %s", queue_url, e)
            self.stats["errors"] += 1

    def emit_order(self):
        event = make_order_event(self.data)
        if event is None:
            return
        self._send(self.orders_queue, asdict(event))
        self.stats["orders"] += 1

    def emit_clicks(self, session_id: str, n: int = 3):
        for _ in range(n):
            event = make_click_event(session_id, self.data)
            self._send(self.click_queue, asdict(event))
            self.stats["clicks"] += 1

    def emit_inventory(self):
        event = make_inventory_event(self.data)
        self._send(self.inventory_queue, asdict(event))
        self.stats["inventory"] += 1

    def run(self, orders_per_sec: float = 2.0, duration_sec: Optional[int] = None):
        log.info("Starting Olist-powered generator — %.1f orders/sec", orders_per_sec)
        interval = 1.0 / orders_per_sec
        start = time.time()
        session_pool = [str(uuid.uuid4()) for _ in range(50)]
        tick = 0

        while True:
            if duration_sec and (time.time() - start) >= duration_sec:
                break

            self.emit_order()

            if random.random() < 0.8:
                session = random.choice(session_pool)
                self.emit_clicks(session, n=random.randint(1, 5))

            if tick % 10 == 0:
                self.emit_inventory()

            tick += 1
            if tick % 50 == 0:
                elapsed = time.time() - start
                log.info(
                    "t=%.0fs | orders=%d clicks=%d inventory=%d errors=%d",
                    elapsed, self.stats["orders"], self.stats["clicks"],
                    self.stats["inventory"], self.stats["errors"],
                )

            time.sleep(interval)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist-powered ecommerce event generator")
    parser.add_argument("--region",          default="eu-north-1")
    parser.add_argument("--orders-queue",    default="ecom-orders-queue", help="SQS queue name or URL")
    parser.add_argument("--click-queue",     default="ecom-clickstream-queue", help="SQS queue name or URL")
    parser.add_argument("--inventory-queue", default="ecom-inventory-queue", help="SQS queue name or URL")
    parser.add_argument("--rate",            type=float, default=2.0, help="Orders per second")
    parser.add_argument("--duration",        type=int,   default=None, help="Run for N seconds then stop")
    parser.add_argument("--dataset-path",    default="D:/Dataset_AWS", help="Path to Olist CSV files")
    args = parser.parse_args()

    # Resolve queue names to URLs if needed
    sqs_client = boto3.client("sqs", region_name=args.region)
    def resolve_queue_url(name_or_url: str) -> str:
        if name_or_url.startswith("https://"):
            return name_or_url
        return sqs_client.get_queue_url(QueueName=name_or_url)["QueueUrl"]

    gen = EcommerceGenerator(
        region=args.region,
        orders_queue=resolve_queue_url(args.orders_queue),
        click_queue=resolve_queue_url(args.click_queue),
        inventory_queue=resolve_queue_url(args.inventory_queue),
        dataset_path=args.dataset_path,
    )
    gen.run(orders_per_sec=args.rate, duration_sec=args.duration)
