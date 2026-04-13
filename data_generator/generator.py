"""
Ecommerce Event Generator
Simulates realistic order, clickstream, and inventory events
and pumps them into Kinesis Data Streams.
"""
import json
import random
import time
import uuid
import argparse
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional

import boto3
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

fake = Faker()

CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Beauty", "Toys", "Food"]
PRODUCTS = [
    {"product_id": f"PROD-{i:04d}", "name": fake.catch_phrase(), "category": random.choice(CATEGORIES),
     "price": round(random.uniform(5, 500), 2), "stock": random.randint(0, 500)}
    for i in range(1, 201)
]
CUSTOMERS = [
    {"customer_id": f"CUST-{i:06d}", "email": fake.email(), "tier": random.choice(["bronze", "silver", "gold", "platinum"])}
    for i in range(1, 1001)
]

# Fraud signals: small % of customers are "risky"
RISKY_CUSTOMERS = {c["customer_id"] for c in random.sample(CUSTOMERS, 30)}


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
    product_name: str
    category: str
    previous_stock: int
    current_stock: int
    delta: int
    warehouse_id: str
    timestamp: str


def make_order_event() -> OrderEvent:
    customer = random.choice(CUSTOMERS)
    n_items = random.randint(1, 5)
    selected = random.sample(PRODUCTS, n_items)
    items = [
        {
            "product_id": p["product_id"],
            "name": p["name"],
            "category": p["category"],
            "quantity": random.randint(1, 3),
            "unit_price": p["price"],
        }
        for p in selected
    ]
    subtotal = sum(i["quantity"] * i["unit_price"] for i in items)
    discount = round(subtotal * random.choice([0, 0, 0, 0.05, 0.10, 0.15]), 2)
    total = round(subtotal - discount, 2)

    is_suspicious = (
        customer["customer_id"] in RISKY_CUSTOMERS
        or total > 800
        or (customer["tier"] == "bronze" and total > 300)
    )

    return OrderEvent(
        event_type="ORDER_PLACED",
        event_id=str(uuid.uuid4()),
        order_id=f"ORD-{uuid.uuid4().hex[:10].upper()}",
        customer_id=customer["customer_id"],
        customer_tier=customer["tier"],
        items=items,
        subtotal=round(subtotal, 2),
        discount=discount,
        total=total,
        payment_method=random.choice(["credit_card", "debit_card", "paypal", "crypto", "bank_transfer"]),
        shipping_address={
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            "country": "US",
        },
        status=random.choice(["confirmed", "confirmed", "confirmed", "pending", "failed"]),
        created_at=datetime.now(timezone.utc).isoformat(),
        is_suspicious=is_suspicious,
    )


def make_click_event(session_id: str) -> ClickEvent:
    product = random.choice(PRODUCTS)
    customer = random.choice(CUSTOMERS) if random.random() > 0.3 else None
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


def make_inventory_event() -> InventoryEvent:
    product = random.choice(PRODUCTS)
    delta = random.choice([-5, -3, -1, -1, -1, 10, 20, 50])
    previous = product["stock"]
    product["stock"] = max(0, previous + delta)
    return InventoryEvent(
        event_type="INVENTORY_UPDATE",
        event_id=str(uuid.uuid4()),
        product_id=product["product_id"],
        product_name=product["name"],
        category=product["category"],
        previous_stock=previous,
        current_stock=product["stock"],
        delta=delta,
        warehouse_id=f"WH-{random.randint(1, 5):02d}",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


class EcommerceGenerator:
    def __init__(self, region: str, orders_stream: str, click_stream: str, inventory_stream: str):
        self.kinesis = boto3.client("kinesis", region_name=region)
        self.orders_stream = orders_stream
        self.click_stream = click_stream
        self.inventory_stream = inventory_stream
        self.stats = {"orders": 0, "clicks": 0, "inventory": 0, "errors": 0}

    def _put(self, stream: str, event: dict, partition_key: str):
        try:
            self.kinesis.put_record(
                StreamName=stream,
                Data=json.dumps(event).encode(),
                PartitionKey=partition_key,
            )
        except Exception as e:
            log.error("Failed to put record to %s: %s", stream, e)
            self.stats["errors"] += 1

    def emit_order(self):
        event = make_order_event()
        self._put(self.orders_stream, asdict(event), event.customer_id)
        self.stats["orders"] += 1

    def emit_clicks(self, session_id: str, n: int = 3):
        for _ in range(n):
            event = make_click_event(session_id)
            self._put(self.click_stream, asdict(event), event.session_id)
            self.stats["clicks"] += 1

    def emit_inventory(self):
        event = make_inventory_event()
        self._put(self.inventory_stream, asdict(event), event.product_id)
        self.stats["inventory"] += 1

    def run(self, orders_per_sec: float = 2.0, duration_sec: Optional[int] = None):
        log.info("Starting generator — %.1f orders/sec", orders_per_sec)
        interval = 1.0 / orders_per_sec
        start = time.time()
        session_pool = [str(uuid.uuid4()) for _ in range(50)]
        tick = 0

        while True:
            if duration_sec and (time.time() - start) >= duration_sec:
                break

            self.emit_order()

            # ~3 click events per order on average
            if random.random() < 0.8:
                session = random.choice(session_pool)
                self.emit_clicks(session, n=random.randint(1, 5))

            # Inventory update every ~10 orders
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ecommerce event generator")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--orders-stream", default="ecom-orders-stream")
    parser.add_argument("--click-stream", default="ecom-clickstream")
    parser.add_argument("--inventory-stream", default="ecom-inventory-stream")
    parser.add_argument("--rate", type=float, default=2.0, help="Orders per second")
    parser.add_argument("--duration", type=int, default=None, help="Run for N seconds then stop")
    args = parser.parse_args()

    gen = EcommerceGenerator(
        region=args.region,
        orders_stream=args.orders_stream,
        click_stream=args.click_stream,
        inventory_stream=args.inventory_stream,
    )
    gen.run(orders_per_sec=args.rate, duration_sec=args.duration)
