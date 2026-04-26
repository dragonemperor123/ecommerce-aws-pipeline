"""
Tests for the Olist-powered data generator.
All tests use an in-memory `data` dict instead of loading real CSVs,
so they run without the D:/Dataset_AWS files present.
"""
import os
import pytest
import pandas as pd

from data_generator.generator import (
    make_order_event,
    make_click_event,
    make_inventory_event,
    PAYMENT_MAP,
    FRAUD_RULES,
)


# ---------------------------------------------------------------------------
# Minimal test data that mirrors what load_olist_data() returns
# ---------------------------------------------------------------------------

def make_test_data():
    products = [
        {"product_id": "abc123", "category": "health_beauty",          "unit_price": 49.90},
        {"product_id": "def456", "category": "computers_accessories",   "unit_price": 120.0},
        {"product_id": "ghi789", "category": "bed_bath_table",          "unit_price": 75.0},
    ]
    customers = [
        {"customer_id": "cust-aaa", "customer_unique_id": "uniq-aaa",
         "customer_city": "sao paulo",     "customer_state": "SP", "tier": "gold"},
        {"customer_id": "cust-bbb", "customer_unique_id": "uniq-bbb",
         "customer_city": "rio de janeiro", "customer_state": "RJ", "tier": "bronze"},
        {"customer_id": "cust-ccc", "customer_unique_id": "uniq-ccc",
         "customer_city": "belo horizonte", "customer_state": "MG", "tier": "platinum"},
    ]
    enriched_orders = pd.DataFrame([
        {"order_id": "ord-111", "customer_id": "cust-aaa",
         "order_status": "delivered", "payment_type": "credit_card"},
        {"order_id": "ord-222", "customer_id": "cust-bbb",
         "order_status": "delivered", "payment_type": "boleto"},
        {"order_id": "ord-333", "customer_id": "cust-ccc",
         "order_status": "delivered", "payment_type": "credit_card"},
    ])
    order_items_grouped = {
        "ord-111": [{"product_id": "abc123", "price": 49.90, "freight_value": 5.0}],
        "ord-222": [{"product_id": "def456", "price": 120.0, "freight_value": 15.0}],
        "ord-333": [{"product_id": "ghi789", "price": 75.0,  "freight_value": 8.0}],
    }
    warehouse_ids = ["seller-001", "seller-002", "seller-003"]
    return {
        "products": products,
        "customers": customers,
        "enriched_orders": enriched_orders,
        "order_items_grouped": order_items_grouped,
        "warehouse_ids": warehouse_ids,
    }


# ---------------------------------------------------------------------------
# Order event tests
# ---------------------------------------------------------------------------

def test_order_event_structure():
    data = make_test_data()
    order = make_order_event(data)
    assert order is not None
    assert order.order_id.startswith("ORD-")
    assert order.total >= 0
    assert abs(order.total - round(order.subtotal - order.discount, 2)) < 0.01
    assert len(order.items) >= 1
    assert order.payment_method in {"credit_card", "debit_card", "paypal", "bank_transfer", "crypto"}
    assert order.status in {"confirmed", "pending", "failed"}
    assert order.shipping_address["country"] == "BR"


def test_order_items_valid():
    data = make_test_data()
    for _ in range(10):
        order = make_order_event(data)
        if order is None:
            continue
        for item in order.items:
            assert "product_id" in item
            assert "unit_price" in item
            assert item["unit_price"] > 0
            assert "freight" in item


def test_order_total_matches_items():
    data = make_test_data()
    for _ in range(20):
        order = make_order_event(data)
        if order is None:
            continue
        expected = sum(i["unit_price"] for i in order.items)
        assert abs(order.subtotal - round(expected, 2)) < 0.01


def test_order_shipping_address_has_state():
    data = make_test_data()
    for _ in range(10):
        order = make_order_event(data)
        if order is None:
            continue
        assert "state" in order.shipping_address
        assert "city" in order.shipping_address


def test_boleto_maps_to_bank_transfer():
    """boleto orders should appear as bank_transfer, not flagged as suspicious payment."""
    assert PAYMENT_MAP["boleto"] == "bank_transfer"
    assert "bank_transfer" not in FRAUD_RULES["suspicious_payment"]


def test_high_value_order_is_suspicious():
    data = make_test_data()
    # Inject a high-value order directly
    data["enriched_orders"] = pd.DataFrame([{
        "order_id": "ord-big", "customer_id": "cust-bbb",
        "order_status": "delivered", "payment_type": "credit_card",
    }])
    data["order_items_grouped"] = {
        "ord-big": [
            {"product_id": "abc123", "price": 300.0, "freight_value": 10.0},
            {"product_id": "def456", "price": 300.0, "freight_value": 10.0},
        ]
    }
    order = make_order_event(data)
    assert order is not None
    # subtotal = 600 > high_value_threshold(500), bronze tier > bronze_threshold(200)
    assert order.is_suspicious is True


# ---------------------------------------------------------------------------
# Click event tests
# ---------------------------------------------------------------------------

def test_click_event_structure():
    data = make_test_data()
    click = make_click_event("sess-test-001", data)
    assert click.session_id == "sess-test-001"
    assert click.product_id in {p["product_id"] for p in data["products"]}
    assert click.action in {"view", "add_to_cart", "remove_from_cart", "wishlist"}
    assert click.device in {"desktop", "mobile", "tablet"}
    assert click.duration_ms > 0


def test_click_event_may_be_anonymous():
    data = make_test_data()
    # Run many times — ~30% should be anonymous
    anonymous = sum(
        1 for _ in range(50)
        if make_click_event("sess-anon", data).customer_id is None
    )
    assert anonymous > 0


# ---------------------------------------------------------------------------
# Inventory event tests
# ---------------------------------------------------------------------------

def test_inventory_event_structure():
    data = make_test_data()
    inv = make_inventory_event(data)
    assert inv.product_id in {p["product_id"] for p in data["products"]}
    assert inv.event_type == "INVENTORY_UPDATE"
    assert inv.current_stock >= 0
    assert inv.warehouse_id in data["warehouse_ids"]
    assert inv.delta != 0


def test_inventory_stock_never_negative():
    data = make_test_data()
    for _ in range(50):
        inv = make_inventory_event(data)
        assert inv.current_stock >= 0


def test_inventory_category_is_real():
    data = make_test_data()
    valid_categories = {p["category"] for p in data["products"]}
    for _ in range(20):
        inv = make_inventory_event(data)
        assert inv.category in valid_categories


# ---------------------------------------------------------------------------
# Fraud scoring tests (via order_processor)
# ---------------------------------------------------------------------------

def test_fraud_score_high_value_crypto():
    os.environ.setdefault("ORDERS_TABLE",            "dummy")
    os.environ.setdefault("FRAUD_TOPIC_ARN",          "dummy")
    os.environ.setdefault("ORDER_EVENTS_TOPIC_ARN",   "dummy")
    from lambdas.order_processor.handler import score_fraud

    order = {
        "total": 600.0,           # > high_value_threshold (500)
        "customer_tier": "bronze",
        "payment_method": "crypto",
        "is_suspicious": True,
        "items": [{"product_id": "P1"}, {"product_id": "P2"}, {"product_id": "P3"}, {"product_id": "P4"}],
    }
    result = score_fraud(order)
    assert result["score"] >= 0.5
    assert result["is_fraud"] is True
    assert len(result["flags"]) > 0


def test_no_fraud_for_normal_order():
    from lambdas.order_processor.handler import score_fraud

    order = {
        "total": 45.0,
        "customer_tier": "gold",
        "payment_method": "credit_card",
        "is_suspicious": False,
        "items": [{"product_id": "P1"}],
    }
    result = score_fraud(order)
    assert result["score"] < 0.5
    assert result["is_fraud"] is False


def test_boleto_not_flagged_as_fraud():
    """bank_transfer (boleto) must NOT raise the unusual_payment_method flag."""
    from lambdas.order_processor.handler import score_fraud

    order = {
        "total": 150.0,
        "customer_tier": "silver",
        "payment_method": "bank_transfer",  # boleto
        "is_suspicious": False,
        "items": [{"product_id": "P1"}],
    }
    result = score_fraud(order)
    assert "unusual_payment_method" not in result["flags"]
    assert result["is_fraud"] is False
