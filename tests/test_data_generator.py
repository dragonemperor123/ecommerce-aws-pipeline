"""Tests for the ecommerce data generator (no AWS calls needed)."""
import os
import pytest
from data_generator.generator import (
    make_order_event,
    make_click_event,
    make_inventory_event,
    PRODUCTS,
    CUSTOMERS,
)


def test_order_event_structure():
    order = make_order_event()
    assert order.order_id.startswith("ORD-")
    assert order.customer_id.startswith("CUST-")
    assert order.total >= 0
    assert order.total == round(order.subtotal - order.discount, 2)
    assert len(order.items) >= 1
    assert order.payment_method in {"credit_card", "debit_card", "paypal", "crypto", "bank_transfer"}
    assert order.status in {"confirmed", "pending", "failed"}
    assert order.shipping_address["country"] == "US"


def test_order_items_are_valid():
    for _ in range(20):
        order = make_order_event()
        for item in order.items:
            assert "product_id" in item
            assert "unit_price" in item
            assert item["quantity"] >= 1
            assert item["unit_price"] > 0


def test_order_total_matches_items():
    for _ in range(50):
        order = make_order_event()
        expected_subtotal = sum(i["quantity"] * i["unit_price"] for i in order.items)
        assert abs(order.subtotal - round(expected_subtotal, 2)) < 0.01


def test_click_event_structure():
    click = make_click_event(session_id="sess-test-001")
    assert click.session_id == "sess-test-001"
    assert click.product_id.startswith("PROD-")
    assert click.action in {"view", "add_to_cart", "remove_from_cart", "wishlist"}
    assert click.device in {"desktop", "mobile", "tablet"}
    assert click.duration_ms > 0


def test_inventory_event_structure():
    inv = make_inventory_event()
    assert inv.product_id.startswith("PROD-")
    assert inv.event_type == "INVENTORY_UPDATE"
    assert inv.current_stock >= 0  # never negative
    assert inv.warehouse_id.startswith("WH-")
    assert inv.delta != 0


def test_inventory_stock_never_negative():
    for _ in range(100):
        inv = make_inventory_event()
        assert inv.current_stock >= 0


def test_fraud_flags_for_high_value_order():
    os.environ.setdefault("ORDERS_TABLE", "dummy")
    os.environ.setdefault("FRAUD_TOPIC_ARN", "dummy")
    os.environ.setdefault("ORDER_EVENTS_TOPIC_ARN", "dummy")
    os.environ.setdefault("PRODUCTS_TABLE", "dummy")
    os.environ.setdefault("SESSIONS_TABLE", "dummy")
    os.environ.setdefault("RAW_BUCKET", "dummy")
    os.environ.setdefault("PROCESSED_BUCKET", "dummy")
    os.environ.setdefault("LOW_INVENTORY_TOPIC_ARN", "dummy")
    from lambdas.order_processor.handler import score_fraud

    order = {
        "total": 1500.0,
        "customer_tier": "bronze",
        "payment_method": "crypto",
        "is_suspicious": True,
        "items": [{"product_id": "P1"}, {"product_id": "P2"}, {"product_id": "P3"}, {"product_id": "P4"}],
    }
    result = score_fraud(order)
    assert result["score"] >= 0.5
    assert result["is_fraud"] is True
    assert len(result["flags"]) > 0


def test_no_fraud_flags_for_normal_order():
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
