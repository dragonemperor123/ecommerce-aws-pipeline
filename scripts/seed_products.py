"""
Seed the DynamoDB products table from the Olist product catalog.

Loads product_id, English category, and median unit_price from the Olist CSVs
and writes them into DynamoDB in parallel batches.

Usage:
    python scripts/seed_products.py \
        --table-name <ProductsTableName> \
        --dataset-path D:/Dataset_AWS \
        [--region us-east-1]
"""
import argparse
import logging
import os

import boto3
import pandas as pd
from boto3.dynamodb.types import TypeSerializer
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BATCH_SIZE = 25  # DynamoDB batch_write_item max


def load_product_catalog(dataset_path: str) -> pd.DataFrame:
    log.info("Loading Olist product catalog from %s ...", dataset_path)

    products     = pd.read_csv(os.path.join(dataset_path, "olist_products_dataset.csv"))
    order_items  = pd.read_csv(os.path.join(dataset_path, "olist_order_items_dataset.csv"))
    translations = pd.read_csv(os.path.join(dataset_path, "product_category_name_translation.csv"))

    # Translate categories to English
    products = products.merge(translations, on="product_category_name", how="left")
    products["category"] = (
        products["product_category_name_english"]
        .fillna(products["product_category_name"])
        .fillna("other")
    )

    # Compute median unit price per product from actual transactions
    median_prices = (
        order_items.groupby("product_id")["price"]
        .median()
        .reset_index()
        .rename(columns={"price": "unit_price"})
    )

    catalog = products[["product_id", "category"]].merge(median_prices, on="product_id", how="left")
    catalog["unit_price"] = catalog["unit_price"].fillna(50.0).round(2)
    catalog = catalog.dropna(subset=["product_id"])

    log.info("Loaded %d products", len(catalog))
    return catalog


def seed_table(table_name: str, catalog: pd.DataFrame, region: str):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    records = catalog.to_dict("records")
    total = len(records)
    written = 0

    log.info("Seeding %d products into DynamoDB table '%s' ...", total, table_name)

    with table.batch_writer() as batch:
        for record in records:
            batch.put_item(Item={
                "product_id": record["product_id"],
                "category":   record["category"],
                "unit_price": Decimal(str(record["unit_price"])),
                "stock_level": 0,   # will be updated by inventory events
            })
            written += 1
            if written % 1000 == 0:
                log.info("  %d / %d written ...", written, total)

    log.info("Done — seeded %d products.", written)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed DynamoDB products table from Olist CSVs")
    parser.add_argument("--table-name",   required=True,             help="DynamoDB table name")
    parser.add_argument("--dataset-path", default="D:/Dataset_AWS",  help="Path to Olist CSV files")
    parser.add_argument("--region",       default="us-east-1")
    args = parser.parse_args()

    catalog = load_product_catalog(args.dataset_path)
    seed_table(args.table_name, catalog, args.region)
