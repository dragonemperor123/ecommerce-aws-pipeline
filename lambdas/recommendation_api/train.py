"""
Recommendation Model Training Script
- Trains an ALS collaborative filtering model on product interactions
- Can load interactions directly from Olist CSVs (local/dev) OR from S3 Parquet (SageMaker)
- Saves model artifact to S3 for SageMaker deployment

Usage (local, from Olist CSVs):
    python train.py --dataset-path D:/Dataset_AWS --output-s3 s3://my-bucket/models/reco

Usage (SageMaker, from curated S3 Parquet):
    python train.py --interactions-path s3://my-bucket/curated/product_interactions/ --output-s3 s3://my-bucket/models/reco
"""
import argparse
import logging
import os
import tarfile
import tempfile

import boto3
import numpy as np
import pandas as pd
from implicit import als
from scipy.sparse import csr_matrix
import joblib

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_interactions_from_olist(dataset_path: str) -> pd.DataFrame:
    """
    Build a customer-product interaction matrix directly from Olist CSVs.
    Uses customer_unique_id (stable across orders) instead of customer_id
    so repeat customers are correctly identified.
    """
    log.info("Loading Olist CSVs from %s ...", dataset_path)
    order_items = pd.read_csv(os.path.join(dataset_path, "olist_order_items_dataset.csv"))
    orders      = pd.read_csv(os.path.join(dataset_path, "olist_orders_dataset.csv"))
    customers   = pd.read_csv(os.path.join(dataset_path, "olist_customers_dataset.csv"))

    # Keep only delivered/confirmed orders for clean signal
    confirmed_statuses = {"delivered", "shipped", "approved", "invoiced"}
    orders = orders[orders["order_status"].isin(confirmed_statuses)]

    # Join: order_items → orders → customers (to get customer_unique_id)
    df = order_items.merge(orders[["order_id", "customer_id"]], on="order_id", how="inner")
    df = df.merge(customers[["customer_id", "customer_unique_id"]], on="customer_id", how="inner")

    # Aggregate to one row per (customer_unique_id, product_id)
    interactions = (
        df.groupby(["customer_unique_id", "product_id"])
        .agg(
            purchase_count=("order_id", "count"),
            total_spend=("price", "sum"),
        )
        .reset_index()
        .rename(columns={"customer_unique_id": "customer_id"})
    )

    # Implicit rating: log-scaled purchase count
    interactions["implicit_rating"] = np.log1p(interactions["purchase_count"]).astype(np.float32)

    log.info(
        "Built %d interactions — %d unique customers, %d unique products",
        len(interactions),
        interactions["customer_id"].nunique(),
        interactions["product_id"].nunique(),
    )
    return interactions[["customer_id", "product_id", "implicit_rating", "purchase_count", "total_spend"]]


def load_interactions_from_s3(s3_path: str) -> pd.DataFrame:
    """Load pre-computed interactions Parquet from S3 (used in SageMaker jobs)."""
    import pyarrow.parquet as pq
    import s3fs
    fs = s3fs.S3FileSystem()
    dataset = pq.ParquetDataset(s3_path, filesystem=fs)
    return dataset.read_pandas().to_pandas()


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------

def build_sparse_matrix(df: pd.DataFrame):
    customer_idx = {cid: i for i, cid in enumerate(df["customer_id"].unique())}
    product_idx  = {pid: i for i, pid in enumerate(df["product_id"].unique())}

    rows = df["customer_id"].map(customer_idx)
    cols = df["product_id"].map(product_idx)
    data = df["implicit_rating"].values.astype(np.float32)

    matrix = csr_matrix((data, (rows, cols)), shape=(len(customer_idx), len(product_idx)))
    return matrix, customer_idx, product_idx


def train(df: pd.DataFrame, output_dir: str, factors: int = 64, iterations: int = 20):
    log.info(
        "Training ALS model on %d interactions (%d customers, %d products) "
        "factors=%d iterations=%d",
        len(df), df["customer_id"].nunique(), df["product_id"].nunique(),
        factors, iterations,
    )

    matrix, customer_idx, product_idx = build_sparse_matrix(df)
    item_user_matrix = matrix.T.tocsr()  # implicit expects (items × users)

    model = als.AlternatingLeastSquares(
        factors=factors,
        iterations=iterations,
        regularization=0.05,
        use_gpu=False,
    )
    model.fit(item_user_matrix)

    reverse_product_idx = {v: k for k, v in product_idx.items()}

    os.makedirs(output_dir, exist_ok=True)
    joblib.dump(model,               os.path.join(output_dir, "model.joblib"))
    joblib.dump(customer_idx,        os.path.join(output_dir, "customer_idx.joblib"))
    joblib.dump(product_idx,         os.path.join(output_dir, "product_idx.joblib"))
    joblib.dump(reverse_product_idx, os.path.join(output_dir, "reverse_product_idx.joblib"))

    # We fit on item_user_matrix (items×users), so implicit's internal naming is
    # inverted: model.user_factors = product latent vectors (n_items × factors),
    # model.item_factors = customer latent vectors (n_users × factors).
    np.save(os.path.join(output_dir, "user_factors.npy"),  model.item_factors)  # (n_customers, factors)
    np.save(os.path.join(output_dir, "item_factors.npy"),  model.user_factors)  # (n_products,  factors)

    log.info("Model artifacts saved to %s", output_dir)
    return model, customer_idx, product_idx


# ---------------------------------------------------------------------------
# SageMaker packaging
# ---------------------------------------------------------------------------

def package_for_sagemaker(model_dir: str, output_path: str):
    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(model_dir, arcname=".")
    log.info("Packaged model to %s", output_path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train ALS recommendation model")

    # Data source — one of these two is required
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--dataset-path",      help="Path to Olist CSV files (local training)")
    source.add_argument("--interactions-path", help="S3 path to curated product_interactions Parquet")

    parser.add_argument("--output-s3",  required=True, help="s3://bucket/prefix for model upload")
    parser.add_argument("--factors",    type=int, default=64)
    parser.add_argument("--iterations", type=int, default=20)
    args = parser.parse_args()

    # Load interactions
    if args.dataset_path:
        df = load_interactions_from_olist(args.dataset_path)
    else:
        df = load_interactions_from_s3(args.interactions_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        model_dir = os.path.join(tmpdir, "model")
        train(df, model_dir, args.factors, args.iterations)

        tarball = os.path.join(tmpdir, "model.tar.gz")
        package_for_sagemaker(model_dir, tarball)

        s3 = boto3.client("s3")
        bucket, key = args.output_s3.replace("s3://", "").split("/", 1)
        s3.upload_file(tarball, bucket, f"{key}/model.tar.gz")
        log.info("Uploaded model to s3://%s/%s/model.tar.gz", bucket, key)
