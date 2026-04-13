"""
Recommendation Model Training Script
- Trains an ALS collaborative filtering model on product interactions
- Reads curated product_interactions Parquet from S3
- Saves model artifact to S3 for SageMaker deployment
Run locally or as a SageMaker Training Job.
"""
import argparse
import json
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


def load_interactions(s3_path: str) -> pd.DataFrame:
    import pyarrow.parquet as pq
    import s3fs
    fs = s3fs.S3FileSystem()
    dataset = pq.ParquetDataset(s3_path, filesystem=fs)
    return dataset.read_pandas().to_pandas()


def build_sparse_matrix(df: pd.DataFrame):
    customer_idx = {cid: i for i, cid in enumerate(df["customer_id"].unique())}
    product_idx = {pid: i for i, pid in enumerate(df["product_id"].unique())}

    rows = df["customer_id"].map(customer_idx)
    cols = df["product_id"].map(product_idx)
    data = df["implicit_rating"].values

    matrix = csr_matrix((data, (rows, cols)), shape=(len(customer_idx), len(product_idx)))
    return matrix, customer_idx, product_idx


def train(interactions_path: str, output_dir: str, factors: int = 50, iterations: int = 20):
    log.info("Loading interactions from %s", interactions_path)
    df = load_interactions(interactions_path)
    log.info("Loaded %d interactions for %d customers and %d products",
             len(df), df["customer_id"].nunique(), df["product_id"].nunique())

    matrix, customer_idx, product_idx = build_sparse_matrix(df)
    item_matrix = matrix.T.tocsr()  # implicit expects item-user

    model = als.AlternatingLeastSquares(
        factors=factors,
        iterations=iterations,
        regularization=0.01,
        use_gpu=False,
    )
    log.info("Training ALS model (factors=%d, iterations=%d)...", factors, iterations)
    model.fit(item_matrix)

    reverse_product_idx = {v: k for k, v in product_idx.items()}

    os.makedirs(output_dir, exist_ok=True)
    joblib.dump(model, os.path.join(output_dir, "model.joblib"))
    joblib.dump(customer_idx, os.path.join(output_dir, "customer_idx.joblib"))
    joblib.dump(product_idx, os.path.join(output_dir, "product_idx.joblib"))
    joblib.dump(reverse_product_idx, os.path.join(output_dir, "reverse_product_idx.joblib"))

    log.info("Model saved to %s", output_dir)
    return model, customer_idx, product_idx


def package_for_sagemaker(model_dir: str, output_path: str):
    """Package model artifacts as model.tar.gz for SageMaker."""
    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(model_dir, arcname=".")
    log.info("Packaged model to %s", output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interactions-path", required=True)
    parser.add_argument("--output-s3", required=True)
    parser.add_argument("--factors", type=int, default=50)
    parser.add_argument("--iterations", type=int, default=20)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        model_dir = os.path.join(tmpdir, "model")
        train(args.interactions_path, model_dir, args.factors, args.iterations)

        tarball = os.path.join(tmpdir, "model.tar.gz")
        package_for_sagemaker(model_dir, tarball)

        # Upload to S3
        s3 = boto3.client("s3")
        bucket, key = args.output_s3.replace("s3://", "").split("/", 1)
        s3.upload_file(tarball, bucket, f"{key}/model.tar.gz")
        log.info("Uploaded model to s3://%s/%s/model.tar.gz", bucket, key)
