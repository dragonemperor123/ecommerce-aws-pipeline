"""
SageMaker Inference Script
Serves the ALS recommendation model.
"""
import json
import os
import joblib
import numpy as np


def model_fn(model_dir):
    model = joblib.load(os.path.join(model_dir, "model.joblib"))
    customer_idx = joblib.load(os.path.join(model_dir, "customer_idx.joblib"))
    product_idx = joblib.load(os.path.join(model_dir, "product_idx.joblib"))
    reverse_product_idx = joblib.load(os.path.join(model_dir, "reverse_product_idx.joblib"))
    return {
        "model": model,
        "customer_idx": customer_idx,
        "product_idx": product_idx,
        "reverse_product_idx": reverse_product_idx,
    }


def input_fn(request_body, content_type="application/json"):
    return json.loads(request_body)


def predict_fn(input_data, model_artifacts):
    model = model_artifacts["model"]
    customer_idx = model_artifacts["customer_idx"]
    reverse_product_idx = model_artifacts["reverse_product_idx"]

    customer_id = input_data.get("customer_id")
    n = int(input_data.get("n", 5))

    if customer_id not in customer_idx:
        # Cold start: return globally popular items (by factor norms)
        item_norms = np.linalg.norm(model.item_factors, axis=1)
        top_indices = np.argsort(item_norms)[::-1][:n]
        recommendations = [
            {"product_id": reverse_product_idx[i], "score": float(item_norms[i]), "rank": rank + 1}
            for rank, i in enumerate(top_indices)
            if i in reverse_product_idx
        ]
    else:
        uid = customer_idx[customer_id]
        ids, scores = model.recommend(uid, model.user_factors[uid], N=n, filter_already_liked_items=True)
        recommendations = [
            {"product_id": reverse_product_idx[int(iid)], "score": float(score), "rank": rank + 1}
            for rank, (iid, score) in enumerate(zip(ids, scores))
            if int(iid) in reverse_product_idx
        ]

    return {"recommendations": recommendations}


def output_fn(prediction, accept="application/json"):
    return json.dumps(prediction), accept
