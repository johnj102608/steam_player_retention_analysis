from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List

import joblib
import numpy as np
import pandas as pd

from config import CFG


def normalize_tag(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)
    return s.strip()


def safe_split_tags(x: Any) -> List[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    if isinstance(x, list):
        return [str(i) for i in x if str(i).strip()]
    return [s.strip() for s in str(x).split(",") if s.strip()]


def build_vocab_maps(vocab: List[str]):
    norm_to_orig = {}
    for v in vocab:
        nv = normalize_tag(v)
        if nv and nv not in norm_to_orig:
            norm_to_orig[nv] = v
    return norm_to_orig


def encode_multihot(raw_items, norm_map, vocab_list, prefix):
    active = set()
    for t in raw_items:
        nt = normalize_tag(t)
        if nt in norm_map:
            active.add(norm_map[nt])
    return {f"{prefix}{v}": int(v in active) for v in vocab_list}


def safe_log(x: float) -> float:
    if x <= 0:
        raise ValueError("All avg/peak inputs must be > 0.")
    return float(math.log(float(x)))


def build_feature_row(row: pd.Series, vocab_payload: dict) -> Dict[str, Any]:

    required = ["avg_m1", "avg_m2", "avg_m3", "peak_m1", "peak_m2", "peak_m3", "is_free", "price_usd"]
    for c in required:
        if c not in row:
            raise ValueError(f"Missing column: {c}")

    a_km2 = float(row["avg_m1"])
    a_km1 = float(row["avg_m2"])
    a_k = float(row["avg_m3"])

    p_km2 = float(row["peak_m1"])
    p_km1 = float(row["peak_m2"])
    p_k = float(row["peak_m3"])

    if min(a_km2, a_km1, a_k, p_km2, p_km1, p_k) <= 0:
        raise ValueError("All avg/peak values must be > 0.")

    log_avg_km2 = safe_log(a_km2)
    log_avg_km1 = safe_log(a_km1)
    log_avg_k = safe_log(a_k)

    log_peak_km2 = safe_log(p_km2)
    log_peak_km1 = safe_log(p_km1)
    log_peak_k = safe_log(p_k)

    feat = {
        # lags
        "avg_k": a_k,
        "avg_km1": a_km1,
        "avg_km2": a_km2,
        "peak_k": p_k,
        "peak_km1": p_km1,
        "peak_km2": p_km2,

        # growth
        "avg_growth_k": (a_k / a_km1) - 1.0,
        "avg_growth_km1": (a_km1 / a_km2) - 1.0,
        "peak_growth_k": (p_k / p_km1) - 1.0,
        "peak_growth_km1": (p_km1 / p_km2) - 1.0,

        # ratios
        "avg_over_peak_k": a_k / p_k,
        "avg_over_peak_km1": a_km1 / p_km1,

        # log regime features
        "log_avg_k": log_avg_k,
        "log_peak_k": log_peak_k,
        "trend3_log_avg": log_avg_k - log_avg_km2,
        "trend3_log_peak": log_peak_k - log_peak_km2,
        "curv_log_avg": (log_avg_k - log_avg_km1) - (log_avg_km1 - log_avg_km2),
        "curv_log_peak": (log_peak_k - log_peak_km1) - (log_peak_km1 - log_peak_km2),
        "vol3_log_avg": float(np.std([log_avg_km2, log_avg_km1, log_avg_k], ddof=0)),
        "vol3_log_peak": float(np.std([log_peak_km2, log_peak_km1, log_peak_k], ddof=0)),
        "shock_log_peak_avg": log_peak_k - log_avg_k,

        # static
        "is_free": int(row["is_free"]),
        "price_usd": float(row["price_usd"]) if not pd.isna(row["price_usd"]) else np.nan,

        "age_months": float(row.get("age_months", np.nan)),
        "age_since_release_months": float(row.get("age_since_release_months", np.nan)),
    }

    # tags
    genre_vocab = vocab_payload.get("genre_vocab", [])
    cat_vocab = vocab_payload.get("cat_vocab", [])

    genre_map = build_vocab_maps(genre_vocab)
    cat_map = build_vocab_maps(cat_vocab)

    feat.update(encode_multihot(
        safe_split_tags(row.get("genres", "")),
        genre_map,
        genre_vocab,
        "genre_"
    ))

    feat.update(encode_multihot(
        safe_split_tags(row.get("categories", "")),
        cat_map,
        cat_vocab,
        "cat_"
    ))

    return feat


def main():

    input_path = Path("predict_input.csv")
    output_path = Path("predict_output.csv")

    model_dir = CFG.processed_dir / "models_quantile_delta_weighted"
    vocab_path = CFG.processed_dir / "tag_vocab.json"

    if not input_path.exists():
        raise FileNotFoundError("Missing predict_input.csv")

    if not vocab_path.exists():
        raise FileNotFoundError("Missing tag_vocab.json")

    model_q10 = joblib.load(model_dir / "quantile_delta_weighted_q10.pkl")["model"]
    model_q50 = joblib.load(model_dir / "quantile_delta_weighted_q50.pkl")["model"]
    model_q90 = joblib.load(model_dir / "quantile_delta_weighted_q90.pkl")["model"]

    vocab_payload = json.loads(vocab_path.read_text(encoding="utf-8"))
    df_in = pd.read_csv(input_path)

    features = []
    errors = []

    for _, r in df_in.iterrows():
        try:
            features.append(build_feature_row(r, vocab_payload))
            errors.append("")
        except Exception as e:
            features.append({})
            errors.append(str(e))

    X = pd.DataFrame(features)

    # align columns
    expected_cols = model_q50.named_steps["pre"].transformers_[0][2]
    for c in expected_cols:
        if c not in X:
            X[c] = np.nan
    X = X[expected_cols]

    ok_mask = X.notna().any(axis=1)

    base_log = np.log(np.clip(X["avg_k"].astype(float), 1e-9, None))

    pred_q10 = np.full(len(X), np.nan)
    pred_q50 = np.full(len(X), np.nan)
    pred_q90 = np.full(len(X), np.nan)

    if ok_mask.any():
        X_ok = X[ok_mask]

        delta_q10 = model_q10.predict(X_ok)
        delta_q50 = model_q50.predict(X_ok)
        delta_q90 = model_q90.predict(X_ok)

        pred_q10[ok_mask] = np.exp(base_log[ok_mask] + delta_q10)
        pred_q50[ok_mask] = np.exp(base_log[ok_mask] + delta_q50)
        pred_q90[ok_mask] = np.exp(base_log[ok_mask] + delta_q90)

    df_out = df_in.copy()
    df_out["pred_p10_players"] = pred_q10
    df_out["pred_p50_players"] = pred_q50
    df_out["pred_p90_players"] = pred_q90
    df_out["row_error"] = errors

    df_out.to_csv(output_path, index=False)
    print(f"Wrote predictions: {output_path}")


if __name__ == "__main__":
    main()
