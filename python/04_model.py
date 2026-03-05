from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Dict

import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.ensemble import GradientBoostingRegressor

import joblib

from config import CFG, ensure_dirs


@dataclass(frozen=True)
class QuantileConfig:
    data_path: Path = CFG.model_panel_h3_path

    group_col: str = "app_id"
    test_size: float = 0.2
    seed: int = CFG.seed

    # Quantiles
    quantiles: Tuple[float, ...] = (0.10, 0.50, 0.90)

    # Conservative GBDT params
    n_estimators: int = 300
    learning_rate: float = 0.05
    max_depth: int = 3
    min_samples_leaf: int = 30
    subsample: float = 0.8
    random_state: int = CFG.seed

    # If recent 3-month trend is clearly down, weigh the row more during training.
    downtrend_feature: str = "trend3_log_avg"
    downtrend_threshold: float = -0.05   # "clear downtrend" in log-space over 3 months
    downtrend_weight: float = 1.8

    # Outputs
    out_dir: Path = CFG.processed_dir / "models_quantile_delta_weighted"
    split_path: Path = CFG.processed_dir / "split_groups_quantile.json"  # reuse same split for stability


def make_or_load_group_split(
    df: pd.DataFrame,
    group_col: str,
    test_size: float,
    seed: int,
    split_path: Path,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    split_path.parent.mkdir(parents=True, exist_ok=True)
    groups = df[group_col].dropna().astype(int).unique().tolist()

    if split_path.exists():
        payload = json.loads(split_path.read_text(encoding="utf-8"))
        test_groups = set(int(x) for x in payload.get("test_groups", []))
    else:
        splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=seed)
        tr_idx, te_idx = next(splitter.split(df, groups=df[group_col]))
        test_groups = set(df.iloc[te_idx][group_col].dropna().astype(int).unique().tolist())
        payload = {
            "group_col": group_col,
            "test_size": float(test_size),
            "seed": int(seed),
            "n_groups_total": int(len(groups)),
            "n_test_groups": int(len(test_groups)),
            "test_groups": sorted(list(test_groups)),
        }
        split_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    test_mask = df[group_col].astype(int).isin(test_groups)
    return df[~test_mask].copy(), df[test_mask].copy()


def build_preprocessor(X: pd.DataFrame) -> ColumnTransformer:
    num_cols: List[str] = list(X.columns)
    return ColumnTransformer(
        transformers=[
            ("num", Pipeline([
                ("impute", SimpleImputer(strategy="median")),
            ]), num_cols),
        ],
        remainder="drop",
    )


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def make_sample_weights(X_train: pd.DataFrame, cfg: QuantileConfig) -> np.ndarray:
    """
    Single, simple weighting rule:
      if trend3_log_avg < threshold => weight *= downtrend_weight
    """
    w = np.ones(len(X_train), dtype=float)

    if cfg.downtrend_feature in X_train.columns:
        t = pd.to_numeric(X_train[cfg.downtrend_feature], errors="coerce").to_numpy(dtype=float)
        t = np.nan_to_num(t, nan=0.0)
        w[t < cfg.downtrend_threshold] *= float(cfg.downtrend_weight)

    return w


def main() -> None:
    ensure_dirs()
    cfg = QuantileConfig()
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(cfg.data_path)
    df = df.replace([np.inf, -np.inf], np.nan)

    # Required columns
    df = df.dropna(subset=["app_id", "y_log_avg", "avg_k"]).copy()

    # Base persistence in log space
    df["base_log"] = np.log(np.clip(df["avg_k"].astype(float), 1e-9, None))

    # Target delta relative to persistence
    df["delta"] = df["y_log_avg"].astype(float) - df["base_log"]

    # Stable group split
    train_df, test_df = make_or_load_group_split(
        df, cfg.group_col, cfg.test_size, cfg.seed, cfg.split_path
    )

    drop_cols = [
        "app_id", "k_index",
        "y_avg", "y_peak", "y_log_avg", "y_log_peak",
        "delta", "base_log",
        "release_date_str", "month_k_label",
    ]

    X_train = train_df.drop(columns=drop_cols, errors="ignore")
    X_test = test_df.drop(columns=drop_cols, errors="ignore")
    y_train = train_df["delta"].values.astype(float)
    y_test = test_df["delta"].values.astype(float)

    # Drop cols fully missing in TRAIN
    all_missing = [c for c in X_train.columns if X_train[c].notna().sum() == 0]
    if all_missing:
        X_train = X_train.drop(columns=all_missing)
        X_test = X_test.drop(columns=all_missing, errors="ignore")

    # Coerce to numeric
    for c in X_train.columns:
        if not pd.api.types.is_numeric_dtype(X_train[c]):
            X_train[c] = pd.to_numeric(X_train[c], errors="coerce")
            X_test[c] = pd.to_numeric(X_test[c], errors="coerce")

    pre = build_preprocessor(X_train)
    weights = make_sample_weights(X_train, cfg)

    models: Dict[float, Pipeline] = {}
    preds_delta: Dict[float, np.ndarray] = {}

    print("\n[STEP04 — Quantile Delta Forecasting (Weighted Downtrend Touch)]")
    print(f"Train rows: {len(train_df):,} | Test rows: {len(test_df):,}")
    print(f"Features: {X_train.shape[1]:,}")
    print(f"Split file: {cfg.split_path}")
    if cfg.downtrend_feature in X_train.columns:
        frac_boost = float((pd.to_numeric(X_train[cfg.downtrend_feature], errors='coerce') < cfg.downtrend_threshold).mean())
        print(f"Downtrend weight rule: {cfg.downtrend_feature} < {cfg.downtrend_threshold} => x{cfg.downtrend_weight} (train frac={frac_boost:.3f})")
    else:
        print(f"[WARN] Missing {cfg.downtrend_feature}; weights will be all-ones.")

    for q in cfg.quantiles:
        gbr = GradientBoostingRegressor(
            loss="quantile",
            alpha=float(q),
            n_estimators=cfg.n_estimators,
            learning_rate=cfg.learning_rate,
            max_depth=cfg.max_depth,
            min_samples_leaf=cfg.min_samples_leaf,
            subsample=cfg.subsample,
            random_state=cfg.random_state,
        )

        pipe = Pipeline([
            ("pre", pre),
            ("gbr", gbr),
        ])

        pipe.fit(X_train, y_train, gbr__sample_weight=weights)
        pred = pipe.predict(X_test)

        models[q] = pipe
        preds_delta[q] = pred

        joblib.dump(
            {
                "model": pipe,
                "quantile": float(q),
                "feature_cols": list(X_train.columns),
                "split_path": str(cfg.split_path),
                "target": "delta = y_log_avg - log(avg_k)",
                "weight_rule": {
                    "feature": cfg.downtrend_feature,
                    "threshold": cfg.downtrend_threshold,
                    "multiplier": cfg.downtrend_weight,
                },
            },
            cfg.out_dir / f"quantile_delta_weighted_q{int(q*100):02d}.pkl",
        )

        print(f"Trained & saved q{int(q*100):02d}")

    # Reconstruct forecasts
    base_log_test = test_df["base_log"].values.astype(float)
    y_log_true = test_df["y_log_avg"].values.astype(float)
    yhat_log = {q: base_log_test + preds_delta[q] for q in cfg.quantiles}

    rmse_persist = rmse(y_log_true, base_log_test)
    rmse_q50 = rmse(y_log_true, yhat_log[0.50])

    print("\n[Core RMSE]")
    print(f"Persistence RMSE (log): {rmse_persist:.4f}")
    print(f"q50 Forecast RMSE (log): {rmse_q50:.4f}")
    print(f"ΔRMSE vs persistence (positive=better): {rmse_persist - rmse_q50:.4f}")

    # Quantile calibration
    low, mid, high = yhat_log[0.10], yhat_log[0.50], yhat_log[0.90]
    coverage_10_90 = float(((y_log_true >= low) & (y_log_true <= high)).mean())
    below_10 = float((y_log_true < low).mean())
    below_50 = float((y_log_true < mid).mean())

    print("\n[Quantile Calibration]")
    print(f"Coverage P10–P90 (target ~0.80): {coverage_10_90:.3f}")
    print(f"P(y < P10) (target ~0.10): {below_10:.3f}")
    print(f"P(y < P50) (target ~0.50): {below_50:.3f}")

    # Direction sanity (delta sign)
    pred_dir = (preds_delta[0.50] > 0).astype(int)
    true_dir = (y_test > 0).astype(int)
    dir_acc = float((pred_dir == true_dir).mean())

    print("\n[Directional sanity on delta]")
    print(f"Directional accuracy (q50): {dir_acc:.4f}")
    print(f"Predicted positive rate (q50): {pred_dir.mean():.3f}")
    print(f"True positive rate: {true_dir.mean():.3f}")

    # Downtrend slice
    if all(c in test_df.columns for c in ["avg_km2", "avg_km1", "avg_k"]):
        downtrend = (test_df["avg_km2"] > test_df["avg_km1"]) & (test_df["avg_km1"] > test_df["avg_k"])
        if downtrend.sum() > 0:
            pred_neg = float((preds_delta[0.50][downtrend.values] < 0).mean())
            true_neg = float((y_test[downtrend.values] < 0).mean())
            print("\n[Downtrend slice]")
            print(f"Rows: {int(downtrend.sum())}")
            print(f"Predicted negative % (q50 delta): {pred_neg:.3f}")
            print(f"True negative % (delta): {true_neg:.3f}")
        else:
            print("\n[Downtrend slice] No rows found.")
    else:
        print("\n[Downtrend slice] Missing avg_km2/avg_km1/avg_k columns; skipped.")

    summary = {
        "rmse_persistence_log": rmse_persist,
        "rmse_q50_log": rmse_q50,
        "delta_rmse_vs_persistence": rmse_persist - rmse_q50,
        "coverage_10_90": coverage_10_90,
        "p_below_10": below_10,
        "p_below_50": below_50,
        "directional_acc_q50_delta": dir_acc,
        "pred_pos_rate_q50_delta": float(pred_dir.mean()),
        "true_pos_rate_delta": float(true_dir.mean()),
        "n_train": int(len(train_df)),
        "n_test": int(len(test_df)),
        "n_features": int(X_train.shape[1]),
        "quantiles": [float(q) for q in cfg.quantiles],
        "model_family": "GradientBoostingRegressor(loss=quantile) + downtrend sample_weight",
        "target": "delta = y_log_avg - log(avg_k)",
        "split_path": str(cfg.split_path),
        "weight_rule": {
            "feature": cfg.downtrend_feature,
            "threshold": cfg.downtrend_threshold,
            "multiplier": cfg.downtrend_weight,
        },
    }
    (cfg.out_dir / "metrics_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nSaved metrics: {cfg.out_dir / 'metrics_summary.json'}")
    print(f"Saved models to: {cfg.out_dir}")


if __name__ == "__main__":
    main()