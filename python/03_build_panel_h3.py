from __future__ import annotations

import math
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from config import CFG, ensure_dirs

import json


# Config
@dataclass(frozen=True)
class Step03Config:
    metrics_parts_dir: Path = CFG.steamcharts_metrics_parts_dir
    store_meta_path: Path = CFG.store_meta_merged_path
    out_path: Path = CFG.model_panel_h3_path

    horizon: int = CFG.horizon
    min_months_required: int = CFG.min_months_required

    top_k_genres: int = CFG.top_k_genres
    top_k_categories: int = CFG.top_k_categories


def concat_parquets(parts_dir: Path) -> pd.DataFrame:
    parts = sorted(parts_dir.glob("part_*.parquet"))
    if not parts:
        raise FileNotFoundError(f"No parquet parts found in: {parts_dir}")
    dfs = [pd.read_parquet(p) for p in parts]
    return pd.concat(dfs, ignore_index=True)


def safe_list(x) -> List[str]:
    if isinstance(x, list):
        return [str(i) for i in x if str(i).strip()]
    return []


def build_topk_vocab(series: pd.Series, k: int) -> List[str]:
    counts: Dict[str, int] = {}
    for v in series.dropna():
        for item in safe_list(v):
            key = item.strip()
            if not key:
                continue
            counts[key] = counts.get(key, 0) + 1
    top = sorted(counts.items(), key=lambda t: t[1], reverse=True)[:k]
    return [t[0] for t in top]


def encode_multihot(items: List[str], vocab: List[str], prefix: str) -> Dict[str, int]:
    s = set(i.strip() for i in items if isinstance(i, str) and i.strip())
    out: Dict[str, int] = {}
    for v in vocab:
        out[f"{prefix}{v}"] = 1 if v in s else 0
    return out


def extract_month_series(row: pd.Series) -> Tuple[List[str], List[float], List[float]]:
    """
    SteamCharts convention (as scraped):
      month1 = most recent completed month
      month12 = oldest available
    Chronological oldest->newest for panel building.
    """
    labels: List[str] = []
    avgs: List[float] = []
    peaks: List[float] = []

    for i in range(1, CFG.steamcharts_n_months + 1):
        labels.append(row.get(f"month{i}_label"))
        avgs.append(row.get(f"month{i}_avg_players"))
        peaks.append(row.get(f"month{i}_peak_players"))

    # Keep only valid points (avg+peak both present)
    seq = []
    for lab, a, p in zip(labels, avgs, peaks):
        if pd.isna(a) or pd.isna(p):
            continue
        seq.append((lab, float(a), float(p)))

    # Currently newest->oldest; reverse to oldest->newest
    seq = list(reversed(seq))

    out_labels = [x[0] for x in seq]
    out_avgs = [x[1] for x in seq]
    out_peaks = [x[2] for x in seq]
    return out_labels, out_avgs, out_peaks


def safe_log(x: float) -> Optional[float]:
    if x is None:
        return None
    if x <= 0:
        return None
    return float(math.log(x))


def parse_release_date(release_date_str: Optional[str]) -> Optional[pd.Timestamp]:
    """Parse Steam Store release date strings to Timestamp (UTC-naive).
    Examples seen:
      - '23 Feb, 2017'
      - 'Feb 23, 2017'
      - 'Dec 2020'
      - 'Coming Soon' (-> None)
    """
    if not release_date_str or not isinstance(release_date_str, str):
        return None
    s = release_date_str.strip()
    if not s or any(x in s.lower() for x in ["coming soon", "tba", "to be announced"]):
        return None
    # pandas parser is quite robust; keep it simple
    try:
        dt = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
    except Exception:
        dt = pd.NaT
    if pd.isna(dt):
        return None
    # normalize to first day of month for month-based age
    return pd.Timestamp(year=int(dt.year), month=int(dt.month), day=1)


def parse_month_label(label: Optional[str]) -> Optional[pd.Timestamp]:
    """Parse SteamCharts month label like 'February 2024' to first-of-month Timestamp."""
    if not label or not isinstance(label, str):
        return None
    s = label.strip()
    if not s or s.lower() == "last 30 days":
        return None
    # common format: 'February 2024'
    try:
        dt = datetime.strptime(s, "%B %Y")
        return pd.Timestamp(year=dt.year, month=dt.month, day=1)
    except Exception:
        # fallback
        try:
            dt2 = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
        except Exception:
            dt2 = pd.NaT
        if pd.isna(dt2):
            return None
        return pd.Timestamp(year=int(dt2.year), month=int(dt2.month), day=1)


def months_between(start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]) -> Optional[int]:
    """Whole-month difference end - start. Returns None if either missing."""
    if start is None or end is None:
        return None
    return int((end.year - start.year) * 12 + (end.month - start.month))


# Panel row builder
def build_rows_for_app(
    app_id: int,
    labels: List[str],
    avgs: List[float],
    peaks: List[float],
    meta: dict,
    *,
    h: int,
) -> List[dict]:
    """
    Build rows:
      features at k (and k-1, k-2) -> target at k+h
    """
    rows: List[dict] = []
    n = len(avgs)
    # Parse release date once per app (month-granularity)
    release_dt = parse_release_date(meta.get("release_date_str"))


    # Need k-2..k and k+h
    for k in range(2, n - h):
        a_km2, a_km1, a_k = avgs[k - 2], avgs[k - 1], avgs[k]
        p_km2, p_km1, p_k = peaks[k - 2], peaks[k - 1], peaks[k]
        # --- Log-space regime features (scale + trend + curvature + volatility) ---
        log_avg_km2 = math.log(a_km2)
        log_avg_km1 = math.log(a_km1)
        log_avg_k = math.log(a_k)

        log_peak_km2 = math.log(p_km2)
        log_peak_km1 = math.log(p_km1)
        log_peak_k = math.log(p_k)

        # Strength of 3-month trend (log space)
        trend3_log_avg = log_avg_k - log_avg_km2
        trend3_log_peak = log_peak_k - log_peak_km2

        # Curvature: acceleration/deceleration of log trend
        curv_log_avg = (log_avg_k - log_avg_km1) - (log_avg_km1 - log_avg_km2)
        curv_log_peak = (log_peak_k - log_peak_km1) - (log_peak_km1 - log_peak_km2)

        # Volatility over last 3 months (log space)
        vol3_log_avg = float(np.std([log_avg_km2, log_avg_km1, log_avg_k], ddof=0))
        vol3_log_peak = float(np.std([log_peak_km2, log_peak_km1, log_peak_k], ddof=0))

        # Spikiness: peak vs avg in current month (eventiness proxy)
        shock_log_peak_avg = log_peak_k - log_avg_k

        # Real-ish age: months since release (if release date + month label parse ok)
        month_k_dt = parse_month_label(labels[k] if k < len(labels) else None)
        age_since_release_months = months_between(release_dt, month_k_dt)

        a_y = avgs[k + h]
        p_y = peaks[k + h]

        # Require positive for log targets
        if a_k <= 0 or p_k <= 0 or a_y <= 0 or p_y <= 0:
            continue
        if a_km1 <= 0 or a_km2 <= 0 or p_km1 <= 0 or p_km2 <= 0:
            continue

        # Growth features (month-to-month)
        avg_growth_k = (a_k / a_km1) - 1.0 if a_km1 > 0 else None
        avg_growth_km1 = (a_km1 / a_km2) - 1.0 if a_km2 > 0 else None
        peak_growth_k = (p_k / p_km1) - 1.0 if p_km1 > 0 else None
        peak_growth_km1 = (p_km1 / p_km2) - 1.0 if p_km2 > 0 else None

        avg_over_peak_k = (a_k / p_k) if p_k > 0 else None
        avg_over_peak_km1 = (a_km1 / p_km1) if p_km1 > 0 else None

        row = {
            "app_id": int(app_id),
            "k_index": int(k),  # within this app's available history (chronological)
            # Lifecycle proxies
            "age_months": int(k),  # months since first observed month in this app's SteamCharts history
            "age_since_release_months": (None if age_since_release_months is None else int(age_since_release_months)),
            "month_k_label": labels[k] if k < len(labels) else None,

            # Targets
            "y_avg": float(a_y),
            "y_peak": float(p_y),
            "y_log_avg": safe_log(float(a_y)),
            "y_log_peak": safe_log(float(p_y)),

            # Lags
            "avg_k": float(a_k),
            "avg_km1": float(a_km1),
            "avg_km2": float(a_km2),
            "peak_k": float(p_k),
            "peak_km1": float(p_km1),
            "peak_km2": float(p_km2),

            # Growth
            "avg_growth_k": float(avg_growth_k) if avg_growth_k is not None else None,
            "avg_growth_km1": float(avg_growth_km1) if avg_growth_km1 is not None else None,
            "peak_growth_k": float(peak_growth_k) if peak_growth_k is not None else None,
            "peak_growth_km1": float(peak_growth_km1) if peak_growth_km1 is not None else None,

            # Ratios
            "avg_over_peak_k": float(avg_over_peak_k) if avg_over_peak_k is not None else None,
            "avg_over_peak_km1": float(avg_over_peak_km1) if avg_over_peak_km1 is not None else None,

            # Log-scale (helps regime separation)
            "log_avg_k": float(log_avg_k),
            "log_peak_k": float(log_peak_k),

            # Trend strength (log space)
            "trend3_log_avg": float(trend3_log_avg),
            "trend3_log_peak": float(trend3_log_peak),

            # Curvature (accel/decel)
            "curv_log_avg": float(curv_log_avg),
            "curv_log_peak": float(curv_log_peak),

            # Volatility + spikiness
            "vol3_log_avg": float(vol3_log_avg),
            "vol3_log_peak": float(vol3_log_peak),
            "shock_log_peak_avg": float(shock_log_peak_avg),
        }

        # Static meta (already numeric except lists)
        row["is_free"] = int(bool(meta.get("is_free", False)))
        row["price_usd"] = meta.get("price_usd")
        # keep string for analysis; Step04 will drop it
        row["release_date_str"] = meta.get("release_date_str")

        # Multi-hot features are already expanded before calling this function
        row.update({k: v for k, v in meta.items() if k.startswith("genre_") or k.startswith("cat_")})

        rows.append(row)

    return rows


def main() -> None:
    ensure_dirs()
    cfg = Step03Config()

    # Load SteamCharts metrics
    mdf = concat_parquets(cfg.metrics_parts_dir)
    if "status" not in mdf.columns:
        raise ValueError("Metrics parts must include 'status' column.")
    mdf = mdf[mdf["status"] == "ok"].copy()
    mdf = mdf.drop_duplicates(subset=["app_id"], keep="last").reset_index(drop=True)
    print(f"[STEP03] Metrics ok apps: {len(mdf):,}")

    # Load store meta
    if not cfg.store_meta_path.exists():
        raise FileNotFoundError(f"Missing store meta: {cfg.store_meta_path}")
    sdf = pd.read_parquet(cfg.store_meta_path)
    sdf = sdf[sdf.get("status", "ok") == "ok"].copy() if "status" in sdf.columns else sdf.copy()
    sdf = sdf.drop_duplicates(subset=["app_id"], keep="last").reset_index(drop=True)
    print(f"[STEP03] Store meta apps: {len(sdf):,}")

    # Merge (inner join: only apps with BOTH player metrics and store meta)
    df = mdf.merge(sdf, on="app_id", how="inner", suffixes=("", "_meta"))
    print(f"[STEP03] After inner merge: {len(df):,} apps")

    # Build vocab for top-K tags
    # Ensure these are lists (they should be lists from Step02b)
    df["genres"] = df["genres"].apply(safe_list) if "genres" in df.columns else [[] for _ in range(len(df))]
    df["categories"] = df["categories"].apply(safe_list) if "categories" in df.columns else [[] for _ in range(len(df))]

    genre_vocab = build_topk_vocab(df["genres"], cfg.top_k_genres)
    cat_vocab = build_topk_vocab(df["categories"], cfg.top_k_categories)

    print(f"[STEP03] genre_vocab: {len(genre_vocab)} | cat_vocab: {len(cat_vocab)}")

    vocab_path = CFG.processed_dir / "tag_vocab.json"
    vocab_path.write_text(json.dumps({"genre_vocab": genre_vocab, "cat_vocab": cat_vocab}, indent=2), encoding="utf-8")
    print(f"[STEP03] Wrote vocab: {vocab_path}")

    # Build panel rows
    all_rows: List[dict] = []
    skipped_short = 0

    for _, r in df.iterrows():
        app_id = int(r["app_id"])
        labels, avgs, peaks = extract_month_series(r)

        if len(avgs) < cfg.min_months_required:
            skipped_short += 1
            continue

        meta = {
            "is_free": bool(r.get("is_free", False)),
            "price_usd": (None if pd.isna(r.get("price_usd")) else float(r.get("price_usd"))),
            "release_date_str": r.get("release_date_str"),
        }
        meta.update(encode_multihot(r.get("genres", []), genre_vocab, prefix="genre_"))
        meta.update(encode_multihot(r.get("categories", []), cat_vocab, prefix="cat_"))

        rows = build_rows_for_app(app_id, labels, avgs, peaks, meta, h=cfg.horizon)
        all_rows.extend(rows)

    out = pd.DataFrame(all_rows)
    print(f"[STEP03] Built panel rows: {len(out):,}")
    print(f"[STEP03] Skipped apps (<{cfg.min_months_required} months): {skipped_short:,}")

    # --- Force numeric dtypes for engineered numeric features ---
    for c in [
        "age_since_release_months",
        "vol3_log_avg", "vol3_log_peak",
        "shock_log_peak_avg",
        "log_avg_k", "log_peak_k",
        "trend3_log_avg", "trend3_log_peak",
        "curv_log_avg", "curv_log_peak",
        "age_months",
    ]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    # Clean infinities just in case
    out = out.replace([np.inf, -np.inf], np.nan)

    cfg.out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(cfg.out_path, index=False)
    print(f"[STEP03] Wrote: {cfg.out_path}")

    # Quick sanity
    if len(out) > 0 and "y_log_avg" in out.columns:
        null_rate = float(out["y_log_avg"].isna().mean())
        print(f"[STEP03] y_log_avg null_rate: {null_rate:.4f}")


if __name__ == "__main__":
    main()