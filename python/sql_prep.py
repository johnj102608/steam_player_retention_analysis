from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from config import CFG, ensure_dirs


OUT_PATH = CFG.processed_dir / "sql_load" / "store_meta_clean.csv"


def bool_to_tf(x: Any) -> str:
    """Normalize boolean-ish values to TRUE/FALSE strings (or empty)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    if isinstance(x, bool):
        return "TRUE" if x else "FALSE"
    t = str(x).strip().upper()
    if t in {"TRUE", "1", "YES", "Y"}:
        return "TRUE"
    if t in {"FALSE", "0", "NO", "N"}:
        return "FALSE"
    return t


def _extract_quoted_items(s: str) -> list[str]:
    """
    Extract items inside single quotes from strings like:
      ['Single-player' 'Multi-player' 'PvP' ...]
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return []
    txt = str(s)
    items = re.findall(r"'([^']+)'", txt)
    return [i.strip() for i in items if i.strip()]


def _as_list_of_str(x: Any) -> list[str]:
    """
    Robustly convert x into a list[str] for genres/categories fields.
    Handles:
      - actual Python list
      - quoted-list strings: "['A' 'B']"
      - comma-separated strings: "A, B, C"
      - pipe-separated strings: "A|B|C"
      - empty / NaN
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []

    # already list-like
    if isinstance(x, list) or isinstance(x, tuple):
        items = [str(i).strip() for i in x if str(i).strip()]
        return items

    s = str(x).strip()
    if not s:
        return []

    # 1) quoted items form (most problematic)
    quoted = _extract_quoted_items(s)
    if quoted:
        return quoted

    # 2) if looks like bracketed list but no quotes, strip brackets
    s2 = s
    if s2.startswith("[") and s2.endswith("]"):
        s2 = s2[1:-1].strip()

    # 3) split on pipe first (if already pipe-delimited)
    if "|" in s2:
        parts = [p.strip() for p in s2.split("|")]
        return [p for p in parts if p]

    # 4) split on comma (common)
    if "," in s2:
        parts = [p.strip() for p in s2.split(",")]
        return [p for p in parts if p]

    # 5) fallback: single token string
    return [s2] if s2 else []


def to_pipe_field(x: Any) -> str:
    """Final serialization for SQL: pipe-delimited string (no brackets, no quotes)."""
    items = _as_list_of_str(x)
    return "|".join(items)


def clean_text_field(x: Any) -> str:
    """Remove newlines/tabs; keep it SQL-load friendly."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x)
    return (
        s.replace("\r", " ")
        .replace("\n", " ")
        .replace("\t", " ")
        .strip()
    )


def main() -> None:
    ensure_dirs()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not CFG.store_meta_merged_path.exists():
        raise FileNotFoundError(f"Missing store meta parquet: {CFG.store_meta_merged_path.resolve()}")

    print("[StoreMeta] Loading merged parquet...")
    sdf = pd.read_parquet(CFG.store_meta_merged_path)

    # Keep only ok rows if status exists
    if "status" in sdf.columns:
        sdf = sdf[sdf["status"] == "ok"].copy()

    # Dedup by app_id (latest wins if scraped_utc exists)
    if "scraped_utc" in sdf.columns and "app_id" in sdf.columns:
        sdf = sdf.sort_values(["app_id", "scraped_utc"])
        sdf = sdf.drop_duplicates(subset=["app_id"], keep="last")
    elif "app_id" in sdf.columns:
        sdf = sdf.drop_duplicates(subset=["app_id"], keep="last")

    # Build final fields
    out = pd.DataFrame()
    if "app_id" not in sdf.columns:
        raise ValueError("store meta parquet missing required column: app_id")

    out["app_id"] = sdf["app_id"].astype("int64")

    out["is_free"] = sdf["is_free"].apply(bool_to_tf) if "is_free" in sdf.columns else ""
    out["price_usd"] = sdf["price_usd"] if "price_usd" in sdf.columns else pd.NA
    out["release_date_str"] = sdf["release_date_str"].apply(clean_text_field) if "release_date_str" in sdf.columns else ""

    # genres / categories → pipe delimited
    out["genres_csv"] = sdf["genres"].apply(to_pipe_field) if "genres" in sdf.columns else ""
    out["categories_csv"] = sdf["categories"].apply(to_pipe_field) if "categories" in sdf.columns else ""

    out["scraped_utc"] = sdf["scraped_utc"] if "scraped_utc" in sdf.columns else ""

    # strip any stray whitespace
    for c in ["genres_csv", "categories_csv", "release_date_str"]:
        out[c] = out[c].apply(clean_text_field)

    out.to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"[StoreMeta] Wrote: {OUT_PATH.resolve()} rows={len(out):,}")

    # checking
    if len(out) > 0:
        print("Sample genres_csv:", out["genres_csv"].dropna().head(3).tolist())
        print("Sample categories_csv:", out["categories_csv"].dropna().head(3).tolist())


if __name__ == "__main__":
    main()