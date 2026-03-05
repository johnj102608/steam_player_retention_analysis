# src/step02b_pull_store_meta.py
from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

from config import CFG, ensure_dirs



# Config
@dataclass
class Step02bConfig:
    # Source
    metrics_parts_dir: Path = CFG.steamcharts_metrics_parts_dir
    require_metrics_status: str = "ok"

    # Output
    out_parts_dir: Path = CFG.store_meta_parts_dir
    out_merged_path: Path = CFG.store_meta_merged_path

    # Cache
    cache_dir: Path = CFG.store_cache_dir
    cache_json: bool = True
    overwrite_cache: bool = False

    # Networking
    timeout_sec: int = CFG.request_timeout_sec
    sleep_min: float = CFG.sleep_min
    sleep_max: float = CFG.sleep_max

    # Batching / resume
    chunk_size: int = 200
    resume_enabled: bool = True
    progress_path: Path = CFG.processed_dir / "step02b_progress.json"
    failures_path: Path = CFG.logs_dir / "step02b_failures.csv"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def append_csv_row(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([row])
    if path.exists():
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, index=False)


def load_progress(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "created_utc": utc_iso(),
        "updated_utc": utc_iso(),
        "last_index": -1,
        "processed": 0,
        "ok": 0,
        "no_data": 0,
        "errors": 0,
        "parts_written": 0,
        "source": "steamcharts_metrics_parts(status=ok)",
    }


def save_progress(path: Path, prog: dict) -> None:
    prog["updated_utc"] = utc_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(prog, indent=2, sort_keys=True), encoding="utf-8")


def load_success_app_ids_from_metrics(parts_dir: Path, status_ok: str) -> List[int]:
    parts = sorted(parts_dir.glob("part_*.parquet"))
    if not parts:
        raise FileNotFoundError(f"No metrics parts found in: {parts_dir}")

    ids: Set[int] = set()
    for p in parts:
        df = pd.read_parquet(p, columns=["app_id", "status"])
        df = df[df["status"] == status_ok]
        ids.update(df["app_id"].dropna().astype(int).tolist())
    return sorted(ids)


# Fetch + cache
@retry(
    reraise=True,
    stop=stop_after_attempt(CFG.store_retry_attempts),
    wait=wait_exponential(multiplier=0.7, min=0.7, max=8),
    retry=retry_if_exception_type((requests.RequestException,)),
)
def fetch_appdetails(app_id: int, session: requests.Session, cfg: Step02bConfig) -> dict:
    params = {"appids": app_id, "l": "english"}
    headers = {"User-Agent": CFG.user_agent, "Accept-Language": CFG.accept_language}
    r = session.get(CFG.store_appdetails_url, params=params, headers=headers, timeout=cfg.timeout_sec)
    r.raise_for_status()
    return r.json()


def get_appdetails(app_id: int, session: requests.Session, cfg: Step02bConfig) -> Optional[dict]:
    cache_path = cfg.cache_dir / f"{app_id}.json"

    if cfg.cache_json:
        cfg.cache_dir.mkdir(parents=True, exist_ok=True)
        if cache_path.exists() and not cfg.overwrite_cache:
            try:
                return json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception:
                pass

    data = fetch_appdetails(app_id, session, cfg)

    if cfg.cache_json:
        cache_path.write_text(json.dumps(data), encoding="utf-8")

    return data


def safe_list_of_names(items: Any) -> List[str]:
    if not items:
        return []
    out: List[str] = []
    for x in items:
        if not isinstance(x, dict):
            continue
        name = x.get("description") or x.get("name")
        if name:
            out.append(str(name).strip())
    return out


def parse_meta(app_id: int, payload: dict) -> Optional[dict]:
    node = payload.get(str(app_id))
    if not node or not node.get("success"):
        return None

    data = node.get("data") or {}

    genres = safe_list_of_names(data.get("genres"))
    categories = safe_list_of_names(data.get("categories"))

    is_free = bool(data.get("is_free", False))

    release_date_str = None
    rd = data.get("release_date") or {}
    if rd.get("date"):
        release_date_str = rd.get("date")

    price_usd = None
    po = data.get("price_overview")
    if isinstance(po, dict):
        final = po.get("final")
        currency = po.get("currency")
        if isinstance(final, (int, float)) and currency == "USD":
            price_usd = final / 100.0

    return {
        "app_id": int(app_id),
        "is_free": is_free,
        "price_usd": price_usd,
        "release_date_str": release_date_str,
        "genres": genres,
        "categories": categories,
    }


def run(cfg: Step02bConfig) -> None:
    ensure_dirs()
    cfg.out_parts_dir.mkdir(parents=True, exist_ok=True)
    cfg.cache_dir.mkdir(parents=True, exist_ok=True)

    app_ids = load_success_app_ids_from_metrics(cfg.metrics_parts_dir, cfg.require_metrics_status)
    print(f"[STEP02b] SteamCharts-success app_ids: {len(app_ids):,}")

    prog = load_progress(cfg.progress_path)

    start_idx = 0
    if cfg.resume_enabled:
        start_idx = int(prog.get("last_index", -1)) + 1
        start_idx = max(0, min(start_idx, len(app_ids)))
        if start_idx > 0:
            print(f"[RESUME] start_idx={start_idx} / {len(app_ids)}")

    overall = tqdm(total=(len(app_ids) - start_idx), desc="Step02b Store Meta", unit="app")

    with requests.Session() as session:
        for chunk_start in range(start_idx, len(app_ids), cfg.chunk_size):
            chunk_end = min(chunk_start + cfg.chunk_size, len(app_ids))
            chunk = app_ids[chunk_start:chunk_end]
            rows: List[dict] = []

            for idx, app_id in enumerate(chunk, start=chunk_start):
                prog["last_index"] = idx
                prog["processed"] = int(prog.get("processed", 0)) + 1

                time.sleep(random.uniform(cfg.sleep_min, cfg.sleep_max))

                try:
                    payload = get_appdetails(app_id, session, cfg)
                    meta = parse_meta(app_id, payload or {})
                    if meta is None:
                        prog["no_data"] = int(prog.get("no_data", 0)) + 1
                        rows.append({"app_id": int(app_id), "status": "no_data"})
                    else:
                        prog["ok"] = int(prog.get("ok", 0)) + 1
                        rows.append({"status": "ok", "scraped_utc": utc_iso(), **meta})

                except Exception as e:
                    prog["errors"] = int(prog.get("errors", 0)) + 1
                    rows.append({"app_id": int(app_id), "status": "error", "error": repr(e)})
                    append_csv_row(cfg.failures_path, {
                        "utc": utc_iso(),
                        "app_id": int(app_id),
                        "stage": "fetch_or_parse",
                        "error": repr(e),
                    })

                overall.update(1)

            part_id = int(prog.get("parts_written", 0))
            part_path = cfg.out_parts_dir / f"part_{part_id:06d}.parquet"
            pd.DataFrame(rows).to_parquet(part_path, index=False)

            prog["parts_written"] = part_id + 1
            save_progress(cfg.progress_path, prog)

    overall.close()

    print("\n[STEP02b DONE]")
    print(f"OK: {prog.get('ok', 0):,} | no_data: {prog.get('no_data', 0):,} | errors: {prog.get('errors', 0):,}")
    print(f"Parts written: {prog.get('parts_written', 0):,}")
    print(f"Parts dir: {cfg.out_parts_dir}")

    # Merge parts
    parts = sorted(cfg.out_parts_dir.glob("part_*.parquet"))
    if not parts:
        print("No parts to merge (unexpected).")
        return
    df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    df.to_parquet(cfg.out_merged_path, index=False)
    print(f"Merged meta: {cfg.out_merged_path}")


if __name__ == "__main__":
    run(Step02bConfig())