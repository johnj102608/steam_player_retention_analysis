from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow
import fastparquet

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import CFG, ensure_dirs


# Config
@dataclass
class Step02Config:
    # Input
    candidates_csv: Path = CFG.candidate_appids_csv

    # How many app_ids to process
    max_apps: int = 6000
    seed: int = CFG.seed
    shuffle: bool = True

    # SteamCharts
    base_url: str = CFG.steamcharts_base_url
    timeout_sec: int = CFG.request_timeout_sec

    # Extract
    n_months: int = CFG.steamcharts_n_months  # month1..month12

    # Caching
    cache_html: bool = True
    overwrite_cache: bool = False
    cache_dir: Path = CFG.steamcharts_cache_dir

    # Output parts
    out_parts_dir: Path = CFG.steamcharts_metrics_parts_dir
    chunk_size: int = 300

    # Resume
    resume_enabled: bool = True
    progress_path: Path = CFG.processed_dir / "step02_progress.json"

    # Politeness
    sleep_min: float = CFG.sleep_min
    sleep_max: float = CFG.sleep_max

    # Fail-fast
    max_attempts_per_app: int = 1
    retry_sleep_min: float = 0.5
    retry_sleep_max: float = 1.5


def utc_iso(timespec: str = "seconds") -> str:
    return datetime.now(timezone.utc).isoformat(timespec=timespec)


def load_progress(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_progress(path: Path, prog: dict) -> None:
    prog = dict(prog)
    prog["updated_utc"] = utc_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(prog, indent=2, sort_keys=True), encoding="utf-8")


def to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    s = value.strip().replace(",", "")
    if s == "" or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fetch_html_once(url: str, session: requests.Session, cfg: Step02Config) -> Tuple[str, int]:
    headers = {"User-Agent": CFG.user_agent, "Accept-Language": CFG.accept_language}
    r = session.get(url, headers=headers, timeout=cfg.timeout_sec)
    status = int(r.status_code)
    if status == 404:
        return "", 404
    if 500 <= status <= 599:
        raise requests.RequestException(f"5xx: {status}")
    r.raise_for_status()
    return r.text, status


def get_html(app_id: int, session: requests.Session, cfg: Step02Config) -> Tuple[Optional[str], str, bool, Optional[int], Optional[str]]:
    """
    Returns: (html, url, from_cache, status_code, error_message)
    """
    url = cfg.base_url.format(appid=app_id)
    cache_path = cfg.cache_dir / f"{app_id}.html"

    if cfg.cache_html:
        cfg.cache_dir.mkdir(parents=True, exist_ok=True)
        if cache_path.exists() and not cfg.overwrite_cache:
            try:
                html = cache_path.read_text(encoding="utf-8", errors="ignore")
                return html, url, True, None, None
            except Exception:
                pass

    try:
        html, status_code = fetch_html_once(url, session, cfg)
        if status_code == 404:
            return None, url, False, 404, "404_not_found"

        if cfg.cache_html and html is not None:
            cache_path.write_text(html, encoding="utf-8", errors="ignore")

        return html, url, False, status_code, None
    except Exception as e:
        return None, url, False, None, repr(e)


def parse_monthly_table(html: str) -> List[Dict[str, Optional[float]]]:
    """
    Extracts rows from the SteamCharts monthly table:
    label | avg_players | peak_players

    Includes the 'Last 30 Days' row if present.
    """
    soup = BeautifulSoup(html, "html.parser")

    target_table = None
    for table in soup.find_all("table"):
        header_text = " ".join(th.get_text(" ", strip=True) for th in table.find_all("th"))
        if "Month" in header_text and "Avg. Players" in header_text and "Peak Players" in header_text:
            target_table = table
            break

    if target_table is None:
        return []

    rows: List[Dict[str, Optional[float]]] = []
    for tr in target_table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        label = tds[0].get_text(" ", strip=True)
        avg_txt = tds[1].get_text(" ", strip=True)
        peak_txt = tds[4].get_text(" ", strip=True)
        rows.append(
            {
                "label": label,
                "avg_players": to_float(avg_txt),
                "peak_players": to_float(peak_txt),
            }
        )
    return rows


def extract_metrics(table_rows: List[Dict[str, Optional[float]]], n_months: int) -> Dict[str, Any]:
    """
    Output schema:
      last30_avg_players, last30_peak_players
      month1_label, month1_avg_players, month1_peak_players
      ...
      monthN_label, monthN_avg_players, monthN_peak_players

    Convention:
      month1 = most recent completed month
      month12 = oldest (if available)
    """
    out: Dict[str, Any] = {
        "last30_avg_players": None,
        "last30_peak_players": None,
    }
    for i in range(1, n_months + 1):
        out[f"month{i}_label"] = None
        out[f"month{i}_avg_players"] = None
        out[f"month{i}_peak_players"] = None

    if not table_rows:
        return out

    # Last 30 Days row
    for r in table_rows:
        if (r.get("label") or "").strip().lower() == "last 30 days":
            out["last30_avg_players"] = r.get("avg_players")
            out["last30_peak_players"] = r.get("peak_players")
            break

    # Monthly rows (SteamCharts lists newest first)
    monthly = [r for r in table_rows if (r.get("label") or "").strip().lower() != "last 30 days"]
    for i in range(min(n_months, len(monthly))):
        out[f"month{i+1}_label"] = monthly[i].get("label")
        out[f"month{i+1}_avg_players"] = monthly[i].get("avg_players")
        out[f"month{i+1}_peak_players"] = monthly[i].get("peak_players")

    return out


def load_candidate_appids(path: Path) -> List[int]:
    df = pd.read_csv(path)
    if "app_id" not in df.columns and "appid" in df.columns:
        df = df.rename(columns={"appid": "app_id"})
    if "app_id" not in df.columns:
        raise ValueError(f"Candidate CSV must contain 'app_id'. Found: {list(df.columns)}")
    ids = df["app_id"].dropna().astype(int).drop_duplicates().tolist()
    return ids


def flush_part(cfg: Step02Config, prog: dict, buffer_rows: List[dict]) -> None:
    if not buffer_rows:
        return
    part_id = int(prog.get("parts_written", 0))
    out_path = cfg.out_parts_dir / f"part_{part_id:06d}.parquet"
    cfg.out_parts_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(buffer_rows).to_parquet(out_path, index=False)
    buffer_rows.clear()
    prog["parts_written"] = part_id + 1


def run(cfg: Step02Config) -> None:
    ensure_dirs()
    cfg.out_parts_dir.mkdir(parents=True, exist_ok=True)

    app_ids = load_candidate_appids(cfg.candidates_csv)
    if cfg.max_apps > 0:
        app_ids = app_ids[: min(cfg.max_apps, len(app_ids))]

    if cfg.shuffle:
        random.seed(cfg.seed)
        random.shuffle(app_ids)

    prog_existing = load_progress(cfg.progress_path) if cfg.resume_enabled else None
    if cfg.resume_enabled and prog_existing:
        prog = prog_existing
        start_idx = int(prog.get("last_index", -1)) + 1
        start_idx = max(0, min(start_idx, len(app_ids)))
        print(f"[RESUME] start_idx={start_idx} / {len(app_ids)}")
    else:
        prog = {
            "created_utc": utc_iso(),
            "updated_utc": utc_iso(),
            "last_index": -1,
            "processed": 0,
            "ok": 0,
            "not_found": 0,
            "no_table": 0,
            "error": 0,
            "parts_written": 0,
        }
        start_idx = 0
        save_progress(cfg.progress_path, prog)
        print("[FRESH RUN] Step02")

    buffer_rows: List[dict] = []

    overall = tqdm(total=(len(app_ids) - start_idx), desc="Step02 SteamCharts metrics", unit="app")

    with requests.Session() as session:
        for idx in range(start_idx, len(app_ids)):
            app_id = int(app_ids[idx])
            prog["last_index"] = idx
            prog["processed"] = int(prog.get("processed", 0)) + 1

            html: Optional[str] = None
            url: str = cfg.base_url.format(appid=app_id)
            from_cache = False
            status_code: Optional[int] = None
            err_msg: Optional[str] = None

            handled = False

            for attempt in range(cfg.max_attempts_per_app):
                html, url, from_cache, status_code, err_msg = get_html(app_id, session, cfg)

                if html is None:
                    # 404
                    if status_code == 404:
                        prog["not_found"] = int(prog.get("not_found", 0)) + 1
                        buffer_rows.append({
                            "app_id": app_id,
                            "status": "not_found",
                            "scraped_utc": utc_iso(),
                            "url": url,
                            "status_code": 404,
                        })
                        handled = True
                        break

                    # other error
                    if attempt < cfg.max_attempts_per_app - 1:
                        time.sleep(random.uniform(cfg.retry_sleep_min, cfg.retry_sleep_max))
                        continue

                    prog["error"] = int(prog.get("error", 0)) + 1
                    buffer_rows.append({
                        "app_id": app_id,
                        "status": "error",
                        "scraped_utc": utc_iso(),
                        "url": url,
                        "status_code": status_code,
                        "error": err_msg,
                    })
                    handled = True
                    break

                # Success fetch (cache or network)
                if not from_cache:
                    time.sleep(random.uniform(cfg.sleep_min, cfg.sleep_max))

                table_rows = parse_monthly_table(html)
                if not table_rows:
                    prog["no_table"] = int(prog.get("no_table", 0)) + 1
                    buffer_rows.append({
                        "app_id": app_id,
                        "status": "no_table",
                        "scraped_utc": utc_iso(),
                        "url": url,
                        "status_code": status_code,
                    })
                    handled = True
                    break

                metrics = extract_metrics(table_rows, cfg.n_months)
                buffer_rows.append({
                    "app_id": app_id,
                    "status": "ok",
                    "scraped_utc": utc_iso(),
                    "url": url,
                    **metrics,
                })
                prog["ok"] = int(prog.get("ok", 0)) + 1
                handled = True
                break

            if not handled:
                # Should not happen, but keep a hard fallback
                prog["error"] = int(prog.get("error", 0)) + 1
                buffer_rows.append({
                    "app_id": app_id,
                    "status": "error",
                    "scraped_utc": utc_iso(),
                    "url": url,
                    "error": "unhandled_state",
                })

            # Flush parts
            if len(buffer_rows) >= cfg.chunk_size:
                flush_part(cfg, prog, buffer_rows)
                save_progress(cfg.progress_path, prog)

            overall.update(1)

    # Final flush
    flush_part(cfg, prog, buffer_rows)
    save_progress(cfg.progress_path, prog)
    overall.close()

    print("\n[STEP02 DONE]")
    print(f"Processed: {prog.get('processed', 0):,}")
    print(f"OK: {prog.get('ok', 0):,} | 404: {prog.get('not_found', 0):,} | no_table: {prog.get('no_table', 0):,} | error: {prog.get('error', 0):,}")
    print(f"Parts written: {prog.get('parts_written', 0):,}")
    print(f"Out dir: {cfg.out_parts_dir}")
    print(f"Progress: {cfg.progress_path}")


if __name__ == "__main__":
    run(Step02Config())