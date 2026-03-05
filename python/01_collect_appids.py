from __future__ import annotations

import csv
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Set
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import CFG, ensure_dirs


# Regex
STEAMCHARTS_APP_LINK_RE = re.compile(r"^/app/(\d+)$")
STORE_APPID_RE = re.compile(r"/app/(\d+)/")
STORE_DATA_APPID_RE = re.compile(r'data-ds-appid="(\d+)"')



# Config
@dataclass(frozen=True)
class Step01Config:
    # SteamCharts Top (about 25 apps/page)
    steamcharts_top_pages: int = 80

    # Optional: expand pool via Steam Store search
    enable_store_release_sweep: bool = True
    store_pages_release_sweep: int = 80
    store_page_size: int = 50

    # Networking
    timeout_sec: int = CFG.request_timeout_sec
    sleep_min: float = CFG.sleep_min
    sleep_max: float = CFG.sleep_max


def steamcharts_top_urls(pages: int) -> List[str]:
    pages = max(1, int(pages))
    urls = ["https://steamcharts.com/top"]
    for p in range(2, pages + 1):
        urls.append(f"https://steamcharts.com/top/p.{p}")
    return urls


def fetch_html(url: str, session: requests.Session, timeout_sec: int) -> str:
    headers = {"User-Agent": CFG.user_agent, "Accept-Language": CFG.accept_language}
    r = session.get(url, headers=headers, timeout=timeout_sec)
    r.raise_for_status()
    return r.text


def extract_appids_from_steamcharts_list_page(html: str) -> Set[int]:
    soup = BeautifulSoup(html, "html.parser")
    ids: Set[int] = set()
    for a in soup.select('a[href^="/app/"]'):
        href = a.get("href", "")
        m = STEAMCHARTS_APP_LINK_RE.match(href)
        if m:
            ids.add(int(m.group(1)))
    return ids


def build_store_search_url(start: int, *, page_size: int, sort_by: str, term: Optional[str] = None) -> str:
    base = "https://store.steampowered.com/search/"
    params = {
        "start": start,
        "count": page_size,
        "sort_by": sort_by,  # Released_DESC etc.
        "snr": "1_7_7_151_7",
    }
    if term:
        params["term"] = term
    return base + "?" + urlencode(params)


def parse_appids_from_store_html(html: str) -> Set[int]:
    ids: Set[int] = set()

    for m in STORE_APPID_RE.findall(html):
        ids.add(int(m))
    for m in STORE_DATA_APPID_RE.findall(html):
        ids.add(int(m))

    # soup fallback
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a[href*='/app/']"):
        href = a.get("href", "")
        m = STORE_APPID_RE.search(href)
        if m:
            ids.add(int(m.group(1)))
    return ids


def write_candidates_csv(path, app_ids: Set[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["app_id"])
        for a in sorted(app_ids):
            w.writerow([a])


def run(cfg: Step01Config) -> None:
    ensure_dirs()

    found: Set[int] = set()

    with requests.Session() as session:
        # SteamCharts Top
        top_urls = steamcharts_top_urls(cfg.steamcharts_top_pages)
        for url in tqdm(top_urls, desc=f"Step01 SteamCharts Top (pages={cfg.steamcharts_top_pages})"):
            try:
                html = fetch_html(url, session, timeout_sec=cfg.timeout_sec)
                found |= extract_appids_from_steamcharts_list_page(html)
            except Exception:
                # skip on error; Step02 will further filter anyway
                pass
            time.sleep(cfg.sleep_min)

        # Store release sweep
        if cfg.enable_store_release_sweep:
            pages = cfg.store_pages_release_sweep
            page_size = cfg.store_page_size
            for p in tqdm(range(pages), desc=f"Step01 Store sweep Released_DESC (pages={pages})"):
                start = p * page_size
                url = build_store_search_url(start, page_size=page_size, sort_by="Released_DESC", term=None)
                try:
                    html = fetch_html(url, session, timeout_sec=cfg.timeout_sec)
                    batch = parse_appids_from_store_html(html)
                    if not batch:
                        break
                    found |= batch
                except Exception:
                    pass
                time.sleep(cfg.sleep_min)

    write_candidates_csv(CFG.candidate_appids_csv, found)

    print("\n[STEP01 DONE]")
    print(f"Candidate app_ids: {len(found):,}")
    print(f"Wrote: {CFG.candidate_appids_csv}")


if __name__ == "__main__":
    run(Step01Config())