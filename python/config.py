# src/config.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:

    # Project paths
    project_root: Path = Path(__file__).resolve().parents[1]

    data_dir: Path = project_root / "data"
    raw_dir: Path = data_dir / "raw"
    processed_dir: Path = data_dir / "processed"
    logs_dir: Path = processed_dir / "logs"


    # Step 01 output
    candidate_appids_csv: Path = processed_dir / "candidate_appids.csv"

    # Step 02 (SteamCharts monthly metrics)
    steamcharts_cache_dir: Path = raw_dir / "html_cache" / "steamcharts"
    steamcharts_metrics_parts_dir: Path = processed_dir / "steamcharts_metrics_parts"

    # Step 02b (Steam Store appdetails metadata)
    store_cache_dir: Path = raw_dir / "json_cache" / "steam_store_appdetails"
    store_meta_parts_dir: Path = processed_dir / "steam_store_meta_parts"
    store_meta_merged_path: Path = processed_dir / "steam_store_meta.parquet"

    # Step 03 (Panel dataset)
    model_panel_h3_path: Path = processed_dir / "model_panel_h3.parquet"


    # Networking defaults
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    accept_language: str = "en-US,en;q=0.9"

    request_timeout_sec: int = 20

    # Polite
    sleep_min: float = 0.20
    sleep_max: float = 0.70

    # SteamCharts extraction
    steamcharts_base_url: str = "https://steamcharts.com/app/{appid}"
    steamcharts_n_months: int = 12  # month1..month12


    # Store metadata extraction
    store_appdetails_url: str = "https://store.steampowered.com/api/appdetails"
    store_retry_attempts: int = 3

    # Panel dataset rules
    horizon: int = 6
    min_months_required: int = 12

    # Tag encoding (top-K)
    top_k_genres: int = 80
    top_k_categories: int = 120

    # Reproducibility
    seed: int = 12345


CFG = Config()


def ensure_dirs() -> None:
    """
    Create all directories needed by the pipeline.
    """
    CFG.data_dir.mkdir(parents=True, exist_ok=True)
    CFG.raw_dir.mkdir(parents=True, exist_ok=True)
    CFG.processed_dir.mkdir(parents=True, exist_ok=True)
    CFG.logs_dir.mkdir(parents=True, exist_ok=True)

    CFG.steamcharts_cache_dir.mkdir(parents=True, exist_ok=True)
    CFG.steamcharts_metrics_parts_dir.mkdir(parents=True, exist_ok=True)

    CFG.store_cache_dir.mkdir(parents=True, exist_ok=True)
    CFG.store_meta_parts_dir.mkdir(parents=True, exist_ok=True)