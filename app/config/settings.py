"""Configuration utilities for Untatiz."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

BASE_DIR = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class AppConfig:
    """Centralized application configuration."""

    base_dir: Path
    api_dir: Path
    db_dir: Path
    log_dir: Path
    graph_dir: Path
    news_dir: Path
    db_path: Path
    state_file: Path

    flask_secret_key: str
    flask_env: str
    database_path: str

    discord_webhook_url: Optional[str]
    openai_api_key: Optional[str]
    openai_base_url: Optional[str]
    openai_model: str
    league_name: str

    credentials_path: Path
    discord_config_path: Path
    openai_config_path: Path


def _load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def load_config() -> AppConfig:
    api_dir = BASE_DIR / "api"
    db_dir = BASE_DIR / "db"
    log_dir = BASE_DIR / "log"
    graph_dir = BASE_DIR / "graph"
    news_dir = BASE_DIR / "news"

    credentials_path = api_dir / "credentials.json"
    discord_config_path = api_dir / "discord.json"
    openai_config_path = api_dir / "openai.json"

    discord_data = _load_json_file(discord_config_path)
    openai_data = _load_json_file(openai_config_path)

    discord_webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", discord_data.get("webhook_url"))

    openai_api_key = os.environ.get("OPENAI_API_KEY", openai_data.get("api_key"))
    openai_base_url = os.environ.get("OPENAI_BASE_URL", openai_data.get("base_url"))
    openai_model = os.environ.get("OPENAI_MODEL") or openai_data.get("model") or "gpt-4.1"
    league_name = os.environ.get("LEAGUE_NAME") or openai_data.get("league_name") or "코민코 리그"

    state_file = Path(os.environ.get("STATE_FILE_PATH", str(db_dir / "scraper_state.json")))
    db_path = Path(os.environ.get("DATABASE_PATH", str(db_dir / "untatiz_db.db")))

    return AppConfig(
        base_dir=BASE_DIR,
        api_dir=api_dir,
        db_dir=db_dir,
        log_dir=log_dir,
        graph_dir=graph_dir,
        news_dir=news_dir,
        db_path=db_path,
        state_file=state_file,
        flask_secret_key=os.environ.get("FLASK_SECRET_KEY", "auto_generated"),
        flask_env=os.environ.get("FLASK_ENV", "production"),
        database_path=str(db_path),
        discord_webhook_url=discord_webhook_url,
        openai_api_key=openai_api_key,
        openai_base_url=openai_base_url,
        openai_model=openai_model,
        league_name=league_name,
        credentials_path=credentials_path,
        discord_config_path=discord_config_path,
        openai_config_path=openai_config_path,
    )


__all__ = ["AppConfig", "load_config"]
