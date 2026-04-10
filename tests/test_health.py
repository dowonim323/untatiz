import sqlite3
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytz

from app.core.schema import ensure_runtime_db
from web.app import create_app


def test_health_endpoint(tmp_path):
    db_path = tmp_path / "health-default.db"
    ensure_runtime_db(db_path, 2026)

    app = create_app(config=SimpleNamespace(db_path=db_path, flask_secret_key="test-secret"))
    client = app.test_client()
    response = client.get("/health")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "healthy"


def test_health_endpoint_treats_naive_scraper_timestamp_as_kst(tmp_path):
    db_path = tmp_path / "health.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE scraper_status (
            id INTEGER PRIMARY KEY,
            last_updated_at TEXT NOT NULL,
            target_date TEXT,
            total_games INTEGER,
            updated_games INTEGER,
            war_status TEXT
        )
        """
    )
    kst = pytz.timezone("Asia/Seoul")
    recent_kst = datetime.now(kst) - timedelta(minutes=5)
    conn.execute(
        """
        INSERT INTO scraper_status (
            id, last_updated_at, target_date,
            total_games, updated_games, war_status
        )
        VALUES (1, ?, ?, ?, ?, ?)
        """,
        (recent_kst.strftime("%Y-%m-%d %H:%M:%S"), "2026-04-05", 5, 5, "completed"),
    )
    conn.commit()
    conn.close()

    app = create_app(config=SimpleNamespace(db_path=db_path, flask_secret_key="test-secret"))
    client = app.test_client()

    response = client.get("/health")

    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "healthy"
    assert data["scraper"]["war_status"] == "completed"
    assert 0 <= data["scraper"]["minutes_ago"] <= 10
