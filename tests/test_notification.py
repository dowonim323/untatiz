from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from app.services.notification import generate_news, notify_update_complete


class _FakeUrlopen:
    def __init__(self, captured_payloads: list[dict]):
        self.captured_payloads = captured_payloads

    def __call__(self, req, timeout=120):
        payload = json.loads(req.data.decode('utf-8'))
        self.captured_payloads.append(payload)
        response = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "title": "04/03 테스트리그: 테스트 제목",
                                "body": "테스트 본문",
                            },
                            ensure_ascii=False,
                        )
                    }
                }
            ]
        }

        class _Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(response, ensure_ascii=False).encode('utf-8')

        return _Response()


def _build_notification_config(tmp_path: Path, db_path: Path) -> Any:
    return SimpleNamespace(
        db_path=db_path,
        openai_api_key='test-key',
        openai_base_url='https://example.com/v1',
        openai_model='gpt-test',
        league_name='테스트리그',
        news_dir=tmp_path / 'news',
    )


def test_generate_news_uses_active_season_and_stored_snapshot_semantics(temp_db, tmp_path):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute("UPDATE seasons SET is_active = 0 WHERE id = 1")
    cursor.execute("INSERT INTO seasons (id, year, is_active) VALUES (?, ?, ?)", (2, 2026, 1))
    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('16630', '버하겐', 'pit', 'P'),
    )
    cursor.execute(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date, left_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        ('무', '16630', 2, '2026-03-28', '2026-04-01'),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ('16630', 2, '무', None, '2026-04-01', 0.30),
    )
    cursor.executemany(
        """INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('16630', 2, '2026-04-01', 0.30, 0.20),
            ('16630', 2, '2026-04-03', 0.70, 0.40),
        ],
    )
    cursor.execute(
        """
        CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        )
        """
    )
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', 2, '2026-04-01', 0.20, 0.20, 1),
            ('퐈', 2, '2026-04-01', 0.10, None, None),
            ('무', 2, '2026-04-03', 0.20, 0.00, 1),
            ('퐈', 2, '2026-04-03', 0.50, 0.40, None),
        ],
    )
    conn.commit()
    conn.close()

    captured_payloads: list[dict] = []
    config = _build_notification_config(tmp_path, temp_db)

    with patch('app.services.notification.urllib_request.urlopen', _FakeUrlopen(captured_payloads)):
        news_content = generate_news(config)

    assert json.loads(news_content) == {'title': '04/03 테스트리그: 테스트 제목', 'body': '테스트 본문'}
    assert len(captured_payloads) == 1

    prompt = captured_payloads[0]['messages'][1]['content']
    assert '04/03' in prompt
    assert '04/01' in prompt
    assert '2026-04-02' not in prompt
    assert '버하겐' in prompt
    assert '"04/03": "0.20"' in prompt
    assert '"변화량": "+0.00"' in prompt


def test_generate_news_uses_previous_team_snapshot_for_player_diff_when_war_daily_has_gap_dates(temp_db, tmp_path):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute("UPDATE seasons SET is_active = 0 WHERE id = 1")
    cursor.execute("INSERT INTO seasons (id, year, is_active) VALUES (?, ?, ?)", (2, 2026, 1))
    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('16630', '버하겐', 'pit', 'P'),
    )
    cursor.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ('무', '16630', 2, '2026-03-28'),
    )
    cursor.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ('16630', 2, None, '무', '2026-03-28', 0.10),
    )
    cursor.executemany(
        """INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('16630', 2, '2026-04-01', 0.30, 0.20),
            ('16630', 2, '2026-04-02', 0.50, 0.20),
            ('16630', 2, '2026-04-03', 0.70, 0.20),
        ],
    )
    cursor.execute(
        """CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        )"""
    )
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', 2, '2026-04-01', 0.20, 0.20, 1),
            ('퐈', 2, '2026-04-01', 0.10, None, None),
            ('무', 2, '2026-04-03', 0.60, 0.40, 1),
            ('퐈', 2, '2026-04-03', 0.10, 0.00, None),
        ],
    )
    conn.commit()
    conn.close()

    captured_payloads: list[dict] = []
    config = _build_notification_config(tmp_path, temp_db)

    with patch('app.services.notification.urllib_request.urlopen', _FakeUrlopen(captured_payloads)):
        generate_news(config)

    prompt = captured_payloads[0]['messages'][1]['content']
    assert '"04/01": "0.20"' in prompt
    assert '"04/03": "0.60"' in prompt
    assert '"변화량": "+0.40"' in prompt


def test_generate_news_uses_explicit_target_date_instead_of_latest_snapshot(temp_db, tmp_path):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute("UPDATE seasons SET is_active = 0 WHERE id = 1")
    cursor.execute("INSERT INTO seasons (id, year, is_active) VALUES (?, ?, ?)", (2, 2026, 1))
    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('16630', '버하겐', 'pit', 'P'),
    )
    cursor.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ('무', '16630', 2, '2026-03-28'),
    )
    cursor.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ('16630', 2, None, '무', '2026-03-28', 0.10),
    )
    cursor.executemany(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        [
            ('16630', 2, '2026-04-01', 0.30, 0.20),
            ('16630', 2, '2026-04-03', 0.70, 0.40),
            ('16630', 2, '2026-04-05', 0.90, 0.20),
        ],
    )
    cursor.execute(
        """CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        )"""
    )
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', 2, '2026-04-01', 0.20, 0.20, 1),
            ('퐈', 2, '2026-04-01', 0.10, None, None),
            ('무', 2, '2026-04-03', 0.60, 0.40, 1),
            ('퐈', 2, '2026-04-03', 0.10, 0.00, None),
            ('무', 2, '2026-04-05', 0.80, 0.20, 1),
            ('퐈', 2, '2026-04-05', 0.10, 0.00, None),
        ],
    )
    conn.commit()
    conn.close()

    captured_payloads: list[dict] = []
    config = _build_notification_config(tmp_path, temp_db)

    with patch('app.services.notification.urllib_request.urlopen', _FakeUrlopen(captured_payloads)):
        generate_news(config, target_date='2026-04-03')

    prompt = captured_payloads[0]['messages'][1]['content']
    assert '04/03' in prompt
    assert '04/05' not in prompt


def test_generate_news_uses_previous_completed_date_for_targeted_articles(temp_db, tmp_path):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute("UPDATE seasons SET is_active = 0 WHERE id = 1")
    cursor.execute("INSERT INTO seasons (id, year, is_active) VALUES (?, ?, ?)", (2, 2026, 1))
    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('16630', '버하겐', 'pit', 'P'),
    )
    cursor.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ('무', '16630', 2, '2026-03-28'),
    )
    cursor.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ('16630', 2, None, '무', '2026-03-28', 0.10),
    )
    cursor.executemany(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        [
            ('16630', 2, '2026-04-01', 0.30, 0.20),
            ('16630', 2, '2026-04-02', 0.50, 0.20),
            ('16630', 2, '2026-04-03', 0.70, 0.20),
        ],
    )
    cursor.execute(
        """CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        )"""
    )
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', 2, '2026-04-01', 0.20, 0.20, 1),
            ('퐈', 2, '2026-04-01', 0.10, None, None),
            ('무', 2, '2026-04-02', 0.40, 0.20, 1),
            ('퐈', 2, '2026-04-02', 0.10, 0.00, None),
            ('무', 2, '2026-04-03', 0.60, 0.20, 1),
            ('퐈', 2, '2026-04-03', 0.10, 0.00, None),
        ],
    )
    cursor.execute(
        """CREATE TABLE scraper_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TEXT NOT NULL,
            target_date TEXT NOT NULL,
            games_found INTEGER DEFAULT 0,
            games_updated INTEGER DEFAULT 0,
            war_status TEXT,
            duration_seconds REAL,
            error_message TEXT
        )"""
    )
    cursor.executemany(
        "INSERT INTO scraper_log (run_at, target_date, war_status) VALUES (?, ?, ?)",
        [
            ('2026-04-01T23:00:00+09:00', '2026-04-01', 'completed'),
            ('2026-04-02T23:00:00+09:00', '2026-04-02', 'pending'),
            ('2026-04-03T23:00:00+09:00', '2026-04-03', 'completed'),
        ],
    )
    conn.commit()
    conn.close()

    captured_payloads: list[dict] = []
    config = _build_notification_config(tmp_path, temp_db)

    with patch('app.services.notification.urllib_request.urlopen', _FakeUrlopen(captured_payloads)):
        generate_news(config, target_date='2026-04-03')

    prompt = captured_payloads[0]['messages'][1]['content']
    assert '04/03' in prompt
    assert '04/01' in prompt
    assert '04/02' not in prompt


def test_notify_update_complete_uses_target_date_for_embed_titles(temp_db, tmp_path):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE scraper_status (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_updated_at TEXT NOT NULL,
            target_date TEXT NOT NULL,
            total_games INTEGER DEFAULT 0,
            updated_games INTEGER DEFAULT 0,
            war_status TEXT DEFAULT 'pending'
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE daily_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            record_type TEXT NOT NULL,
            team_id TEXT,
            player_id TEXT NOT NULL,
            war_diff REAL NOT NULL
        )
        """
    )
    cursor.execute(
        """INSERT INTO scraper_status
           (id, last_updated_at, target_date, total_games, updated_games, war_status)
           VALUES (1, ?, ?, ?, ?, ?)""",
        ('2026-04-03T23:00:00+09:00', '2026-04-03', 2, 2, 'completed'),
    )
    cursor.execute(
        "INSERT INTO daily_records (date, record_type, team_id, player_id, war_diff) VALUES (?, ?, ?, ?, ?)",
        ('2026-04-03', 'GOAT', '준', '10002', 0.25),
    )
    conn.commit()
    conn.close()

    sent_payloads = []
    config = _build_notification_config(tmp_path, temp_db)

    def _capture_webhook(webhook_url, embeds, content=None):
        sent_payloads.append({'embeds': embeds, 'content': content})
        return True

    with patch('app.services.notification.send_discord_webhook', side_effect=_capture_webhook), patch(
        'app.services.notification.generate_news', return_value=json.dumps({'title': '04/03 테스트리그: 기사', 'body': '본문'}, ensure_ascii=False)
    ) as generate_news_mock, patch('app.services.notification.get_date_slash', return_value='04/05'):
        success = notify_update_complete('https://example.com/webhook', config=config)

    assert success is True
    generate_news_mock.assert_called_once_with(config, target_date='2026-04-03')
    assert sent_payloads[0]['embeds'][0]['title'] == '04/03 결과 업데이트 완료'
    assert sent_payloads[1]['embeds'][0]['title'] == '04/03 코민코 리그 뉴스'
