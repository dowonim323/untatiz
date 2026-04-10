"""Shared pytest fixtures for Untatiz tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from app.core.db import DatabaseManager


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database with schema for testing.
    
    Returns:
        Path: Path to temporary database file
    """
    db_path = tmp_path / "test_untatiz.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Create schema
    cursor.executescript("""
        -- Seasons table
        CREATE TABLE seasons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL UNIQUE,
            is_active INTEGER DEFAULT 0
        );
        
        -- Fantasy teams table
        CREATE TABLE fantasy_teams (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            owner TEXT
        );
        
        -- Players table
        CREATE TABLE players (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            player_type TEXT,
            position TEXT
        );
        
        -- Draft table
        CREATE TABLE draft (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season_id INTEGER NOT NULL,
            team_id TEXT NOT NULL,
            player_id TEXT NOT NULL,
            round TEXT NOT NULL,
            pick_order INTEGER,
            draft_type TEXT DEFAULT 'main',
            application_date TEXT,
            FOREIGN KEY (season_id) REFERENCES seasons(id),
            FOREIGN KEY (team_id) REFERENCES fantasy_teams(id),
            FOREIGN KEY (player_id) REFERENCES players(id)
        );
        
        -- Roster table
        CREATE TABLE roster (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id TEXT NOT NULL,
            player_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            joined_date TEXT NOT NULL,
            left_date TEXT,
            FOREIGN KEY (team_id) REFERENCES fantasy_teams(id),
            FOREIGN KEY (player_id) REFERENCES players(id),
            FOREIGN KEY (season_id) REFERENCES seasons(id)
        );
        
        -- Transactions table
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            from_team_id TEXT,
            to_team_id TEXT,
            transaction_date TEXT NOT NULL,
            war_at_transaction REAL,
            FOREIGN KEY (player_id) REFERENCES players(id),
            FOREIGN KEY (season_id) REFERENCES seasons(id),
            FOREIGN KEY (from_team_id) REFERENCES fantasy_teams(id),
            FOREIGN KEY (to_team_id) REFERENCES fantasy_teams(id)
        );
        
        -- FA config table
        CREATE TABLE fa_config (
            season_id INTEGER PRIMARY KEY,
            roster_size INTEGER DEFAULT 29,
            supplemental_bonus INTEGER DEFAULT 5,
            min_pitchers INTEGER DEFAULT 11,
            min_catchers INTEGER DEFAULT 2,
            min_infielders INTEGER DEFAULT 7,
            min_outfielders INTEGER DEFAULT 5,
            FOREIGN KEY (season_id) REFERENCES seasons(id)
        );
        
        -- War daily table (for transaction WAR lookup)
        CREATE TABLE war_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            war REAL,
            war_diff REAL,
            FOREIGN KEY (player_id) REFERENCES players(id),
            FOREIGN KEY (season_id) REFERENCES seasons(id)
        );
        
        -- Insert test data
        INSERT INTO seasons (id, year, is_active) VALUES (1, 2025, 1);
        
        INSERT INTO fantasy_teams (id, name, owner) VALUES
            ('준', '준팀', '준'),
            ('뚝', '뚝팀', '뚝'),
            ('재', '재팀', '재'),
            ('퐈', 'Free Agent', NULL);
        
        INSERT INTO players (id, name, player_type, position) VALUES
            ('10001', '김투수', 'pitcher', 'P'),
            ('10002', '박포수', 'batter', 'C'),
            ('10003', '이일루', 'batter', '1B'),
            ('10004', '최이루', 'batter', '2B'),
            ('10005', '정유격', 'batter', 'SS'),
            ('10006', '강삼루', 'batter', '3B'),
            ('10007', '윤좌익', 'batter', 'LF'),
            ('10008', '송중견', 'batter', 'CF'),
            ('10009', '한우익', 'batter', 'RF'),
            ('10010', '오지명', 'batter', 'DH'),
            ('10011', '류투수2', 'pitcher', 'P'),
            ('10012', '서포수2', 'batter', 'C');
        
        INSERT INTO fa_config (season_id, roster_size, supplemental_bonus, 
                               min_pitchers, min_catchers, min_infielders, min_outfielders)
        VALUES (1, 29, 5, 11, 2, 7, 5);
    """)
    
    conn.commit()
    conn.close()
    
    return db_path


@pytest.fixture
def db_manager(temp_db):
    """Create DatabaseManager instance with temp database.
    
    Args:
        temp_db: Temporary database path fixture
        
    Returns:
        DatabaseManager: Database manager instance
    """
    return DatabaseManager(temp_db)


@pytest.fixture
def sample_bat_df():
    """Sample batter DataFrame matching statiz.co.kr format.
    
    Returns:
        pd.DataFrame: Batter statistics with oWAR
    """
    data = {
        'Name': ['박포수', '이일루', '최이루', '정유격', '강삼루', 
                 '윤좌익', '송중견', '한우익', '오지명', '서포수2'],
        'POS': ['C', '1B', '2B', 'SS', '3B', 'LF', 'CF', 'RF', 'DH', 'C'],
        'oWAR': [1.5, 2.3, 1.8, 2.1, 1.2, 1.9, 2.5, 1.7, 0.8, 1.1],
        'G': [50, 55, 52, 54, 48, 53, 56, 51, 45, 40],
        'PA': [200, 220, 210, 215, 190, 212, 225, 205, 180, 160],
        'AVG': [.280, .295, .270, .285, .265, .290, .305, .275, .250, .260]
    }
    
    df = pd.DataFrame(data)
    # Set index to player IDs
    df.index = ['10002', '10003', '10004', '10005', '10006', 
                '10007', '10008', '10009', '10010', '10012']
    df.index.name = 'ID'
    
    return df


@pytest.fixture
def sample_pit_df():
    """Sample pitcher DataFrame matching statiz.co.kr format.
    
    Returns:
        pd.DataFrame: Pitcher statistics with WAR
    """
    data = {
        'Name': ['김투수', '류투수2'],
        'WAR': [3.2, 2.1],
        'G': [25, 20],
        'IP': [150.0, 120.0],
        'ERA': [2.85, 3.45],
        'WHIP': [1.15, 1.28]
    }
    
    df = pd.DataFrame(data)
    # Set index to player IDs
    df.index = ['10001', '10011']
    df.index.name = 'ID'
    
    return df


@pytest.fixture
def sample_player_id_df():
    """Sample player ID DataFrame indexed by team (legacy format).
    
    Returns:
        pd.DataFrame: Player IDs by team and roster position
    """
    data = {
        '준': ['10001', '10002', '10003', np.nan, np.nan],
        '뚝': ['10004', '10005', '10006', np.nan, np.nan],
        '재': ['10007', '10008', np.nan, np.nan, np.nan]
    }
    
    return pd.DataFrame(data, index=['용투1', '용타', '1R', '2R', '3R'])


@pytest.fixture
def sample_player_activation_df():
    """Sample player activation DataFrame (legacy format).
    
    Returns:
        pd.DataFrame: Boolean activation status by team and position
    """
    data = {
        '준': [True, True, True, False, False],
        '뚝': [True, True, False, False, False],
        '재': [True, True, False, False, False]
    }
    
    return pd.DataFrame(data, index=['용투1', '용타', '1R', '2R', '3R'])


@pytest.fixture
def sample_war_basis_df():
    """Sample WAR basis DataFrame (legacy format).
    
    Returns:
        pd.DataFrame: WAR values when player was acquired/released
    """
    data = {
        '준': [0.0, 0.0, 0.0, 1.5, 0.0],
        '뚝': [0.0, 0.0, 1.2, 0.0, 0.0],
        '재': [0.0, 0.0, 0.0, 0.0, 0.0]
    }
    
    return pd.DataFrame(data, index=['용투1', '용타', '1R', '2R', '3R'])


@pytest.fixture
def draft_json_valid(tmp_path):
    """Create a valid draft JSON file for testing.
    
    Args:
        tmp_path: pytest tmp_path fixture
        
    Returns:
        Path: Path to JSON file
    """
    import json
    
    json_path = tmp_path / "draft_valid.json"
    
    data = {
        "season": 2025,
        "draft_type": "main",
        "application_date": "2025-03-21",
        "description": "2025 메인 드래프트",
        "picks": [
            {"team": "준", "round": "용타", "player_id": "10002", "player_name": "박포수"},
            {"team": "뚝", "round": "용타", "player_id": "10003", "player_name": "이일루"},
            {"team": "준", "round": "용투1", "player_id": "10001", "player_name": "김투수"},
            {"team": "뚝", "round": "용투1", "player_id": "10011", "player_name": "류투수2"}
        ],
        "fa_config": {
            "roster_size": 29,
            "supplemental_bonus": 5,
            "position_requirements": {
                "P": 11,
                "C": 2,
                "IF": 7,
                "OF": 5
            }
        }
    }
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return json_path


@pytest.fixture
def draft_json_invalid_type(tmp_path):
    """Create draft JSON with invalid draft_type.
    
    Args:
        tmp_path: pytest tmp_path fixture
        
    Returns:
        Path: Path to JSON file
    """
    import json
    
    json_path = tmp_path / "draft_invalid_type.json"
    
    data = {
        "season": 2025,
        "draft_type": "invalid_type",  # Invalid
        "application_date": "2025-03-21",
        "picks": []
    }
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return json_path


@pytest.fixture
def draft_json_missing_fields(tmp_path):
    """Create draft JSON with missing required fields.
    
    Args:
        tmp_path: pytest tmp_path fixture
        
    Returns:
        Path: Path to JSON file
    """
    import json
    
    json_path = tmp_path / "draft_missing_fields.json"
    
    data = {
        "season": 2025,
        # Missing draft_type and application_date
        "picks": []
    }
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return json_path
