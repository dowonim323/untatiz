"""Tests for app/core modules."""

import pytest
import tempfile
from pathlib import Path

import pandas as pd

from app.core.db import DatabaseManager, load_table, save_table, backup_database
from app.core.utils import (
    get_date,
    get_time_status,
    col_to_letter,
    parse_date_string,
    get_team_from_svg,
)


class TestDatabaseManager:
    """Tests for DatabaseManager class."""
    
    def test_create_database_manager(self, tmp_path):
        """Test creating a DatabaseManager instance."""
        db_path = tmp_path / "test.db"
        db = DatabaseManager(db_path)
        assert db.db_path == db_path
    
    def test_save_and_load_table(self, tmp_path):
        """Test saving and loading a table."""
        db_path = tmp_path / "test.db"
        db = DatabaseManager(db_path)
        
        # Create test DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.5, 30.5]
        })
        
        # Save table
        db.save_table(df, "test_table")
        
        # Load and verify
        loaded = db.load_table("test_table")
        assert len(loaded) == 3
        assert list(loaded.columns) == ["id", "name", "value"]
        assert loaded.iloc[0]["name"] == "Alice"
    
    def test_list_tables(self, tmp_path):
        """Test listing tables."""
        db_path = tmp_path / "test.db"
        db = DatabaseManager(db_path)
        
        # Create test tables
        df = pd.DataFrame({"col": [1, 2, 3]})
        db.save_table(df, "table1")
        db.save_table(df, "table2")
        
        tables = db.list_tables()
        assert "table1" in tables
        assert "table2" in tables
    
    def test_table_exists(self, tmp_path):
        """Test checking if table exists."""
        db_path = tmp_path / "test.db"
        db = DatabaseManager(db_path)
        
        df = pd.DataFrame({"col": [1]})
        db.save_table(df, "existing_table")
        
        assert db.table_exists("existing_table") is True
        assert db.table_exists("nonexistent_table") is False
    
    def test_backup(self, tmp_path):
        """Test database backup."""
        db_path = tmp_path / "test.db"
        backup_dir = tmp_path / "backups"
        
        db = DatabaseManager(db_path)
        
        # Create some data
        df = pd.DataFrame({"col": [1, 2, 3]})
        db.save_table(df, "test_table")
        
        # Create backup
        backup_path = db.backup(backup_dir)
        
        assert backup_path.exists()
        assert backup_path.suffix == ".db"
    
    def test_load_nonexistent_table_raises(self, tmp_path):
        """Test that loading nonexistent table raises ValueError."""
        db_path = tmp_path / "test.db"
        db = DatabaseManager(db_path)
        
        with pytest.raises(ValueError, match="does not exist"):
            db.load_table("nonexistent")


class TestUtils:
    """Tests for utility functions."""
    
    def test_get_date_returns_string(self):
        """Test get_date returns properly formatted string."""
        result = get_date()
        assert isinstance(result, str)
        assert "/" in result
        parts = result.split("/")
        assert len(parts) == 2
        assert 1 <= int(parts[0]) <= 12
        assert 1 <= int(parts[1]) <= 31
    
    def test_get_time_status_returns_0_or_1(self):
        """Test get_time_status returns 0 or 1."""
        result = get_time_status()
        assert result in [0, 1]
    
    def test_col_to_letter(self):
        """Test column number to letter conversion."""
        assert col_to_letter(1) == "A"
        assert col_to_letter(26) == "Z"
        assert col_to_letter(27) == "AA"
        assert col_to_letter(28) == "AB"
        assert col_to_letter(52) == "AZ"
        assert col_to_letter(53) == "BA"
    
    def test_parse_date_string(self):
        """Test parsing date string."""
        month, day = parse_date_string("05/15")
        assert month == 5
        assert day == 15
        
        month, day = parse_date_string("12/31")
        assert month == 12
        assert day == 31
    
    def test_get_team_from_svg_any_year(self):
        result = get_team_from_svg("/data/team/ci/2025/2002.svg", 2025)
        assert result == "KIA"
        
        result = get_team_from_svg("/data/team/ci/2025/5002.svg", 2025)
        assert result == "LG"

        result = get_team_from_svg("/data/team/ci/2026/5002.svg", 2026)
        assert result == "LG"
    
    def test_get_team_from_svg_unknown(self):
        """Test getting team name from unknown SVG path."""
        result = get_team_from_svg("/unknown/path.svg", 2025)
        assert result == ""


class TestLegacyFunctions:
    """Tests for legacy standalone functions."""
    
    def test_legacy_load_table(self, tmp_path):
        """Test legacy load_table function."""
        db_path = tmp_path / "test.db"
        
        # Create table first
        db = DatabaseManager(db_path)
        df = pd.DataFrame({"col": [1, 2, 3]})
        db.save_table(df, "legacy_test")
        
        # Use legacy function
        loaded = load_table("legacy_test", db_path)
        assert len(loaded) == 3
    
    def test_legacy_save_table(self, tmp_path):
        """Test legacy save_table function."""
        db_path = tmp_path / "test.db"
        
        df = pd.DataFrame({"col": [1, 2, 3]})
        save_table(df, "legacy_test", db_path)
        
        # Verify with DatabaseManager
        db = DatabaseManager(db_path)
        loaded = db.load_table("legacy_test")
        assert len(loaded) == 3
    
    def test_legacy_backup_database(self, tmp_path):
        """Test legacy backup_database function."""
        db_path = tmp_path / "test.db"
        backup_dir = tmp_path / "backups"
        
        # Create table first
        db = DatabaseManager(db_path)
        df = pd.DataFrame({"col": [1, 2, 3]})
        db.save_table(df, "test_table")
        
        # Use legacy function
        backup_path = backup_database(db_path, backup_dir)
        assert backup_path.exists()
