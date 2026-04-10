"""Database helpers for Untatiz."""

from __future__ import annotations

import shutil
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator, List, Optional

import pandas as pd
import pytz


class DatabaseManager:
    """SQLite database manager with context management.
    
    Usage:
        db = DatabaseManager(db_path)
        
        # Using context manager (recommended)
        with db.connection() as conn:
            df = pd.read_sql_query("SELECT * FROM team_war_daily", conn)
        
        # Direct methods
        df = db.load_table("war_daily")
        db.save_table(df, "war_daily")
        db.backup(backup_dir)
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database connection.
        
        Yields:
            sqlite3.Connection: Database connection with FK enforcement
        """
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    def list_tables(self) -> List[str]:
        """List all tables in the database.
        
        Returns:
            List[str]: Table names
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [table[0] for table in cursor.fetchall()]

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.
        
        Args:
            table_name: Table name to check
            
        Returns:
            bool: True if table exists
        """
        return table_name in self.list_tables()

    def load_table(self, table_name: str) -> pd.DataFrame:
        """Load a table as DataFrame.
        
        Args:
            table_name: Table name to load
            
        Returns:
            pd.DataFrame: Table data
            
        Raises:
            ValueError: If table does not exist
        """
        if not self.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist in database")

        with self.connection() as conn:
            return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    def save_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False
    ) -> None:
        """Save DataFrame to table.
        
        Args:
            df: DataFrame to save
            table_name: Target table name
            if_exists: 'replace' or 'append'
            index: Include DataFrame index as column
        """
        with self.connection() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists=if_exists,
                index=index
            )

    def backup(self, backup_dir: Path, prefix: str = "") -> Path:
        """Create database backup with timestamp.
        
        Args:
            backup_dir: Directory for backup files
            prefix: Optional prefix for backup filename
            
        Returns:
            Path: Path to backup file
        """
        kst = pytz.timezone('Asia/Seoul')
        timestamp = datetime.now(kst).strftime('%Y%m%d%H%M%S')

        backup_dir = Path(backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{prefix}{timestamp}.db" if prefix else f"{timestamp}.db"
        backup_path = backup_dir / filename

        shutil.copy2(str(self.db_path), str(backup_path))
        return backup_path

    def execute(self, query: str, params: tuple = ()) -> None:
        """Execute a query without returning results.
        
        Args:
            query: SQL query
            params: Query parameters
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()

    def fetch_one(self, query: str, params: tuple = ()) -> Optional[tuple]:
        """Execute query and return single row.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            Optional[tuple]: Single row or None
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchone()

    def fetch_all(self, query: str, params: tuple = ()) -> List[tuple]:
        """Execute query and return all rows.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            List[tuple]: All rows
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()

    def get_team_standings(self, date: Optional[str] = None) -> pd.DataFrame:
        """Get fantasy team standings for a date.
        
        Args:
            date: Date string (YYYY-MM-DD). None for latest.
            
        Returns:
            DataFrame with team standings
        """
        query = """
            SELECT 
                tw.team_id,
                ft.name as team_name,
                tw.total_war,
                tw.war_diff,
                tw.rank,
                tw.date
            FROM team_war_daily tw
            JOIN fantasy_teams ft ON tw.team_id = ft.id
            WHERE tw.date = COALESCE(?, (SELECT MAX(date) FROM team_war_daily))
            ORDER BY tw.rank
        """
        with self.connection() as conn:
            return pd.read_sql_query(query, conn, params=(date,))

    def get_player_war(
        self,
        player_id: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get player WAR data.
        
        Args:
            player_id: Filter by player ID
            date_from: Start date (inclusive)
            date_to: End date (inclusive)
            limit: Max rows to return
            
        Returns:
            DataFrame with WAR data
        """
        query = """
            SELECT 
                w.player_id,
                p.name as player_name,
                p.player_type,
                w.date,
                w.war,
                w.war_diff
            FROM war_daily w
            JOIN players p ON w.player_id = p.id
            WHERE 1=1
        """
        params: List = []

        if player_id:
            query += " AND w.player_id = ?"
            params.append(player_id)
        if date_from:
            query += " AND w.date >= ?"
            params.append(date_from)
        if date_to:
            query += " AND w.date <= ?"
            params.append(date_to)

        query += f" ORDER BY w.date DESC, w.war DESC LIMIT {limit}"

        with self.connection() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_roster(self, team_id: str, date: Optional[str] = None) -> pd.DataFrame:
        """Get roster for a team.
        
        Args:
            team_id: Fantasy team ID
            date: Date to check (None for current roster)
            
        Returns:
            DataFrame with roster players and their WAR
        """
        if date:
            query = """
                SELECT 
                    r.player_id,
                    p.name as player_name,
                    p.player_type,
                    d.round as draft_round,
                    w.war,
                    w.war_diff
                FROM roster r
                JOIN players p ON r.player_id = p.id
                LEFT JOIN draft d ON d.player_id = p.id AND d.team_id = r.team_id
                LEFT JOIN war_daily w ON w.player_id = p.id AND w.date = ?
                WHERE r.team_id = ?
                  AND r.joined_date <= ?
                  AND (r.left_date IS NULL OR r.left_date > ?)
                ORDER BY d.pick_order
            """
            params = (date, team_id, date, date)
        else:
            query = """
                SELECT 
                    r.player_id,
                    p.name as player_name,
                    p.player_type,
                    d.round as draft_round,
                    w.war,
                    w.war_diff
                FROM roster r
                JOIN players p ON r.player_id = p.id
                LEFT JOIN draft d ON d.player_id = p.id AND d.team_id = r.team_id
                LEFT JOIN (
                    SELECT player_id, war, war_diff 
                    FROM war_daily 
                    WHERE date = (SELECT MAX(date) FROM war_daily)
                ) w ON w.player_id = p.id
                WHERE r.team_id = ? AND r.left_date IS NULL
                ORDER BY d.pick_order
            """
            params = (team_id,)

        with self.connection() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_daily_records(
        self,
        record_type: Optional[str] = None,
        date: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get GOAT/BOAT records.
        
        Args:
            record_type: 'GOAT' or 'BOAT' (None for both)
            date: Filter by date
            limit: Max rows to return
            
        Returns:
            DataFrame with daily records
        """
        query = """
            SELECT 
                dr.date,
                dr.record_type,
                dr.team_id,
                ft.name as team_name,
                p.name as player_name,
                dr.war_diff
            FROM daily_records dr
            JOIN players p ON dr.player_id = p.id
            LEFT JOIN fantasy_teams ft ON dr.team_id = ft.id
            WHERE 1=1
        """
        params: List = []

        if record_type:
            query += " AND dr.record_type = ?"
            params.append(record_type)
        if date:
            query += " AND dr.date = ?"
            params.append(date)

        query += f" ORDER BY dr.date DESC, dr.war_diff DESC LIMIT {limit}"

        with self.connection() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_transactions(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get transaction history.
        
        Args:
            date_from: Start date
            date_to: End date
            limit: Max rows
            
        Returns:
            DataFrame with transactions
        """
        query = """
            SELECT 
                t.transaction_date,
                p.name as player_name,
                ft_from.name as from_team,
                ft_to.name as to_team,
                t.war_at_transaction
            FROM transactions t
            JOIN players p ON t.player_id = p.id
            LEFT JOIN fantasy_teams ft_from ON t.from_team_id = ft_from.id
            LEFT JOIN fantasy_teams ft_to ON t.to_team_id = ft_to.id
            WHERE 1=1
        """
        params: List = []

        if date_from:
            query += " AND t.transaction_date >= ?"
            params.append(date_from)
        if date_to:
            query += " AND t.transaction_date <= ?"
            params.append(date_to)

        query += f" ORDER BY t.transaction_date DESC LIMIT {limit}"

        with self.connection() as conn:
            return pd.read_sql_query(query, conn, params=params)


# Standalone functions for backward compatibility

def get_connection(db_path: Path) -> sqlite3.Connection:
    """Get database connection (legacy function)."""
    return sqlite3.connect(str(db_path))


def list_tables(conn: sqlite3.Connection) -> List[str]:
    """List all tables (legacy function)."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [table[0] for table in cursor.fetchall()]


def load_table(table_name: str, db_path: Path) -> pd.DataFrame:
    """Load table as DataFrame (legacy function)."""
    db = DatabaseManager(db_path)
    return db.load_table(table_name)


def save_table(
    df: pd.DataFrame,
    table_name: str,
    db_path: Path,
    if_exists: str = "replace",
    index: bool = False
) -> None:
    """Save DataFrame to table (legacy function)."""
    db = DatabaseManager(db_path)
    db.save_table(df, table_name, if_exists=if_exists, index=index)


def backup_database(db_path: Path, backup_dir: Path) -> Path:
    """Create database backup (legacy function)."""
    db = DatabaseManager(db_path)
    return db.backup(backup_dir)


__all__ = [
    "DatabaseManager",
    "get_connection",
    "list_tables",
    "load_table",
    "save_table",
    "backup_database",
]
