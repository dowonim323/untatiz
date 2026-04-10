"""Database migration script: Wide format -> Long format.

This script migrates the legacy wide-format tables (where each date is a column)
to the new normalized Long format (one row per player per date).

Usage:
    python -m app.core.migrate [--dry-run] [--verify]
    
Options:
    --dry-run: Preview changes without writing to database
    --verify: Run integrity checks after migration
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pytz

from app.core.schema import (
    DRAFT_ROUNDS,
    FANTASY_TEAMS,
    init_fantasy_teams,
    init_schema,
    init_season,
)

logger = logging.getLogger(__name__)

# Regex pattern for date columns (MM/DD format)
DATE_COL_PATTERN = re.compile(r'^\d{2}/\d{2}$')


def parse_date_col(col: str, year: int = 2025) -> Optional[str]:
    """Convert MM/DD column to YYYY-MM-DD format.
    
    Args:
        col: Column name in MM/DD format
        year: Year to use (defaults to 2025)
        
    Returns:
        Date string in YYYY-MM-DD format, or None if not a date column
    """
    if not DATE_COL_PATTERN.match(col):
        return None
    month, day = col.split('/')
    return f"{year}-{month}-{day}"


def migrate_players(conn: sqlite3.Connection, dry_run: bool = False) -> int:
    """Migrate players from bat/pit tables to players table.
    
    Args:
        conn: Database connection
        dry_run: If True, don't commit changes
        
    Returns:
        Number of players migrated
    """
    logger.info("Migrating players...")
    
    cursor = conn.cursor()
    count = 0
    
    # Migrate batters
    cursor.execute("SELECT ID, Name FROM bat")
    for row in cursor.fetchall():
        player_id, name = row
        if not dry_run:
            cursor.execute(
                """INSERT OR IGNORE INTO players (id, name, player_type)
                   VALUES (?, ?, 'bat')""",
                (str(player_id), name)
            )
        count += 1
    
    # Migrate pitchers
    cursor.execute("SELECT ID, Name FROM pit")
    for row in cursor.fetchall():
        player_id, name = row
        if not dry_run:
            cursor.execute(
                """INSERT OR REPLACE INTO players (id, name, player_type)
                   VALUES (?, ?, 'pit')""",
                (str(player_id), name)
            )
        count += 1
    
    if not dry_run:
        conn.commit()
    
    logger.info(f"Migrated {count} players")
    return count


def migrate_war_daily(
    conn: sqlite3.Connection, 
    season_id: int,
    year: int = 2025,
    dry_run: bool = False
) -> int:
    """Migrate WAR data from wide bat/pit tables to war_daily.
    
    Args:
        conn: Database connection
        season_id: Season ID to associate records with
        year: Year for date conversion
        dry_run: If True, don't commit changes
        
    Returns:
        Number of records migrated
    """
    logger.info("Migrating WAR daily data...")
    
    cursor = conn.cursor()
    count = 0
    
    for table, player_type in [('bat', 'bat'), ('pit', 'pit')]:
        # Get all columns
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Identify date columns
        date_cols = [c for c in columns if DATE_COL_PATTERN.match(c)]
        logger.info(f"Found {len(date_cols)} date columns in {table}")
        
        # Get all player data
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        
        for _, row in df.iterrows():
            player_id = str(row['ID'])
            prev_war = None
            
            for date_col in date_cols:
                war = row[date_col]
                if pd.isna(war):
                    continue
                    
                try:
                    war = float(war)
                except (ValueError, TypeError):
                    continue
                
                date_str = parse_date_col(date_col, year)
                if not date_str:
                    continue
                
                # Calculate diff
                war_diff = None
                if prev_war is not None:
                    war_diff = round(war - prev_war, 2)
                prev_war = war
                
                if not dry_run:
                    cursor.execute(
                        """INSERT OR REPLACE INTO war_daily 
                           (player_id, season_id, date, war, war_diff)
                           VALUES (?, ?, ?, ?, ?)""",
                        (player_id, season_id, date_str, war, war_diff)
                    )
                count += 1
    
    if not dry_run:
        conn.commit()
    
    logger.info(f"Migrated {count} WAR records")
    return count


def migrate_draft(
    conn: sqlite3.Connection,
    season_id: int,
    dry_run: bool = False
) -> int:
    """Migrate draft data from draft_id/draft_name to draft table.
    
    Args:
        conn: Database connection
        season_id: Season ID
        dry_run: If True, don't commit changes
        
    Returns:
        Number of draft picks migrated
    """
    logger.info("Migrating draft data...")
    
    cursor = conn.cursor()
    
    # Load draft_id (has player IDs)
    df_id = pd.read_sql_query("SELECT * FROM draft_id", conn)
    
    count = 0
    for _, row in df_id.iterrows():
        team_id = row['팀']
        if team_id not in FANTASY_TEAMS:
            logger.warning(f"Unknown team: {team_id}")
            continue
        
        for round_name in DRAFT_ROUNDS:
            if round_name not in row:
                continue
            
            player_id = row[round_name]
            if pd.isna(player_id):
                continue
            
            player_id = str(int(player_id))
            pick_order = DRAFT_ROUNDS.index(round_name) + 1
            
            if not dry_run:
                cursor.execute(
                    """INSERT OR REPLACE INTO draft 
                       (season_id, team_id, player_id, round, pick_order)
                       VALUES (?, ?, ?, ?, ?)""",
                    (season_id, team_id, player_id, round_name, pick_order)
                )
            count += 1
    
    if not dry_run:
        conn.commit()
    
    logger.info(f"Migrated {count} draft picks")
    return count


def migrate_roster(
    conn: sqlite3.Connection,
    season_id: int,
    year: int = 2025,
    dry_run: bool = False
) -> int:
    """Migrate roster data to new roster table.
    
    The legacy roster table has comma-separated player IDs per date.
    We need to extract individual player assignments.
    
    Args:
        conn: Database connection
        season_id: Season ID
        year: Year for dates
        dry_run: If True, don't commit changes
        
    Returns:
        Number of roster entries migrated
    """
    logger.info("Migrating roster data...")
    
    cursor = conn.cursor()
    
    # Load draft_id to get initial roster (draft picks)
    df_draft = pd.read_sql_query("SELECT * FROM draft_id", conn)
    
    # Start with draft date as joined_date
    start_date = f"{year}-03-21"  # Season start
    
    count = 0
    for _, row in df_draft.iterrows():
        team_id = row['팀']
        if team_id not in FANTASY_TEAMS or team_id == '퐈':
            continue
        
        for round_name in DRAFT_ROUNDS:
            if round_name not in row:
                continue
            
            player_id = row[round_name]
            if pd.isna(player_id):
                continue
            
            player_id = str(int(player_id))
            
            if not dry_run:
                cursor.execute(
                    """INSERT OR IGNORE INTO roster 
                       (team_id, player_id, season_id, joined_date)
                       VALUES (?, ?, ?, ?)""",
                    (team_id, player_id, season_id, start_date)
                )
            count += 1
    
    if not dry_run:
        conn.commit()
    
    logger.info(f"Migrated {count} initial roster entries (from draft)")
    return count


def migrate_transactions(
    conn: sqlite3.Connection,
    dry_run: bool = False
) -> int:
    """Migrate player_transaction to transactions table.
    
    Args:
        conn: Database connection
        dry_run: If True, don't commit changes
        
    Returns:
        Number of transactions migrated
    """
    logger.info("Migrating transactions...")
    
    cursor = conn.cursor()
    
    df = pd.read_sql_query("SELECT * FROM player_transaction", conn)
    
    count = 0
    for _, row in df.iterrows():
        # Parse date (format: 2025/04/25 -> 2025-04-25)
        date_str = row['date'].replace('/', '-')
        
        player_id = str(row['id'])
        
        # Parse team names (format: "팀 옥" -> "옥")
        from_team = row['old'].replace('팀 ', '') if pd.notna(row['old']) else None
        to_team = row['new'].replace('팀 ', '') if pd.notna(row['new']) else None
        
        # Handle FA (퐈)
        if from_team == '퐈':
            from_team = None
        if to_team == '퐈':
            to_team = None
        
        war_at_transaction = row['WAR'] if pd.notna(row['WAR']) else None
        
        if not dry_run:
            cursor.execute(
                """INSERT INTO transactions 
                   (player_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
                   VALUES (?, ?, ?, ?, ?)""",
                (player_id, from_team, to_team, date_str, war_at_transaction)
            )
            
            # Update roster: mark left_date for player leaving team
            if from_team:
                cursor.execute(
                    """UPDATE roster SET left_date = ?
                       WHERE team_id = ? AND player_id = ? AND left_date IS NULL""",
                    (date_str, from_team, player_id)
                )
            
            # Add new roster entry for player joining team
            if to_team:
                cursor.execute(
                    """SELECT id FROM seasons WHERE is_active = 1"""
                )
                season_row = cursor.fetchone()
                if season_row:
                    active_season_id = season_row[0]
                    cursor.execute(
                        """INSERT OR IGNORE INTO roster 
                           (team_id, player_id, season_id, joined_date)
                           VALUES (?, ?, ?, ?)""",
                        (to_team, player_id, active_season_id, date_str)
                    )
        
        count += 1
    
    if not dry_run:
        conn.commit()
    
    logger.info(f"Migrated {count} transactions")
    return count


def migrate_goat_boat(
    conn: sqlite3.Connection,
    dry_run: bool = False
) -> int:
    """Migrate GOAT/BOAT tables to daily_records.
    
    Args:
        conn: Database connection
        dry_run: If True, don't commit changes
        
    Returns:
        Number of records migrated
    """
    logger.info("Migrating GOAT/BOAT records...")
    
    cursor = conn.cursor()
    count = 0
    
    for table, record_type in [('GOAT', 'GOAT'), ('BOAT', 'BOAT')]:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        except Exception as e:
            logger.warning(f"Could not read {table}: {e}")
            continue
        
        for _, row in df.iterrows():
            team_id = row['소속팀']
            date_str = f"2025-{row['날짜'].replace('/', '-')}"
            player_name = row['이름']
            
            # Parse WAR diff (remove formatting if needed)
            war_diff_str = str(row['WAR 변동'])
            try:
                war_diff = float(war_diff_str)
            except ValueError:
                logger.warning(f"Could not parse WAR diff: {war_diff_str}")
                continue
            
            # Look up player ID by name
            cursor.execute(
                "SELECT id FROM players WHERE name = ?",
                (player_name,)
            )
            player_row = cursor.fetchone()
            if not player_row:
                logger.warning(f"Player not found: {player_name}")
                continue
            
            player_id = player_row[0]
            
            if not dry_run:
                cursor.execute(
                    """INSERT OR REPLACE INTO daily_records 
                       (date, record_type, team_id, player_id, war_diff)
                       VALUES (?, ?, ?, ?, ?)""",
                    (date_str, record_type, team_id, player_id, war_diff)
                )
            count += 1
    
    if not dry_run:
        conn.commit()
    
    logger.info(f"Migrated {count} GOAT/BOAT records")
    return count


def migrate_team_war_daily(
    conn: sqlite3.Connection,
    season_id: int,
    year: int = 2025,
    dry_run: bool = False
) -> int:
    """Migrate teams table to team_war_daily.
    
    Args:
        conn: Database connection
        season_id: Season ID
        year: Year for dates
        dry_run: If True, don't commit changes
        
    Returns:
        Number of records migrated
    """
    logger.info("Migrating team WAR daily data...")
    
    cursor = conn.cursor()
    
    df = pd.read_sql_query("SELECT * FROM teams", conn)
    
    count = 0
    for _, row in df.iterrows():
        team_id = row['팀']
        if team_id not in FANTASY_TEAMS:
            continue
        
        # Get date columns
        date_cols = [c for c in df.columns if DATE_COL_PATTERN.match(c)]
        prev_war = None
        
        for date_col in date_cols:
            war = row[date_col]
            if pd.isna(war):
                continue
            
            try:
                war = float(war)
            except (ValueError, TypeError):
                continue
            
            date_str = parse_date_col(date_col, year)
            if not date_str:
                continue
            
            # Calculate diff
            war_diff = None
            if prev_war is not None:
                war_diff = round(war - prev_war, 2)
            prev_war = war
            
            if not dry_run:
                cursor.execute(
                    """INSERT OR REPLACE INTO team_war_daily 
                       (team_id, season_id, date, total_war, war_diff)
                       VALUES (?, ?, ?, ?, ?)""",
                    (team_id, season_id, date_str, war, war_diff)
                )
            count += 1
    
    if not dry_run:
        conn.commit()
    
    # Update ranks
    if not dry_run:
        cursor.execute("""
            UPDATE team_war_daily SET rank = (
                SELECT COUNT(*) + 1 
                FROM team_war_daily t2 
                WHERE t2.date = team_war_daily.date 
                  AND t2.total_war > team_war_daily.total_war
            )
        """)
        conn.commit()
    
    logger.info(f"Migrated {count} team WAR records")
    return count


def rename_legacy_tables(conn: sqlite3.Connection, dry_run: bool = False) -> list:
    """Rename legacy tables that conflict with new schema.
    
    Args:
        conn: Database connection
        dry_run: If True, don't commit changes
        
    Returns:
        List of renamed tables
    """
    logger.info("Checking for legacy table conflicts...")
    
    cursor = conn.cursor()
    renamed = []
    
    # Tables that conflict with new schema
    conflict_tables = ['roster', 'draft', 'transactions']
    
    for table in conflict_tables:
        # Check if table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        if not cursor.fetchone():
            continue
        
        # Check if it's a legacy table (has Korean column like '팀' or old format)
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Detect legacy format
        is_legacy = False
        if table == 'roster' and '팀' in columns:
            is_legacy = True
        elif table == 'draft' and '팀' in columns:
            is_legacy = True
        elif table == 'transactions' and 'old' in columns:
            is_legacy = True
        
        if is_legacy:
            new_name = f"legacy_{table}"
            logger.info(f"Renaming legacy table: {table} -> {new_name}")
            if not dry_run:
                try:
                    cursor.execute(f"ALTER TABLE {table} RENAME TO {new_name}")
                except sqlite3.OperationalError as e:
                    # Table might already be renamed
                    logger.warning(f"Could not rename {table}: {e}")
            renamed.append(table)
    
    if not dry_run:
        conn.commit()
    
    return renamed


def run_migration(
    db_path: Path,
    year: int = 2025,
    dry_run: bool = False
) -> dict:
    """Run full migration.
    
    Args:
        db_path: Path to database file
        year: Season year
        dry_run: If True, don't commit changes
        
    Returns:
        dict: Migration statistics
    """
    kst = pytz.timezone('Asia/Seoul')
    start_time = datetime.now(kst)
    logger.info(f"Starting migration at {start_time}")
    
    if dry_run:
        logger.info("DRY RUN MODE - no changes will be committed")
    
    conn = sqlite3.connect(str(db_path))
    
    try:
        # Rename legacy tables that conflict with new schema
        renamed = rename_legacy_tables(conn, dry_run)
        
        # Initialize new schema
        logger.info("Initializing new schema...")
        init_schema(conn)
        init_fantasy_teams(conn)
        season_id = init_season(conn, year)
        
        stats: dict = {
            'renamed_legacy_tables': renamed,
            'players': migrate_players(conn, dry_run),
            'war_daily': migrate_war_daily(conn, season_id, year, dry_run),
            'draft': migrate_draft(conn, season_id, dry_run),
            'roster': migrate_roster(conn, season_id, year, dry_run),
            'transactions': migrate_transactions(conn, dry_run),
            'goat_boat': migrate_goat_boat(conn, dry_run),
            'team_war_daily': migrate_team_war_daily(conn, season_id, year, dry_run),
        }
        
        end_time = datetime.now(kst)
        duration = (end_time - start_time).total_seconds()
        
        stats['duration_seconds'] = duration
        stats['dry_run'] = dry_run
        
        logger.info(f"Migration completed in {duration:.2f}s")
        logger.info(f"Stats: {stats}")
        
        return stats
        
    finally:
        conn.close()


def verify_migration(db_path: Path) -> dict:
    """Verify migration integrity.
    
    Args:
        db_path: Path to database file
        
    Returns:
        dict: Verification results
    """
    logger.info("Verifying migration...")
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    results = {}
    
    # Count records in new tables
    for table in ['players', 'war_daily', 'draft', 'roster', 'transactions', 
                  'daily_records', 'team_war_daily', 'fantasy_teams', 'seasons']:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        results[f'{table}_count'] = cursor.fetchone()[0]
    
    # Verify player counts match
    cursor.execute("SELECT COUNT(*) FROM bat")
    bat_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM pit")
    pit_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM players")
    player_count = cursor.fetchone()[0]
    
    # Note: Some pitchers might also bat, so we don't expect exact match
    results['legacy_bat_count'] = bat_count
    results['legacy_pit_count'] = pit_count
    results['new_player_count'] = player_count
    
    # Verify views work
    try:
        cursor.execute("SELECT COUNT(*) FROM v_current_roster")
        results['v_current_roster_works'] = True
    except Exception as e:
        results['v_current_roster_works'] = False
        results['v_current_roster_error'] = str(e)
    
    try:
        cursor.execute("SELECT COUNT(*) FROM v_team_standings")
        results['v_team_standings_works'] = True
    except Exception as e:
        results['v_team_standings_works'] = False
        results['v_team_standings_error'] = str(e)
    
    conn.close()
    
    logger.info(f"Verification results: {results}")
    return results


if __name__ == '__main__':
    import argparse
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='Migrate Untatiz database')
    parser.add_argument('--dry-run', action='store_true', 
                        help='Preview changes without writing')
    parser.add_argument('--verify', action='store_true',
                        help='Run verification after migration')
    parser.add_argument('--db', type=str, 
                        default='/home/ubuntu/untatiz/db/untatiz_db.db',
                        help='Database path')
    parser.add_argument('--year', type=int, default=2025,
                        help='Season year')
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    
    # Run migration
    stats = run_migration(db_path, year=args.year, dry_run=args.dry_run)
    print(f"\nMigration stats: {stats}")
    
    # Verify if requested
    if args.verify and not args.dry_run:
        results = verify_migration(db_path)
        print(f"\nVerification results: {results}")
