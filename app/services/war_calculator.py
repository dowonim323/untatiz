"""WAR calculation service for Untatiz.

Includes FA WAR calculation with proper handling of:
- WAR accumulated only during FA periods
- Position-based roster requirements
- Supplemental draft bonus slots
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, cast

import numpy as np
import pandas as pd

from app.core.db import DatabaseManager


class _ConnectionDatabaseManagerAdapter:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def fetch_one(self, query: str, params: tuple = ()) -> tuple | None:
        return self.conn.execute(query, params).fetchone()

    def fetch_all(self, query: str, params: tuple = ()) -> list[tuple]:
        return self.conn.execute(query, params).fetchall()


def _as_db_manager(db: DatabaseManager | sqlite3.Connection) -> DatabaseManager | _ConnectionDatabaseManagerAdapter:
    if isinstance(db, DatabaseManager):
        return db
    return _ConnectionDatabaseManagerAdapter(db)


@dataclass
class FAConfig:
    """Configuration for FA WAR calculation."""
    roster_size: int = 29
    supplemental_bonus: int = 5
    min_pitchers: int = 11
    min_catchers: int = 2
    min_infielders: int = 7
    min_outfielders: int = 5


@dataclass
class PlayerFAWar:
    """Player's FA WAR data."""
    player_id: str
    name: str
    position: Optional[str]
    position_group: str  # 'P', 'C', 'IF', 'OF', 'NONE'
    current_war: float
    fa_war: float  # WAR accumulated during FA periods


def get_fa_config(db: DatabaseManager, season_id: int) -> FAConfig:
    """Load FA configuration for a season.
    
    Args:
        db: DatabaseManager instance
        season_id: Season ID
        
    Returns:
        FAConfig with roster requirements
    """
    result = db.fetch_one(
        """SELECT roster_size, supplemental_bonus, min_pitchers, 
                  min_catchers, min_infielders, min_outfielders
           FROM fa_config WHERE season_id = ?""",
        (season_id,)
    )
    
    if result:
        return FAConfig(
            roster_size=result[0],
            supplemental_bonus=result[1],
            min_pitchers=result[2],
            min_catchers=result[3],
            min_infielders=result[4],
            min_outfielders=result[5]
        )
    
    return FAConfig()  # Default values


def get_position_group(position: Optional[str]) -> str:
    """Map position to position group.
    
    Args:
        position: Raw position (P, C, 1B, 2B, SS, 3B, LF, CF, RF, DH, None)
        
    Returns:
        Position group: 'P', 'C', 'IF', 'OF', or 'NONE'
    """
    if position is None:
        return 'NONE'
    
    pos = position.upper()
    
    if pos == 'P':
        return 'P'
    elif pos == 'C':
        return 'C'
    elif pos in ('1B', '2B', 'SS', '3B', 'IF'):
        return 'IF'
    elif pos in ('LF', 'CF', 'RF', 'OF'):
        return 'OF'
    else:  # DH or unknown
        return 'NONE'


def is_currently_rostered(db: DatabaseManager, player_id: str, season_id: int) -> bool:
    """Check if player is currently on a fantasy team roster.
    
    Args:
        db: DatabaseManager instance
        player_id: Player ID
        season_id: Season ID
        
    Returns:
        True if player is on a roster (left_date is NULL)
    """
    result = db.fetch_one(
        """SELECT 1 FROM roster 
           WHERE player_id = ? AND season_id = ? AND left_date IS NULL""",
        (player_id, season_id)
    )
    return result is not None


def is_rostered_on_date(
    db: DatabaseManager,
    player_id: str,
    season_id: int,
    target_date: str,
) -> bool:
    result = db.fetch_one(
        """SELECT 1 FROM roster
           WHERE player_id = ?
             AND season_id = ?
             AND joined_date <= ?
             AND (left_date IS NULL OR left_date > ?)
           LIMIT 1""",
        (player_id, season_id, target_date, target_date),
    )
    return result is not None


def get_player_application_date(
    db: DatabaseManager,
    player_id: str,
    season_id: int,
) -> str | None:
    result = db.fetch_one(
        """SELECT MIN(application_date)
           FROM draft
           WHERE player_id = ? AND season_id = ? AND application_date IS NOT NULL""",
        (player_id, season_id),
    )
    return result[0] if result and result[0] else None


def get_war_on_or_before(
    db: DatabaseManager,
    player_id: str,
    season_id: int,
    target_date: str,
) -> float:
    result = db.fetch_one(
        """SELECT war FROM war_daily
           WHERE player_id = ? AND season_id = ? AND date <= ?
           ORDER BY date DESC, id DESC LIMIT 1""",
        (player_id, season_id, target_date),
    )
    return float(result[0]) if result and result[0] is not None else 0.0


def get_war_before(
    db: DatabaseManager,
    player_id: str,
    season_id: int,
    target_date: str,
) -> float:
    result = db.fetch_one(
        """SELECT war FROM war_daily
           WHERE player_id = ? AND season_id = ? AND date < ?
           ORDER BY date DESC, id DESC LIMIT 1""",
        (player_id, season_id, target_date),
    )
    return float(result[0]) if result and result[0] is not None else 0.0


def calculate_player_fa_war(
    db: DatabaseManager,
    player_id: str,
    current_war: float,
    season_id: int,
    season_start_date: str,
    target_date: str | None = None,
) -> float:
    """Calculate FA WAR for a single player.
    
    FA WAR = WAR accumulated only during periods when player was not on any team.
    
    Cases:
    1. Never drafted: Full current WAR is FA WAR
    2. Released from team: current_war - war_at_release = FA WAR for that period
    3. Re-acquired then released again: Sum of all FA periods
    
    Args:
        db: DatabaseManager instance
        player_id: Player ID
        current_war: Player's current total WAR
        season_id: Season ID
        season_start_date: Season start date (YYYY-MM-DD)
        
    Returns:
        FA WAR value
    """
    # Get all transactions for this player, ordered by date
    # Join with war_daily to get WAR at transaction date (with fallback to closest previous date)
    transactions_query = (
        """SELECT t.from_team_id, t.to_team_id, 
                   COALESCE(w.war, 0.0) as war_at_transaction, 
                   t.transaction_date
           FROM transactions t
            LEFT JOIN war_daily w ON w.player_id = t.player_id 
                AND w.date = (
                    SELECT MAX(date) FROM war_daily 
                    WHERE player_id = t.player_id 
                    AND season_id = ?
                    AND date < REPLACE(t.transaction_date, '/', '-')
                )
               AND w.season_id = ?
            WHERE t.player_id = ?
             AND t.season_id = ?
             AND t.transaction_date >= ?"""
    )
    params: tuple = (season_id, season_id, player_id, season_id, season_start_date)
    if target_date is not None:
        transactions_query += " AND t.transaction_date <= ?"
        params = (*params, target_date)
    transactions_query += " ORDER BY t.transaction_date ASC, t.id ASC"
    transactions = db.fetch_all(
        transactions_query,
        params,
    )

    application_date = get_player_application_date(db, player_id, season_id)

    if not transactions and application_date is None:
        return current_war

    if target_date is not None and application_date is not None and target_date < application_date:
        return round(current_war, 2)

    fa_war_total = 0.0
    last_release_war = None
    currently_fa = application_date is None

    if application_date is not None:
        fa_war_total = get_war_before(db, player_id, season_id, application_date)

    for from_team, to_team, war_at_tx, _ in transactions:
        if from_team is not None and to_team is None:
            last_release_war = war_at_tx
            currently_fa = True
        elif from_team is None and to_team is not None:
            if currently_fa and last_release_war is not None:
                fa_war_total += war_at_tx - last_release_war
            elif currently_fa and last_release_war is None and application_date is None:
                fa_war_total += war_at_tx
            currently_fa = False
            last_release_war = None
        elif from_team is not None and to_team is not None:
            currently_fa = False
            last_release_war = None

    if currently_fa and last_release_war is not None:
        fa_war_total += current_war - last_release_war
    elif currently_fa and last_release_war is None and application_date is None:
        fa_war_total = current_war

    return round(fa_war_total, 2)


def get_latest_player_war_rows(
    db: DatabaseManager,
    season_id: int,
    target_date: str,
) -> list[tuple[str, str, str | None, float]]:
    return [
        (str(player_id), name, position, float(war or 0.0))
        for player_id, name, position, war in db.fetch_all(
            """SELECT p.id, p.name, p.position, w.war
               FROM players p
               JOIN war_daily w ON w.player_id = p.id
               WHERE w.season_id = ?
                 AND w.date = (
                     SELECT MAX(w2.date)
                     FROM war_daily w2
                     WHERE w2.player_id = p.id
                       AND w2.season_id = ?
                       AND w2.date <= ?
                 )""",
            (season_id, season_id, target_date),
        )
    ]


def build_fa_players_from_frames(
    db: DatabaseManager,
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    season_id: int,
    season_start_date: str,
    current_date: str,
) -> List[PlayerFAWar]:
    fa_players = []
    player_ids = sorted({str(player_id) for player_id in bat.index} | {str(player_id) for player_id in pit.index})

    for player_id_str in player_ids:
        if is_rostered_on_date(db, player_id_str, season_id, current_date):
            continue

        bat_row = bat.loc[player_id_str] if player_id_str in bat.index else None
        pit_row = pit.loc[player_id_str] if player_id_str in pit.index else None

        current_war = 0.0
        name = 'Unknown'
        position: Optional[str] = None

        if bat_row is not None:
            current_war += float(bat_row.get('oWAR', 0) or 0.0)
            name = bat_row.get('Name', name)
            position = bat_row.get('POS', position)

        if pit_row is not None:
            current_war += float(pit_row.get('WAR', 0) or 0.0)
            if name == 'Unknown':
                name = pit_row.get('Name', name)
            if position is None:
                position = 'P'

        fa_war = calculate_player_fa_war(
            db,
            player_id_str,
            current_war,
            season_id,
            season_start_date,
            current_date,
        )
        if fa_war <= 0:
            continue

        fa_players.append(
            PlayerFAWar(
                player_id=player_id_str,
                name=name,
                position=position,
                position_group=get_position_group(position),
                current_war=current_war,
                fa_war=fa_war,
            )
        )

    return fa_players


def get_all_fa_players(
    db: DatabaseManager,
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    season_id: int,
    season_start_date: str,
    current_date: str,
) -> List[PlayerFAWar]:
    """Get all players currently in FA status with their FA WAR.
    
    Args:
        db: DatabaseManager instance
        bat: Batter statistics DataFrame
        pit: Pitcher statistics DataFrame
        season_id: Season ID
        season_start_date: Season start date
        
    Returns:
        List of PlayerFAWar objects for all current FA players
    """
    rows = get_latest_player_war_rows(db, season_id, current_date)

    if not rows:
        return build_fa_players_from_frames(db, bat, pit, season_id, season_start_date, current_date)

    fa_players = []
    for player_id, name, position, war in rows:
        if is_rostered_on_date(db, player_id, season_id, current_date):
            continue

        fa_war = calculate_player_fa_war(
            db,
            player_id,
            war,
            season_id,
            season_start_date,
            current_date,
        )
        if fa_war <= 0:
            continue

        fa_players.append(
            PlayerFAWar(
                player_id=player_id,
                name=name,
                position=position,
                position_group=get_position_group(position),
                current_war=war,
                fa_war=fa_war,
            )
        )

    return fa_players


def select_fa_roster(
    fa_players: List[PlayerFAWar],
    config: FAConfig,
    is_supplemental_active: bool
) -> List[PlayerFAWar]:
    """Select FA roster based on position requirements.
    
    Selection process:
    1. For each position group, select top N players by FA WAR
    2. Fill remaining slots with best available (any position)
    3. If supplemental draft is active, add bonus slots
    
    Args:
        fa_players: List of all FA players
        config: FA configuration
        is_supplemental_active: Whether supplemental draft bonus applies
        
    Returns:
        Selected FA roster players
    """
    selected = []
    remaining = list(fa_players)
    
    # Sort all by FA WAR descending
    remaining.sort(key=lambda p: p.fa_war, reverse=True)
    
    # Group players by position
    by_position = {
        'P': [p for p in remaining if p.position_group == 'P'],
        'C': [p for p in remaining if p.position_group == 'C'],
        'IF': [p for p in remaining if p.position_group == 'IF'],
        'OF': [p for p in remaining if p.position_group == 'OF'],
    }
    
    # Sort each group by FA WAR
    for group in by_position.values():
        group.sort(key=lambda p: p.fa_war, reverse=True)
    
    # Select minimum required from each position
    position_mins = {
        'P': config.min_pitchers,
        'C': config.min_catchers,
        'IF': config.min_infielders,
        'OF': config.min_outfielders,
    }
    
    for pos_group, min_count in position_mins.items():
        players = by_position[pos_group]
        for i in range(min(min_count, len(players))):
            selected.append(players[i])
    
    # Remove selected from remaining
    selected_ids = {p.player_id for p in selected}
    remaining = [p for p in remaining if p.player_id not in selected_ids]
    
    # Calculate how many more spots to fill
    base_roster = config.roster_size
    if is_supplemental_active:
        base_roster += config.supplemental_bonus
    
    spots_to_fill = base_roster - len(selected)
    
    # Fill with best available
    for i in range(min(spots_to_fill, len(remaining))):
        selected.append(remaining[i])
    
    return selected


def has_supplemental_draft(db: DatabaseManager, season_id: int, current_date: str) -> bool:
    """Check if supplemental draft is active (date >= supplemental application_date).
    
    Args:
        db: DatabaseManager instance
        season_id: Season ID
        current_date: Current date (YYYY-MM-DD)
        
    Returns:
        True if supplemental draft bonus should apply
    """
    result = db.fetch_one(
        """SELECT application_date FROM draft 
           WHERE season_id = ? AND draft_type = 'supplemental'
           LIMIT 1""",
        (season_id,)
    )
    
    if result and result[0]:
        return current_date >= result[0]
    
    return False


def get_season_start_date(db: DatabaseManager, season_id: int) -> str:
    """Get the main draft application_date as season start.
    
    Args:
        db: DatabaseManager instance
        season_id: Season ID
        
    Returns:
        Season start date (YYYY-MM-DD)
    """
    result = db.fetch_one(
        """SELECT application_date FROM draft 
           WHERE season_id = ? AND draft_type = 'main'
           LIMIT 1""",
        (season_id,)
    )
    
    if result and result[0]:
        return result[0]
    
    result = db.fetch_one("SELECT year FROM seasons WHERE id = ?", (season_id,))
    season_year = result[0] if result else 2026
    return f"{season_year}-03-01"


def calculate_fa_war(
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    player_id: pd.DataFrame,
    player_activation: pd.DataFrame,
    current_war: pd.DataFrame,
    db_path: str = "/home/ubuntu/untatiz/db/untatiz_db.db",
    current_date: str | None = None,
    season_id: int | None = None,
) -> float:
    """Calculate total WAR for Free Agent team (퐈).
    
    This is the main entry point called by the scraper.
    
    FA WAR calculation:
    1. Find all players currently not on any roster
    2. Calculate each player's "FA WAR" (WAR accumulated only during FA periods)
    3. Select roster based on position requirements
    4. Sum the FA WAR of selected players
    
    Args:
        bat: Batter statistics DataFrame
        pit: Pitcher statistics DataFrame
        player_id: Player ID DataFrame indexed by team (legacy, for compatibility)
        player_activation: Player activation status (legacy, for compatibility)
        current_war: Current WAR DataFrame indexed by team (legacy, for compatibility)
        db_path: Path to database
        current_date: Current date for supplemental check (auto-detected if None)
        
    Returns:
        float: Total FA WAR
    """
    from app.core.utils import get_date
    from app.scraper.jobs import _parse_date_to_iso
    
    db = DatabaseManager(Path(db_path))
    
    if season_id is None:
        result = db.fetch_one("SELECT id FROM seasons WHERE is_active = 1")
        season_id = result[0] if result else 1
    
    # Get current date
    if current_date is None:
        current_date = _parse_date_to_iso(get_date())
    
    return calculate_fa_total_as_of(db, season_id, current_date, bat=bat, pit=pit)


def calculate_fa_total_as_of(
    db: DatabaseManager | sqlite3.Connection,
    season_id: int,
    current_date: str,
    bat: pd.DataFrame | None = None,
    pit: pd.DataFrame | None = None,
) -> float:
    db_manager = _as_db_manager(db)
    typed_db_manager = cast(DatabaseManager, db_manager)
    bat_frame = bat if bat is not None else pd.DataFrame()
    pit_frame = pit if pit is not None else pd.DataFrame()
    season_start = get_season_start_date(typed_db_manager, season_id)
    config = get_fa_config(typed_db_manager, season_id)
    is_supplemental = has_supplemental_draft(typed_db_manager, season_id, current_date)
    fa_players = get_all_fa_players(
        typed_db_manager,
        bat_frame,
        pit_frame,
        season_id,
        season_start,
        current_date,
    )
    roster = select_fa_roster(fa_players, config, is_supplemental)
    return round(sum(player.fa_war for player in roster), 2)


# Legacy functions for backward compatibility

def isactive(
    player_id: str, 
    player_id_df: pd.DataFrame, 
    player_activation: pd.DataFrame
) -> bool:
    """Check if a player is currently active on any team.
    
    Args:
        player_id: Player ID to check
        player_id_df: DataFrame with player IDs indexed by team
        player_activation: DataFrame with activation status indexed by team
        
    Returns:
        bool: True if player is active on at least one team
    """
    positions = player_id_df.apply(lambda col: col[col == player_id].index.tolist()).dropna()
    activation = False
    
    for col in positions.index:
        for row in positions[col]:
            if player_activation.at[row, col]:
                activation = True
                break
        if activation:
            break
    
    return activation


def get_war(
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    player_id: pd.DataFrame,
    player_activation: pd.DataFrame,
    war_basis: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate live and current WAR for all players.
    
    Live WAR: Current season WAR values from statiz
    Current WAR: Adjusted WAR based on when player was active on team
    
    Args:
        bat: Batter statistics DataFrame (indexed by ID)
        pit: Pitcher statistics DataFrame (indexed by ID)
        player_id: Player ID DataFrame indexed by team
        player_activation: Player activation status DataFrame indexed by team
        war_basis: WAR basis values DataFrame indexed by team
        
    Returns:
        Tuple of (live_war, current_war) DataFrames indexed by team
    """
    live_war = player_id.copy()
    
    # Calculate live WAR for each player position
    for team in live_war.index:
        for column in live_war.columns:
            player_id_val = live_war.loc[team, column]

            if isinstance(player_id_val, (pd.Series, pd.DataFrame, np.ndarray, pd.Index)):
                live_war.loc[team, column] = np.nan
                continue

            try:
                numeric_player_id = float(str(player_id_val))
            except (TypeError, ValueError):
                live_war.loc[team, column] = np.nan
                continue

            if np.isnan(numeric_player_id):
                live_war.loc[team, column] = np.nan
            elif numeric_player_id == 0:
                live_war.loc[team, column] = 0
            else:
                # Get oWAR from bat and WAR from pit
                bat_war = float(bat.loc[player_id_val, "oWAR"]) if player_id_val in bat.index else 0
                pit_war = float(pit.loc[player_id_val, "WAR"]) if player_id_val in pit.index else 0
                live_war.loc[team, column] = bat_war + pit_war
    
    # Calculate current WAR based on activation status
    current_war = live_war.copy()
    
    for team in current_war.index:
        for column in current_war.columns:
            war = live_war.loc[team, column]
            activation_val = player_activation.loc[team, column]
            
            if pd.isna(activation_val):
                current_war.loc[team, column] = np.nan
            elif activation_val:
                # Active: current WAR minus basis (what they had when acquired)
                current_war.loc[team, column] = war - war_basis.loc[team, column]
            else:
                # Inactive: use basis value (what they had when released)
                current_war.loc[team, column] = war_basis.loc[team, column]
    
    return live_war, current_war


__all__ = [
    "FAConfig",
    "PlayerFAWar",
    "get_fa_config",
    "calculate_fa_war",
    "calculate_fa_total_as_of",
    "calculate_player_fa_war",
    "get_all_fa_players",
    "get_latest_player_war_rows",
    "build_fa_players_from_frames",
    "select_fa_roster",
    "isactive",
    "get_war",
]
