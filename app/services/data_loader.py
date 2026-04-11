"""Data loading service for Untatiz - player data and transactions.

This module provides functions for loading player data from the new Long format schema.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Tuple

import numpy as np
import pandas as pd

from app.core.db import DatabaseManager
from app.core.draft_loader import get_draft_slots

if TYPE_CHECKING:
    pass


def _get_active_season_id(db: DatabaseManager) -> int:
    result = db.fetch_one("SELECT id FROM seasons WHERE is_active = 1")
    if result is None:
        raise ValueError("No active season found")
    return int(result[0])


def _get_team_order(conn, season_id: int) -> list[str]:
    draft_teams = pd.read_sql_query(
        """
        SELECT team_id as id
        FROM draft
        WHERE season_id = ?
        GROUP BY team_id
        ORDER BY MIN(pick_order)
        """,
        conn,
        params=(season_id,),
    )['id'].tolist()
    if draft_teams:
        return draft_teams

    roster_teams = pd.read_sql_query(
        """
        SELECT team_id as id
        FROM roster
        WHERE season_id = ?
        GROUP BY team_id
        ORDER BY MIN(joined_date), MIN(id)
        """,
        conn,
        params=(season_id,),
    )['id'].tolist()
    if roster_teams:
        return roster_teams

    return pd.read_sql_query(
        "SELECT id FROM fantasy_teams WHERE id != '퐈' ORDER BY rowid",
        conn,
    )['id'].tolist()


def load_data(
    db_path: Path,
    season_id: int | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load player data and process transactions.
    
    Loads draft data from database and applies player transactions
    to determine current roster state.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Tuple containing:
            - player_name: DataFrame with player names indexed by team
            - player_id: DataFrame with player IDs indexed by team
            - player_activation: DataFrame with activation status indexed by team
            - war_basis: DataFrame with WAR basis values indexed by team
            - transactions_df: DataFrame with transaction history
    """
    db = DatabaseManager(db_path)
    active_season_id = season_id or _get_active_season_id(db)

    with db.connection() as conn:
        season_year = pd.read_sql_query(
            "SELECT year FROM seasons WHERE id = ?",
            conn,
            params=(active_season_id,),
        ).iloc[0, 0]

        # Load draft data (new schema)
        draft_df = pd.read_sql_query("""
            SELECT d.team_id, d.round as slot, d.player_id, p.name
            FROM draft d
            LEFT JOIN players p ON d.player_id = p.id
            WHERE d.season_id = ?
            ORDER BY d.team_id, d.pick_order
        """, conn, params=(active_season_id,))

        transactions_df = pd.read_sql_query("""
            SELECT t.id as tx_id, t.transaction_date as date, p.name, t.player_id as player_id,
                   COALESCE(t.from_team_id, '퐈') as old,
                   COALESCE(t.to_team_id, '퐈') as new,
                   COALESCE(w.war, 0.0) as WAR
            FROM transactions t
            LEFT JOIN players p ON t.player_id = p.id
            LEFT JOIN war_daily w ON w.player_id = t.player_id 
                AND w.date = (
                    SELECT MAX(date) FROM war_daily 
                    WHERE player_id = t.player_id 
                    AND season_id = ?
                    AND date < REPLACE(t.transaction_date, '/', '-')
                )
                AND w.season_id = ?
            WHERE t.season_id = ?
            ORDER BY t.transaction_date, t.id
        """, conn, params=(active_season_id, active_season_id, active_season_id))

        # Get team order
        teams = _get_team_order(conn, active_season_id)

    # Pivot draft data to wide format (for backward compatibility)
    # Get dynamic slot order from database
    db = DatabaseManager(db_path)
    slots = get_draft_slots(db, active_season_id)
    
    if not slots:
        slots = ['용투1', '용투2', '용타', '아쿼'] + [f'{i}R' for i in range(1, 26)]

    player_name = pd.DataFrame(index=teams, columns=slots)
    player_id = pd.DataFrame(index=teams, columns=slots)

    for _, row in draft_df.iterrows():
        team = row['team_id']
        slot = row['slot']
        if team in player_name.index and slot in player_name.columns:
            player_name.loc[team, slot] = row['name']
            player_id.loc[team, slot] = str(row['player_id'])

    # Set index name
    player_name.index.name = '팀'
    player_id.index.name = '팀'

    # Initialize activation and war_basis
    player_activation = player_id.copy()
    player_activation.loc[:, :] = True

    war_basis = player_id.copy()
    war_basis.loc[:, :] = 0.0

    # Convert types
    player_id = player_id.astype('str')
    transactions_df = transactions_df.astype('str')
    transactions_df["WAR"] = transactions_df["WAR"].astype(float)

    # Process transactions
    for index in transactions_df.index:
        row = transactions_df.iloc[index]
        name = row["name"]
        pid = row["player_id"]
        old = row["old"]
        new = row["new"]
        war = float(row["WAR"])

        # Handle player leaving a team
        if old != "퐈" and old in player_id.index:
            data = player_id.loc[old]
            matching = data[data == pid].index.tolist()
            if matching:
                old_position = matching[0]
                player_activation.loc[old, old_position] = False
                war_basis.loc[old, old_position] = war - float(war_basis.loc[old, old_position])

        # Handle player joining a team
        if new != "퐈" and new in player_id.index:
            if (player_id.loc[new] == pid).any():
                # Player exists on team - reactivate
                matching_pos = player_id.loc[new, player_id.loc[new] == pid].index.tolist()
                if matching_pos and not player_activation.loc[new, matching_pos[0]]:
                    new_position = matching_pos[0]
                    player_activation.loc[new, new_position] = True
                    war_basis.loc[new, new_position] = war - float(war_basis.loc[new, new_position])
            else:
                # New player - find empty slot or create new column
                if not pd.isna(player_id.loc[new].iloc[-1]) and player_id.loc[new].iloc[-1] != 'nan':
                    # Need new column
                    new_col = "추가" + str(len(player_id.columns) - 27)
                    player_id[new_col] = np.nan
                    player_name[new_col] = np.nan
                    player_activation[new_col] = np.nan
                    war_basis[new_col] = np.nan

                    player_id[new_col] = player_id[new_col].astype(object)
                    player_name[new_col] = player_name[new_col].astype(object)
                    player_activation[new_col] = player_activation[new_col].astype(object)
                    war_basis[new_col] = war_basis[new_col].astype(object)

                # Find first empty slot
                empty_slots = player_id.loc[new, (player_id.loc[new].isna()) | (player_id.loc[new] == 'nan')].index.tolist()
                if empty_slots:
                    new_position = empty_slots[0]
                    player_id.loc[new, new_position] = pid
                    player_name.loc[new, new_position] = name
                    player_activation.loc[new, new_position] = True
                    war_basis.loc[new, new_position] = war

    return player_name, player_id, player_activation, war_basis, transactions_df
__all__ = [
    "load_data",
]
