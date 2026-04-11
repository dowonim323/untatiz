"""Scraper jobs - database update and backup operations.

Updated to use normalized Long format schema instead of legacy Wide format.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pandas as pd

from app.core.cache import invalidate_after_update
from app.core.db import DatabaseManager
from app.core.utils import get_business_year, get_date, get_kst_now, get_kst_timestamp
from app.services.team_war_daily_writer import write_team_war_daily_for_date
from app.services.war_calculator import calculate_fa_total_as_of, calculate_fa_war

if TYPE_CHECKING:
    pass


def _get_season_id(db: DatabaseManager) -> int:
    """Get active season ID from database."""
    result = db.fetch_one("SELECT id FROM seasons WHERE is_active = 1")
    if result:
        return result[0]
    return 1


def _parse_date_to_iso(date_str: str, year: int | None = None) -> str:
    """Convert MM/DD format to YYYY-MM-DD format."""
    year = year or get_business_year()
    if '/' in date_str:
        month, day = date_str.split('/')
        return f"{year}-{month}-{day}"
    return date_str


def _resolve_connection(
    db: DatabaseManager,
    conn: sqlite3.Connection | None = None,
) -> tuple[sqlite3.Connection, bool]:
    if conn is not None:
        return conn, False

    new_conn = sqlite3.connect(str(db.db_path))
    new_conn.execute("PRAGMA foreign_keys = ON")
    return new_conn, True


def _is_missing_value(value: Any) -> bool:
    return value is None or bool(pd.isna(cast(object, value)))


def _coerce_int(value: Any) -> int:
    if _is_missing_value(value) or str(value).strip() == '':
        return 0
    return int(cast(int | float | str, value))


def _coerce_float(value: Any, *, digits: int = 3) -> float | None:
    if _is_missing_value(value) or str(value).strip() == '':
        return None
    return round(float(cast(float | int | str, value)), digits)


def _ip_to_outs(value: Any) -> int:
    if _is_missing_value(value):
        return 0

    text = str(value).strip()
    if not text:
        return 0

    if '.' not in text:
        return _coerce_int(text) * 3

    whole_text, fractional_text = text.split('.', 1)
    whole_innings = int(whole_text)
    if fractional_text and fractional_text[0] in {'0', '1', '2'}:
        return whole_innings * 3 + int(fractional_text[0])

    numeric_value = float(text)
    whole_innings = int(numeric_value)
    remainder = numeric_value - whole_innings
    if abs(remainder - (1 / 3)) < 0.05:
        return whole_innings * 3 + 1
    if abs(remainder - (2 / 3)) < 0.05:
        return whole_innings * 3 + 2
    return whole_innings * 3


def _hash_payload(payload: Any) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()


def _get_snapshot_run_at() -> str:
    return get_kst_now().isoformat(timespec='microseconds')


def _build_team_source_snapshots(
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    team_names: set[str],
) -> dict[str, dict[str, Any]]:
    snapshots: dict[str, dict[str, Any]] = {}

    for team_name in sorted(team_names):
        bat_entries: list[dict[str, Any]] = []
        pit_entries: list[dict[str, Any]] = []

        if 'Team' in bat.columns:
            bat_rows = bat[bat['Team'] == team_name]
            for player_id, row in bat_rows.iterrows():
                resolved_id = row.get('ID', player_id)
                if _is_missing_value(resolved_id) or str(resolved_id).strip() == '':
                    continue
                bat_entries.append(
                    {
                        'id': str(resolved_id),
                        'g': _coerce_int(row.get('G')),
                        'pa': _coerce_int(row.get('PA')),
                        'owar': _coerce_float(row.get('oWAR')),
                        'war': _coerce_float(row.get('WAR')),
                    }
                )

        if 'Team' in pit.columns:
            pit_rows = pit[pit['Team'] == team_name]
            for player_id, row in pit_rows.iterrows():
                resolved_id = row.get('ID', player_id)
                if _is_missing_value(resolved_id) or str(resolved_id).strip() == '':
                    continue
                pit_entries.append(
                    {
                        'id': str(resolved_id),
                        'g': _coerce_int(row.get('G')),
                        'ip_outs': _ip_to_outs(row.get('IP')),
                        'war': _coerce_float(row.get('WAR')),
                    }
                )

        bat_entries.sort(key=lambda item: item['id'])
        pit_entries.sort(key=lambda item: item['id'])

        bat_pa_total = sum(entry['pa'] for entry in bat_entries)
        pit_outs_total = sum(entry['ip_outs'] for entry in pit_entries)
        war_payload = {
            'bat': [[entry['id'], entry['owar'], entry['war']] for entry in bat_entries],
            'pit': [[entry['id'], entry['war']] for entry in pit_entries],
        }
        usage_payload = {
            'bat': [[entry['id'], entry['g'], entry['pa']] for entry in bat_entries],
            'pit': [[entry['id'], entry['g'], entry['ip_outs']] for entry in pit_entries],
        }

        snapshots[team_name] = {
            'war_hash': _hash_payload(war_payload),
            'usage_hash': _hash_payload(usage_payload),
            'bat_pa_total': bat_pa_total,
            'pit_outs_total': pit_outs_total,
        }

    return snapshots


def _persist_source_team_snapshots(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
    run_at: str,
    team_snapshots: dict[str, dict[str, Any]],
) -> None:
    for team_name, snapshot in team_snapshots.items():
        conn.execute(
            """INSERT OR REPLACE INTO source_team_snapshots
               (run_at, season_id, target_date, team_name, phase, war_hash, usage_hash, bat_pa_total, pit_outs_total)
               VALUES (?, ?, ?, ?, 'post_final', ?, ?, ?, ?)""",
            (
                run_at,
                season_id,
                target_date,
                team_name,
                snapshot['war_hash'],
                snapshot['usage_hash'],
                snapshot['bat_pa_total'],
                snapshot['pit_outs_total'],
            ),
        )


def _get_snapshot_rows_for_run(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
    run_at: str,
) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """SELECT team_name, war_hash, usage_hash, bat_pa_total, pit_outs_total
           FROM source_team_snapshots
           WHERE season_id = ? AND target_date = ? AND phase = 'post_final' AND run_at = ?""",
        (season_id, target_date, run_at),
    ).fetchall()
    return {
        row[0]: {
            'war_hash': row[1],
            'usage_hash': row[2],
            'bat_pa_total': row[3],
            'pit_outs_total': row[4],
        }
        for row in rows
    }


def _get_latest_post_final_snapshot(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
    *,
    before_run_at: str | None = None,
) -> dict[str, dict[str, Any]]:
    params: list[Any] = [season_id, target_date]
    run_at_sql = (
        "SELECT MAX(run_at) FROM source_team_snapshots "
        "WHERE season_id = ? AND target_date = ? AND phase = 'post_final'"
    )
    if before_run_at is not None:
        run_at_sql += " AND run_at < ?"
        params.append(before_run_at)
    row = conn.execute(run_at_sql, params).fetchone()
    latest_run_at = row[0] if row else None
    if not latest_run_at:
        return {}
    return _get_snapshot_rows_for_run(conn, season_id, target_date, latest_run_at)


def _get_latest_prior_date_snapshot(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
) -> dict[str, dict[str, Any]]:
    row = conn.execute(
        """SELECT MAX(target_date)
           FROM source_team_snapshots
           WHERE season_id = ? AND target_date < ? AND phase = 'post_final'""",
        (season_id, target_date),
    ).fetchone()
    previous_target_date = row[0] if row else None
    if not previous_target_date:
        return {}
    return _get_latest_post_final_snapshot(conn, season_id, previous_target_date)


def _has_required_usage_progress(
    current_snapshot: dict[str, Any],
    baseline_snapshot: dict[str, Any] | None,
) -> bool:
    current_bat_pa = int(current_snapshot['bat_pa_total'])
    current_pit_outs = int(current_snapshot['pit_outs_total'])

    if current_bat_pa <= 0 or current_pit_outs <= 0:
        return False

    if baseline_snapshot is None:
        return True

    baseline_bat_pa = int(baseline_snapshot['bat_pa_total'])
    baseline_pit_outs = int(baseline_snapshot['pit_outs_total'])

    return current_bat_pa > baseline_bat_pa and current_pit_outs > baseline_pit_outs


def _is_source_war_ready(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
    team_snapshots: dict[str, dict[str, Any]],
    played_teams: set[str],
) -> bool:
    if not played_teams:
        return False

    prior_date_snapshot = _get_latest_prior_date_snapshot(conn, season_id, target_date)

    for team_name in played_teams:
        current_snapshot = team_snapshots.get(team_name)
        if current_snapshot is None:
            return False

        baseline_snapshot = prior_date_snapshot.get(team_name)
        if not _has_required_usage_progress(current_snapshot, baseline_snapshot):
            return False

    return True


def update_db(
    player_name: pd.DataFrame,
    player_id: pd.DataFrame,
    player_activation: pd.DataFrame,
    live_war: pd.DataFrame,
    current_war: pd.DataFrame,
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    games: pd.DataFrame,
    db_path: Path,
) -> None:
    """Update database with latest WAR data.
    
    Updates both legacy tables (for backward compatibility during transition)
    and new Long format tables.
    
    Args:
        player_name: Player names DataFrame indexed by team
        player_id: Player IDs DataFrame indexed by team
        player_activation: Activation status DataFrame indexed by team
        live_war: Live WAR DataFrame indexed by team
        current_war: Current WAR DataFrame indexed by team
        bat: Batter statistics DataFrame
        pit: Pitcher statistics DataFrame
        games: Games status DataFrame
        db_path: Path to database
    """
    # Track execution time
    start_time = datetime.now()
    db = DatabaseManager(db_path)
    date_mmdd = get_date()
    date_iso = _parse_date_to_iso(date_mmdd)
    season_id = _get_season_id(db)

    error_message = None
    games_updated = 0
    war_status = 'pending'

    try:
        with db.connection() as conn:
            # =========================================================================
            # NEW SCHEMA UPDATES (Long format)
            # =========================================================================

            # 1. Update players table
            _update_players_table(db, bat, pit, conn=conn)

            # 2. Update war_daily table (Long format - core table)
            _update_war_daily(db, bat, pit, date_iso, season_id, conn=conn)

            # 3. Update team_war_daily table
            _update_team_war_daily(
                db, bat, pit, player_id, player_activation, current_war,
                date_iso, season_id, conn=conn
            )

            # 4. Update daily_records (GOAT/BOAT)
            _update_daily_records(db, bat, pit, date_iso, season_id, conn=conn)

            # 5. Update info table
            _update_info_table(db, bat, pit, games, conn=conn)

            # Get games_updated from scraper_status
            result = conn.execute(
                "SELECT updated_games, war_status FROM scraper_status WHERE id = 1"
            ).fetchone()
            if result:
                games_updated = result[0]
                war_status = result[1]

            conn.commit()

        # 6. Invalidate cache after update
        invalidate_after_update()

    except Exception as e:
        error_message = str(e)
        war_status = 'failed'

    finally:
        # Calculate duration and log to scraper_log
        duration = (datetime.now() - start_time).total_seconds()
        run_at = get_kst_timestamp()

        try:
            with db.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO scraper_log
                       (run_at, target_date, games_found, games_updated,
                        war_status, duration_seconds, error_message)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (run_at, date_iso, 0, games_updated, war_status,
                     duration, error_message)
                )
                conn.commit()
        except Exception:
            # Silently fail if logging fails to avoid breaking the scraper
            pass

        # Run daily backup (once per day)
        try:
            maybe_run_daily_backup(db_path)
        except Exception as backup_error:
            logging.warning(f"Backup failed: {str(backup_error)}")

        # Re-raise original exception if one occurred
        if error_message:
            raise Exception(error_message)


def _update_players_table(
    db: DatabaseManager,
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Update players master table with new players and positions."""
    bat_lookup = {
        str(player_id): row
        for player_id, row in bat.iterrows()
        if not _is_missing_value(player_id) and str(player_id).strip() != ''
    }
    pit_lookup = {
        str(player_id): row
        for player_id, row in pit.iterrows()
        if not _is_missing_value(player_id) and str(player_id).strip() != ''
    }
    all_player_ids = sorted(set(bat_lookup) | set(pit_lookup))

    conn, owns_connection = _resolve_connection(db, conn)
    try:
        cursor = conn.cursor()

        for player_id in all_player_ids:
            bat_row = bat_lookup.get(player_id)
            pit_row = pit_lookup.get(player_id)

            bat_war = None
            pit_war = None
            player_name = None
            position = None
            player_type = None

            if bat_row is not None:
                player_name = bat_row.get('Name')
                bat_war = bat_row.get('oWAR', None)
                position = bat_row.get('POS', None)
                if position == 'DH':
                    position = None
            if pit_row is not None:
                if _is_missing_value(player_name) or str(player_name).strip() == '':
                    player_name = pit_row.get('Name')
                pit_war = pit_row.get('WAR', None)

            if _is_missing_value(player_name) or str(player_name).strip() == '':
                continue

            has_bat = bat_row is not None
            has_pit = pit_row is not None
            if has_bat and not has_pit:
                player_type = 'bat'
            elif has_pit and not has_bat:
                player_type = 'pit'
                position = 'P'
            elif has_bat and has_pit:
                bat_value = float(bat_war) if bat_war is not None and not pd.isna(bat_war) else 0.0
                pit_value = float(pit_war) if pit_war is not None and not pd.isna(pit_war) else 0.0
                if pit_value > bat_value:
                    player_type = 'pit'
                    position = 'P'
                else:
                    player_type = 'bat'

            cursor.execute(
                """INSERT INTO players (id, name, player_type, position)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       name = excluded.name,
                       player_type = COALESCE(excluded.player_type, players.player_type),
                       position = COALESCE(excluded.position, players.position)""",
                (player_id, str(player_name).strip(), player_type, position)
            )

        if owns_connection:
            conn.commit()
    finally:
        if owns_connection:
            conn.close()


def _update_war_daily(
    db: DatabaseManager,
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    date_iso: str,
    season_id: int,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Update war_daily table with today's WAR values.
    
    For players who appear in both bat and pit tables (two-way players),
    their oWAR and WAR are summed together.
    """
    conn, owns_connection = _resolve_connection(db, conn)
    try:
        cursor = conn.cursor()

        cursor.execute(
            """SELECT current.player_id, current.war
               FROM war_daily current
               WHERE current.season_id = ?
                 AND current.date = (
                     SELECT MAX(previous.date)
                     FROM war_daily previous
                     WHERE previous.player_id = current.player_id
                       AND previous.season_id = current.season_id
                       AND previous.date < ?
                 )""",
            (season_id, date_iso),
        )
        prev_war = {row[0]: row[1] for row in cursor.fetchall()}

        bat_war_map = {}
        pit_war_map = {}

        for player_id, row in bat.iterrows():
            war_value = row.get('oWAR', None)
            if _is_missing_value(war_value) or _is_missing_value(player_id) or str(player_id).strip() == '':
                continue
            bat_war_map[str(player_id)] = round(float(cast(float, war_value)), 2)

        for player_id, row in pit.iterrows():
            war_value = row.get('WAR', None)
            if _is_missing_value(war_value) or _is_missing_value(player_id) or str(player_id).strip() == '':
                continue
            pit_war_map[str(player_id)] = round(float(cast(float, war_value)), 2)

        all_player_ids = sorted(set(bat_war_map) | set(pit_war_map))

        for player_id_str in all_player_ids:
            bat_war = bat_war_map.get(player_id_str)
            pit_war = pit_war_map.get(player_id_str)
            war = round((bat_war or 0) + (pit_war or 0), 2)
            prev = prev_war.get(player_id_str)
            war_diff = round(war - prev, 2) if prev is not None else round(war, 2)

            cursor.execute(
                """INSERT OR REPLACE INTO war_daily 
                   (player_id, season_id, date, bat_war, pit_war, war, war_diff)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (player_id_str, season_id, date_iso, bat_war, pit_war, war, war_diff)
            )

        if owns_connection:
            conn.commit()
    finally:
        if owns_connection:
            conn.close()


def _update_team_war_daily(
    db: DatabaseManager,
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    player_id: pd.DataFrame,
    player_activation: pd.DataFrame,
    current_war: pd.DataFrame,
    date_iso: str,
    season_id: int,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Update team_war_daily table with team totals."""
    provided_conn = conn
    conn, owns_connection = _resolve_connection(db, conn)
    try:
        fa_total_provider = (
            lambda: calculate_fa_total_as_of(
                conn,
                season_id,
                date_iso,
                bat=bat,
                pit=pit,
            )
            if provided_conn is not None
            else calculate_fa_war(
                bat,
                pit,
                player_id,
                player_activation,
                current_war,
                db_path=str(db.db_path),
                current_date=date_iso,
                season_id=season_id,
            )
        )
        write_team_war_daily_for_date(
            conn,
            season_id,
            date_iso,
            fa_total_provider,
        )
        if owns_connection:
            conn.commit()
    finally:
        if owns_connection:
            conn.close()


def _update_daily_records(
    db: DatabaseManager,
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    date_iso: str,
    season_id: int,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Update daily_records table with GOAT/BOAT entries."""
    conn, owns_connection = _resolve_connection(db, conn)
    try:
        cursor = conn.cursor()

        # Get today's WAR diffs from war_daily
        cursor.execute(
            """SELECT w.player_id, p.name, w.war_diff, r.team_id
               FROM war_daily w
               JOIN players p ON w.player_id = p.id
               LEFT JOIN roster r
                 ON r.player_id = w.player_id
                AND r.season_id = w.season_id
                AND r.joined_date <= ?
                AND (r.left_date IS NULL OR r.left_date > ?)
               WHERE w.date = ? AND w.season_id = ? AND w.war_diff IS NOT NULL
               ORDER BY w.war_diff DESC""",
            (date_iso, date_iso, date_iso, season_id)
        )
        diffs = cursor.fetchall()

        if not diffs:
            return

        # Clear previous entries for today
        cursor.execute("DELETE FROM daily_records WHERE date = ?", (date_iso,))

        # Insert GOAT entries (positive diff)
        for player_id, player_name, war_diff, team_id in diffs:
            if war_diff > 0:
                cursor.execute(
                    """INSERT INTO daily_records 
                       (date, record_type, team_id, player_id, war_diff)
                       VALUES (?, 'GOAT', ?, ?, ?)""",
                    (date_iso, team_id, player_id, war_diff)
                )

        # Insert BOAT entries (negative diff)
        for player_id, player_name, war_diff, team_id in diffs:
            if war_diff < 0:
                cursor.execute(
                    """INSERT INTO daily_records 
                       (date, record_type, team_id, player_id, war_diff)
                       VALUES (?, 'BOAT', ?, ?, ?)""",
                    (date_iso, team_id, player_id, war_diff)
                )

        if owns_connection:
            conn.commit()
    finally:
        if owns_connection:
            conn.close()


def _update_info_table(
    db: DatabaseManager,
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    games: pd.DataFrame,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Update scraper_status and daily_games tables.
    
    Replaces the legacy update_info table with normalized schema.
    """
    now = get_kst_timestamp()
    date_iso = _parse_date_to_iso(get_date())
    
    # Parse games DataFrame to extract game info
    total_games = 0
    updated_games = 0
    game_list = []
    no_games = False
    schedule_complete = False
    source_war_ready = False
    publish_ready_at = None
    has_source_frames = not bat.empty and not pit.empty
    
    if len(games) > 0:
        # Check for "no games" message
        if games.iloc[0, 0] == '오늘은 경기가 없습니다.':
            no_games = True
        else:
            # Parse game rows (skip header rows like "경기 날짜", "업데이트")
            for i in range(len(games)):
                row = games.iloc[i]
                away_team = str(row[0]) if pd.notna(row[0]) else ""
                score_str = str(row[1]) if pd.notna(row[1]) else ""
                home_team = str(row[2]) if pd.notna(row[2]) else ""
                
                # Skip non-game rows
                if not away_team or not home_team:
                    continue
                if '경기 날짜' in away_team or '업데이트' in away_team:
                    continue
                if away_team in ['오늘은 경기가 없습니다.']:
                    no_games = True
                    continue
                
                # Parse score
                away_score = None
                home_score = None
                game_status = 'scheduled'
                
                if ':' in score_str:
                    try:
                        parts = score_str.split(':')
                        away_score = int(parts[0].strip())
                        home_score = int(parts[1].strip())
                        game_status = 'final'
                    except (ValueError, IndexError):
                        pass
                elif score_str == '우천취소':
                    game_status = 'cancelled'
                elif score_str == '진행 중':
                    game_status = 'in_progress'
                elif score_str == '시작 전':
                    game_status = 'scheduled'
                elif score_str:
                    game_status = 'scheduled'
                
                total_games += 1
                game_list.append({
                    'away_team': away_team,
                    'home_team': home_team,
                    'away_score': away_score,
                    'home_score': home_score,
                    'game_status': game_status,
                })
    
    season_id = _get_season_id(db)
    active_games = [game for game in game_list if game['game_status'] != 'cancelled']
    final_games = [game for game in active_games if game['game_status'] == 'final']
    played_teams = {
        str(team)
        for game in active_games
        for team in (game['away_team'], game['home_team'])
        if str(team).strip()
    }
    schedule_complete = bool(active_games) and len(final_games) == len(active_games)

    if no_games or not active_games:
        war_status = 'no_games'
        updated_games = 0
        total_games = 0
    else:
        updated_games = len(final_games)
        if has_source_frames and schedule_complete:
            team_snapshots = _build_team_source_snapshots(bat, pit, played_teams)
            snapshot_run_at = _get_snapshot_run_at()
            snapshot_conn, snapshot_owns_connection = _resolve_connection(db, conn)
            try:
                _persist_source_team_snapshots(
                    snapshot_conn,
                    season_id,
                    date_iso,
                    snapshot_run_at,
                    team_snapshots,
                )
                source_war_ready = _is_source_war_ready(
                    snapshot_conn,
                    season_id,
                    date_iso,
                    team_snapshots,
                    played_teams,
                )
            finally:
                if snapshot_owns_connection:
                    snapshot_conn.commit()
                    snapshot_conn.close()

        if schedule_complete and source_war_ready:
            war_status = 'completed'
            publish_ready_at = now
        else:
            war_status = 'pending'

    # Update scraper_status table (upsert)
    conn, owns_connection = _resolve_connection(db, conn)
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO scraper_status (
                id,
                last_updated_at,
                target_date,
                total_games,
                updated_games,
                war_status,
                schedule_complete,
                source_war_ready,
                publish_ready_at,
                last_full_run_at,
                last_full_run_status,
                last_error_message
            )
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                last_updated_at = excluded.last_updated_at,
                target_date = excluded.target_date,
                total_games = excluded.total_games,
                updated_games = excluded.updated_games,
                war_status = excluded.war_status,
                schedule_complete = excluded.schedule_complete,
                source_war_ready = excluded.source_war_ready,
                publish_ready_at = CASE
                    WHEN scraper_status.target_date != excluded.target_date THEN excluded.publish_ready_at
                    ELSE COALESCE(excluded.publish_ready_at, scraper_status.publish_ready_at)
                END,
                last_full_run_at = CASE
                    WHEN scraper_status.target_date != excluded.target_date THEN excluded.last_full_run_at
                    ELSE COALESCE(excluded.last_full_run_at, scraper_status.last_full_run_at)
                END,
                last_full_run_status = CASE
                    WHEN scraper_status.target_date != excluded.target_date THEN excluded.last_full_run_status
                    ELSE COALESCE(excluded.last_full_run_status, scraper_status.last_full_run_status)
                END,
                last_error_message = excluded.last_error_message
        """, (
            now,
            date_iso,
            total_games,
            updated_games,
            war_status,
            int(schedule_complete),
            int(source_war_ready),
            publish_ready_at,
            now if has_source_frames else None,
            'success' if has_source_frames else None,
            None,
        ))
        
        # Update daily_games table
        for i, game in enumerate(game_list, 1):
            cursor.execute("""
                INSERT INTO daily_games (game_date, game_order, away_team, home_team, away_score, home_score, game_status, war_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(game_date, game_order) DO UPDATE SET
                    away_team = excluded.away_team,
                    home_team = excluded.home_team,
                    away_score = excluded.away_score,
                    home_score = excluded.home_score,
                    game_status = excluded.game_status,
                    war_updated = excluded.war_updated
            """, (
                date_iso, i,
                game['away_team'], game['home_team'],
                game['away_score'], game['home_score'],
                game['game_status'],
                1 if game['game_status'] == 'final' else 0
            ))
        
        if owns_connection:
            conn.commit()
    finally:
        if owns_connection:
            conn.close()


def check_update(
    bat: pd.DataFrame,
    pit: pd.DataFrame,
    games: pd.DataFrame,
    db: DatabaseManager,
    conn: sqlite3.Connection | None = None,
    season_id: int | None = None,
) -> bool:
    """Check if all games have been updated in the stats.
    
    Uses new schema to check if WAR has changed from yesterday.
    
    Args:
        bat: Batter statistics DataFrame
        pit: Pitcher statistics DataFrame
        games: Games status DataFrame
        db: DatabaseManager instance
        
    Returns:
        bool: True if all teams with games have updated stats
    """
    date_iso = _parse_date_to_iso(get_date())
    season_id = season_id or _get_season_id(db)

    # Get teams that have different WAR today vs yesterday
    conn, owns_connection = _resolve_connection(db, conn)
    try:
        cursor = conn.cursor()

        # Find players with changed WAR
        cursor.execute("""
            SELECT DISTINCT current.player_id,
                   p.name,
                   CASE WHEN p.player_type = 'bat' THEN 'bat' ELSE 'pit' END as type
            FROM war_daily current
            JOIN players p ON current.player_id = p.id
            LEFT JOIN war_daily previous
              ON previous.player_id = current.player_id
             AND previous.season_id = current.season_id
             AND previous.date = (
                 SELECT MAX(previous2.date)
                 FROM war_daily previous2
                 WHERE previous2.player_id = current.player_id
                   AND previous2.season_id = current.season_id
                   AND previous2.date < ?
             )
            WHERE current.season_id = ?
              AND current.date = ?
              AND (
                  previous.war IS NULL
                  OR current.war != previous.war
              )
        """, (date_iso, season_id, date_iso))

        changed_players = cursor.fetchall()
    finally:
        if owns_connection:
            conn.close()

    # Get teams from changed players (using bat/pit Team column)
    bat_updated_teams = set()
    pit_updated_teams = set()

    for player_id, player_name, player_type in changed_players:
        if player_type == 'bat':
            if 'ID' in bat.columns:
                matches = bat[bat['ID'] == player_id]
            elif getattr(bat.index, 'name', None) == 'ID' or 'ID' in getattr(bat.index, 'names', []):
                matches = bat.loc[[player_id]] if player_id in bat.index else bat.iloc[0:0]
            else:
                matches = bat[bat['Name'] == player_name]
            if not matches.empty and 'Team' in matches.columns:
                bat_updated_teams.update(
                    str(team)
                    for team in matches['Team']
                    if not _is_missing_value(team)
                )
        else:
            if 'ID' in pit.columns:
                matches = pit[pit['ID'] == player_id]
            elif getattr(pit.index, 'name', None) == 'ID' or 'ID' in getattr(pit.index, 'names', []):
                matches = pit.loc[[player_id]] if player_id in pit.index else pit.iloc[0:0]
            else:
                matches = pit[pit['Name'] == player_name]
            if not matches.empty and 'Team' in matches.columns:
                pit_updated_teams.update(
                    str(team)
                    for team in matches['Team']
                    if not _is_missing_value(team)
                )

    # Get teams with games today
    filtered_games = games[(games[2] != '') & (games[1] != '\uc6b0\ucc9c\ucde8\uc18c')]
    if len(filtered_games) == 0:
        return True

    teams_set = set(filtered_games[0].tolist() + filtered_games[2].tolist())

    changed_real_teams = bat_updated_teams | pit_updated_teams
    return all(team in changed_real_teams for team in teams_set)


def backup_db(db_path: Path, backup_dir: Path) -> Path:
    """Create database backup.
    
    Args:
        db_path: Path to database
        backup_dir: Directory for backups
        
    Returns:
        Path: Path to backup file
    """
    db = DatabaseManager(db_path)
    return db.backup(backup_dir)


def maybe_run_daily_backup(db_path: Path) -> None:
    """Run backup if not done today.
    
    Checks if a backup for today already exists. If not, creates one
    and cleans up backups older than RETENTION_DAYS.
    
    Args:
        db_path: Path to database file
    """
    from scripts.backup import backup_database, BACKUP_DIR
    
    today = datetime.now().strftime("%Y%m%d")
    today_backup = BACKUP_DIR / f"untatiz_db_{today}.db"
    
    if not today_backup.exists():
        backup_database()
        logging.info(f"Daily backup created: {today_backup}")
    else:
        logging.debug(f"Backup already exists for today: {today_backup}")


__all__ = [
    "update_db",
    "check_update",
    "backup_db",
    "maybe_run_daily_backup",
]
