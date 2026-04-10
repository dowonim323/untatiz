"""Draft loader utility for importing draft picks from JSON or CSV files.

This module provides functionality to:
1. Load draft picks from JSON or CSV configuration files
2. Validate player IDs against the database
3. Insert picks into the draft table
4. Update the roster table with drafted players
5. Dynamically retrieve draft slot order from the database

CSV Format (Option B - single file with header comment):
    # season=2025,draft_type=main,application_date=2025-03-21,description=2025시즌 메인 드래프트
    pick_order,team,round,player_id,player_name
    1,준,용타,15004,김도영
    2,준,용투1,16088,변우혁
    ...

Usage:
    # Command line
    python -m app.core.draft_loader config/drafts/2025_main.json
    python -m app.core.draft_loader config/drafts/2025_main.csv
    python -m app.core.draft_loader --year 2025  # Load all drafts for year

    # Programmatic
    from app.core.draft_loader import load_draft_from_json, load_draft_from_csv
    result = load_draft_from_json(Path("config/drafts/2025_main.json"))
    result = load_draft_from_csv(Path("config/drafts/2025_main.csv"))
    
    # Get dynamic slot order from DB
    from app.core.draft_loader import get_draft_slots
    slots = get_draft_slots(db)  # Returns ['용타', '용투1', '용투2', '1R', '2R', ...]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

from app.core.db import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Default paths
DEFAULT_DB_PATH = Path("/home/ubuntu/untatiz/db/untatiz_db.db")
DEFAULT_CONFIG_DIR = Path("/home/ubuntu/untatiz/config/drafts")


@dataclass
class DraftPick:
    """Represents a single draft pick."""
    team: str
    round: str
    player_id: str
    player_name: str


@dataclass
class FAConfig:
    """FA WAR calculation configuration."""
    roster_size: int = 29
    supplemental_bonus: int = 5
    min_pitchers: int = 11
    min_catchers: int = 2
    min_infielders: int = 7
    min_outfielders: int = 5


@dataclass
class DraftConfig:
    """Represents a complete draft configuration."""
    season: int
    draft_type: str  # 'main' or 'supplemental'
    application_date: str  # YYYY-MM-DD format
    description: str
    picks: List[DraftPick]
    fa_config: Optional[FAConfig] = None

    @classmethod
    def from_json(cls, json_path: Path) -> "DraftConfig":
        """Load draft configuration from JSON file.
        
        Args:
            json_path: Path to JSON file
            
        Returns:
            DraftConfig instance
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If JSON format is invalid
        """
        if not json_path.exists():
            raise FileNotFoundError(f"Draft config file not found: {json_path}")

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Validate required fields
        required_fields = ["season", "draft_type", "application_date", "picks"]
        missing = [f for f in required_fields if f not in data]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

        # Validate draft_type
        if data["draft_type"] not in ("main", "supplemental"):
            raise ValueError(f"Invalid draft_type: {data['draft_type']}. Must be 'main' or 'supplemental'")

        # Parse picks
        picks = []
        for i, pick_data in enumerate(data["picks"]):
            required_pick_fields = ["team", "round", "player_id", "player_name"]
            missing_pick = [f for f in required_pick_fields if f not in pick_data]
            if missing_pick:
                raise ValueError(f"Pick {i+1} missing fields: {missing_pick}")
            
            picks.append(DraftPick(
                team=pick_data["team"],
                round=pick_data["round"],
                player_id=str(pick_data["player_id"]),
                player_name=pick_data["player_name"]
            ))

        fa_config = None
        if "fa_config" in data:
            fc = data["fa_config"]
            pos_req = fc.get("position_requirements", {})
            fa_config = FAConfig(
                roster_size=fc.get("roster_size", 29),
                supplemental_bonus=fc.get("supplemental_bonus", 5),
                min_pitchers=pos_req.get("P", 11),
                min_catchers=pos_req.get("C", 2),
                min_infielders=pos_req.get("IF", 7),
                min_outfielders=pos_req.get("OF", 5),
            )

        return cls(
            season=data["season"],
            draft_type=data["draft_type"],
            application_date=data["application_date"],
            description=data.get("description", ""),
            picks=picks,
            fa_config=fa_config
        )


@dataclass
class LoadResult:
    """Result of loading a draft."""
    success: bool
    inserted_count: int
    skipped_count: int
    errors: List[str]
    warnings: List[str]


def get_season_id(db: DatabaseManager, year: int) -> Optional[int]:
    """Get season ID for a given year.
    
    Args:
        db: DatabaseManager instance
        year: Season year (e.g., 2025)
        
    Returns:
        Season ID or None if not found
    """
    result = db.fetch_one("SELECT id FROM seasons WHERE year = ?", (year,))
    return result[0] if result else None


def create_season_if_not_exists(db: DatabaseManager, year: int) -> int:
    """Create season record if it doesn't exist.
    
    Args:
        db: DatabaseManager instance
        year: Season year
        
    Returns:
        Season ID
    """
    season_id = get_season_id(db, year)
    if season_id is None:
        db.execute(
            "INSERT INTO seasons (year, is_active) VALUES (?, 0)",
            (year,)
        )
        season_id = get_season_id(db, year)
        if season_id is None:
            raise ValueError(f"Failed to create season: {year}")
        logger.info(f"Created new season: {year} (id={season_id})")
    return season_id


def validate_team(db: DatabaseManager, team_id: str) -> bool:
    """Check if team exists in fantasy_teams table.
    
    Args:
        db: DatabaseManager instance
        team_id: Team ID (e.g., '준', '뚝')
        
    Returns:
        True if team exists
    """
    result = db.fetch_one("SELECT id FROM fantasy_teams WHERE id = ?", (team_id,))
    return result is not None


def validate_player(db: DatabaseManager, player_id: str) -> Tuple[bool, Optional[str]]:
    """Check if player exists in players table.
    
    Args:
        db: DatabaseManager instance
        player_id: Statiz player ID
        
    Returns:
        Tuple of (exists, player_name if exists)
    """
    result = db.fetch_one("SELECT name FROM players WHERE id = ?", (player_id,))
    if result:
        return True, result[0]
    return False, None


def get_draft_slots(db: DatabaseManager, season_id: int | None = None) -> List[str]:
    """Get ordered list of draft slots from database.
    
    Dynamically retrieves slot order based on actual pick_order values,
    eliminating the need for hardcoded DRAFT_ROUNDS constant.
    
    Args:
        db: DatabaseManager instance
        
    Returns:
        List of slot names in draft order, e.g., ['용타', '용투1', '용투2', '1R', ...]
    """
    if season_id is None:
        rows = db.fetch_all(
            """
            SELECT round FROM draft
            GROUP BY round
            ORDER BY MIN(pick_order)
            """
        )
    else:
        rows = db.fetch_all(
            """
            SELECT round FROM draft
            WHERE season_id = ?
            GROUP BY round
            ORDER BY MIN(pick_order)
            """,
            (season_id,),
        )
    return [r[0] for r in rows] if rows else []


def get_pick_order(round_name: str, team_order: int) -> int:
    """Calculate pick order based on round name.
    
    Main draft rounds:
    - 용타: 0-9 (one per team)
    - 용투1: 10-19
    - 용투2: 20-29
    - 아쿼: 30-39 (2026년 신설)
    - 1R ~ 25R: 100+
    
    Supplemental draft:
    - 2차1R, 2차2R, etc.: 1000+
    
    Args:
        round_name: Round name (e.g., '용타', '1R', '2차1R')
        team_order: Team's order in the draft (0-7)
        
    Returns:
        Numeric pick order for sorting
    """
    if round_name == "용타":
        return team_order
    elif round_name == "용투1":
        return 10 + team_order
    elif round_name == "용투2":
        return 20 + team_order
    elif round_name == "아쿼":
        return 30 + team_order
    elif round_name.endswith("R") and round_name[:-1].isdigit():
        # Main draft: 1R = 100, 2R = 200, etc.
        round_num = int(round_name[:-1])
        return 100 * round_num + team_order
    elif round_name.startswith("2차") and round_name.endswith("R"):
        # Supplemental draft: 2차1R = 1100, 2차2R = 1200, etc.
        round_num = int(round_name[2:-1])
        return 1000 + 100 * round_num + team_order
    else:
        # Unknown round type, use high number
        logger.warning(f"Unknown round type: {round_name}")
        return 9000 + team_order


def check_existing_draft(
    db: DatabaseManager,
    season_id: int,
    draft_type: str
) -> List[Tuple[str, str, str]]:
    """Check for existing draft picks.
    
    Args:
        db: DatabaseManager instance
        season_id: Season ID
        draft_type: 'main' or 'supplemental'
        
    Returns:
        List of (team_id, player_id, round) tuples for existing picks
    """
    rows = db.fetch_all(
        """SELECT team_id, player_id, round 
           FROM draft 
           WHERE season_id = ? AND draft_type = ?""",
        (season_id, draft_type)
    )
    return [(r[0], r[1], r[2]) for r in rows]


def load_draft_from_json(
    json_path: Path,
    db_path: Path = DEFAULT_DB_PATH,
    dry_run: bool = False,
    force: bool = False
) -> LoadResult:
    """Load draft picks from JSON file into database.
    
    Args:
        json_path: Path to JSON draft configuration file
        db_path: Path to SQLite database
        dry_run: If True, validate only without inserting
        force: If True, delete existing picks before inserting
        
    Returns:
        LoadResult with counts and any errors/warnings
    """
    errors: List[str] = []
    warnings: List[str] = []
    
    # Load and validate JSON
    try:
        config = DraftConfig.from_json(json_path)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as e:
        return LoadResult(
            success=False,
            inserted_count=0,
            skipped_count=0,
            errors=[str(e)],
            warnings=[]
        )
    
    logger.info(f"Loading draft: {config.description}")
    logger.info(f"  Season: {config.season}, Type: {config.draft_type}")
    logger.info(f"  Application date: {config.application_date}")
    logger.info(f"  Total picks: {len(config.picks)}")
    
    db = DatabaseManager(db_path)
    
    # Get or create season
    season_id = create_season_if_not_exists(db, config.season)
    
    # Track team order for pick_order calculation
    team_order_map: dict[str, int] = {}
    team_counter = 0
    
    # Validate all picks first
    valid_picks: List[Tuple[DraftPick, int]] = []  # (pick, pick_order)
    skipped = 0
    
    for pick in config.picks:
        # Validate team
        if not validate_team(db, pick.team):
            errors.append(f"Invalid team '{pick.team}' for player {pick.player_name}")
            continue
        
        # Skip placeholder picks (XXXXX)
        if pick.player_id == "XXXXX" or not pick.player_id.isdigit():
            warnings.append(f"Skipping placeholder pick: {pick.player_name} ({pick.player_id})")
            skipped += 1
            continue
        
        # Validate player exists (warning only, don't block)
        player_exists, db_name = validate_player(db, pick.player_id)
        if not player_exists:
            warnings.append(
                f"Player ID {pick.player_id} ({pick.player_name}) not in database. "
                "Will be added when scraper runs."
            )
        elif db_name != pick.player_name:
            warnings.append(
                f"Name mismatch for {pick.player_id}: "
                f"JSON='{pick.player_name}', DB='{db_name}'"
            )
        
        # Calculate pick order
        if pick.team not in team_order_map:
            team_order_map[pick.team] = team_counter
            team_counter += 1
        team_order = team_order_map[pick.team]
        pick_order = get_pick_order(pick.round, team_order)
        
        valid_picks.append((pick, pick_order))
    
    if errors:
        return LoadResult(
            success=False,
            inserted_count=0,
            skipped_count=skipped,
            errors=errors,
            warnings=warnings
        )
    
    if dry_run:
        logger.info(f"DRY RUN: Would insert {len(valid_picks)} picks, skip {skipped}")
        return LoadResult(
            success=True,
            inserted_count=0,
            skipped_count=skipped,
            errors=[],
            warnings=warnings
        )
    
    # Check for existing picks
    existing = check_existing_draft(db, season_id, config.draft_type)
    if existing and not force:
        return LoadResult(
            success=False,
            inserted_count=0,
            skipped_count=0,
            errors=[
                f"Found {len(existing)} existing {config.draft_type} draft picks for season {config.season}. "
                "Use --force to replace them."
            ],
            warnings=warnings
        )
    
    # Delete existing picks if force mode
    if existing and force:
        logger.info(f"Deleting {len(existing)} existing picks (--force mode)")
        db.execute(
            "DELETE FROM draft WHERE season_id = ? AND draft_type = ?",
            (season_id, config.draft_type)
        )
        # Also remove from roster
        for team_id, player_id, _ in existing:
            db.execute(
                """DELETE FROM roster 
                   WHERE team_id = ? AND player_id = ? AND season_id = ?""",
                (team_id, player_id, season_id)
            )
    
    # Insert picks
    inserted = 0
    for pick, pick_order in valid_picks:
        try:
            # Insert into draft table
            db.execute(
                """INSERT INTO draft 
                   (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    season_id,
                    pick.team,
                    pick.player_id,
                    pick.round,
                    pick_order,
                    config.draft_type,
                    config.application_date
                )
            )
            
            # Insert into roster table
            db.execute(
                """INSERT OR IGNORE INTO roster 
                   (team_id, player_id, season_id, joined_date)
                   VALUES (?, ?, ?, ?)""",
                (pick.team, pick.player_id, season_id, config.application_date)
            )
            
            inserted += 1
            logger.debug(f"Inserted: {pick.team} - {pick.round} - {pick.player_name}")
            
        except Exception as e:
            errors.append(f"Failed to insert {pick.player_name}: {str(e)}")
    
    logger.info(f"Successfully inserted {inserted} picks, skipped {skipped}")
    
    # Insert fa_config if present (only for main draft)
    if config.fa_config and config.draft_type == "main":
        fc = config.fa_config
        db.execute(
            """INSERT OR REPLACE INTO fa_config 
               (season_id, roster_size, supplemental_bonus, 
                min_pitchers, min_catchers, min_infielders, min_outfielders)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (season_id, fc.roster_size, fc.supplemental_bonus,
             fc.min_pitchers, fc.min_catchers, fc.min_infielders, fc.min_outfielders)
        )
        logger.info(f"Saved FA config: roster={fc.roster_size}, bonus={fc.supplemental_bonus}")
    
    return LoadResult(
        success=len(errors) == 0,
        inserted_count=inserted,
        skipped_count=skipped,
        errors=errors,
        warnings=warnings
    )


def parse_csv_header(header_line: str) -> dict:
    """Parse CSV header comment into metadata dict.
    
    Expected format: # season=2025,draft_type=main,application_date=2025-03-21,description=...
    """
    if not header_line.startswith('#'):
        return {}
    
    content = header_line[1:].strip()
    metadata = {}
    
    for pair in content.split(','):
        if '=' in pair:
            key, value = pair.split('=', 1)
            metadata[key.strip()] = value.strip()
    
    return metadata


def load_draft_from_csv(
    csv_path: Path,
    db_path: Path = DEFAULT_DB_PATH,
    dry_run: bool = False,
    force: bool = False
) -> LoadResult:
    """Load draft picks from CSV file into database.
    
    CSV format (row order = pick order):
        # season=2025,draft_type=main,application_date=2025-03-21,description=2025시즌
        pick_order,team,round,player_id,player_name
        1,준,용타,15004,김도영
        2,준,용투1,16088,변우혁
        ...
    
    Args:
        csv_path: Path to CSV draft file
        db_path: Path to SQLite database
        dry_run: If True, validate only without inserting
        force: If True, delete existing picks before inserting
        
    Returns:
        LoadResult with counts and any errors/warnings
    """
    import csv
    
    errors: List[str] = []
    warnings: List[str] = []
    
    if not csv_path.exists():
        return LoadResult(
            success=False,
            inserted_count=0,
            skipped_count=0,
            errors=[f"CSV file not found: {csv_path}"],
            warnings=[]
        )
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        first_line = f.readline()
        metadata = parse_csv_header(first_line)
        
        if not metadata:
            return LoadResult(
                success=False,
                inserted_count=0,
                skipped_count=0,
                errors=["CSV must start with header comment: # season=...,draft_type=...,application_date=..."],
                warnings=[]
            )
        
        required_fields = ['season', 'draft_type', 'application_date']
        missing = [f for f in required_fields if f not in metadata]
        if missing:
            return LoadResult(
                success=False,
                inserted_count=0,
                skipped_count=0,
                errors=[f"Missing required metadata fields: {missing}"],
                warnings=[]
            )
        
        try:
            season = int(metadata['season'])
        except ValueError:
            return LoadResult(
                success=False,
                inserted_count=0,
                skipped_count=0,
                errors=[f"Invalid season value: {metadata['season']}"],
                warnings=[]
            )
        
        draft_type = metadata['draft_type']
        if draft_type not in ('main', 'supplemental'):
            return LoadResult(
                success=False,
                inserted_count=0,
                skipped_count=0,
                errors=[f"Invalid draft_type: {draft_type}. Must be 'main' or 'supplemental'"],
                warnings=[]
            )
        
        application_date = metadata['application_date']
        description = metadata.get('description', '')
        
        reader = csv.DictReader(f)
        
        required_columns = ['team', 'round', 'player_id', 'player_name']
        if not reader.fieldnames:
            return LoadResult(
                success=False,
                inserted_count=0,
                skipped_count=0,
                errors=["CSV has no header row"],
                warnings=[]
            )
        
        missing_cols = [c for c in required_columns if c not in reader.fieldnames]
        if missing_cols:
            return LoadResult(
                success=False,
                inserted_count=0,
                skipped_count=0,
                errors=[f"Missing required columns: {missing_cols}"],
                warnings=[]
            )
        
        picks = []
        for row_num, row in enumerate(reader, start=2):
            pick = DraftPick(
                team=row['team'],
                round=row['round'],
                player_id=str(row['player_id']),
                player_name=row['player_name']
            )
            
            if 'pick_order' in reader.fieldnames and row.get('pick_order'):
                pick_order = int(row['pick_order'])
            else:
                pick_order = row_num - 1
            
            picks.append((pick, pick_order))
    
    logger.info(f"Loading draft from CSV: {csv_path.name}")
    logger.info(f"  Season: {season}, Type: {draft_type}")
    logger.info(f"  Application date: {application_date}")
    logger.info(f"  Total picks: {len(picks)}")
    
    db = DatabaseManager(db_path)
    
    season_id = create_season_if_not_exists(db, season)
    
    valid_picks: List[Tuple[DraftPick, int]] = []
    skipped = 0
    
    for pick, pick_order in picks:
        if not validate_team(db, pick.team):
            errors.append(f"Invalid team '{pick.team}' for player {pick.player_name}")
            continue
        
        if pick.player_id == "XXXXX" or not pick.player_id.isdigit():
            warnings.append(f"Skipping placeholder pick: {pick.player_name} ({pick.player_id})")
            skipped += 1
            continue
        
        player_exists, db_name = validate_player(db, pick.player_id)
        if not player_exists:
            warnings.append(
                f"Player ID {pick.player_id} ({pick.player_name}) not in database. "
                "Will be added when scraper runs."
            )
        elif db_name != pick.player_name:
            warnings.append(
                f"Name mismatch for {pick.player_id}: "
                f"CSV='{pick.player_name}', DB='{db_name}'"
            )
        
        valid_picks.append((pick, pick_order))
    
    if errors:
        return LoadResult(
            success=False,
            inserted_count=0,
            skipped_count=skipped,
            errors=errors,
            warnings=warnings
        )
    
    if dry_run:
        logger.info(f"DRY RUN: Would insert {len(valid_picks)} picks, skip {skipped}")
        return LoadResult(
            success=True,
            inserted_count=0,
            skipped_count=skipped,
            errors=[],
            warnings=warnings
        )
    
    existing = check_existing_draft(db, season_id, draft_type)
    if existing and not force:
        return LoadResult(
            success=False,
            inserted_count=0,
            skipped_count=0,
            errors=[
                f"Found {len(existing)} existing {draft_type} draft picks for season {season}. "
                "Use --force to replace them."
            ],
            warnings=warnings
        )
    
    if existing and force:
        logger.info(f"Deleting {len(existing)} existing picks (--force mode)")
        db.execute(
            "DELETE FROM draft WHERE season_id = ? AND draft_type = ?",
            (season_id, draft_type)
        )
        for team_id, player_id, _ in existing:
            db.execute(
                """DELETE FROM roster 
                   WHERE team_id = ? AND player_id = ? AND season_id = ?""",
                (team_id, player_id, season_id)
            )
    
    inserted = 0
    for pick, pick_order in valid_picks:
        try:
            db.execute(
                """INSERT INTO draft 
                   (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    season_id,
                    pick.team,
                    pick.player_id,
                    pick.round,
                    pick_order,
                    draft_type,
                    application_date
                )
            )
            
            db.execute(
                """INSERT OR IGNORE INTO roster 
                   (team_id, player_id, season_id, joined_date)
                   VALUES (?, ?, ?, ?)""",
                (pick.team, pick.player_id, season_id, application_date)
            )
            
            inserted += 1
            logger.debug(f"Inserted: {pick.team} - {pick.round} - {pick.player_name}")
            
        except Exception as e:
            errors.append(f"Failed to insert {pick.player_name}: {str(e)}")
    
    logger.info(f"Successfully inserted {inserted} picks, skipped {skipped}")
    
    return LoadResult(
        success=len(errors) == 0,
        inserted_count=inserted,
        skipped_count=skipped,
        errors=errors,
        warnings=warnings
    )


def load_all_drafts_for_year(
    year: int,
    config_dir: Path = DEFAULT_CONFIG_DIR,
    db_path: Path = DEFAULT_DB_PATH,
    dry_run: bool = False,
    force: bool = False
) -> dict[str, LoadResult]:
    """Load all draft files for a given year.
    
    Args:
        year: Season year (e.g., 2025)
        config_dir: Directory containing JSON/CSV config files
        db_path: Path to database
        dry_run: Validate only
        force: Replace existing picks
        
    Returns:
        Dict mapping filename to LoadResult
    """
    results = {}
    
    json_files = sorted(config_dir.glob(f"{year}_*.json"))
    csv_files = sorted(config_dir.glob(f"{year}_*.csv"))
    
    all_files = json_files + csv_files
    
    if not all_files:
        logger.warning(f"No draft files found for year {year} in {config_dir}")
        return results
    
    for file_path in all_files:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {file_path.name}")
        logger.info(f"{'='*60}")
        
        if file_path.suffix == '.csv':
            result = load_draft_from_csv(
                csv_path=file_path,
                db_path=db_path,
                dry_run=dry_run,
                force=force
            )
        else:
            result = load_draft_from_json(
                json_path=file_path,
                db_path=db_path,
                dry_run=dry_run,
                force=force
            )
        results[file_path.name] = result
        
        if result.success:
            logger.info(f"SUCCESS: {result.inserted_count} inserted, {result.skipped_count} skipped")
        else:
            logger.error(f"FAILED: {result.errors}")
        
        if result.warnings:
            for w in result.warnings:
                logger.warning(w)
    
    return results


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Load draft picks from JSON or CSV configuration files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load a specific draft file (JSON or CSV)
  python -m app.core.draft_loader config/drafts/2025_main.json
  python -m app.core.draft_loader config/drafts/2025_main.csv

  # Validate without inserting (dry run)
  python -m app.core.draft_loader config/drafts/2025_main.csv --dry-run

  # Replace existing picks
  python -m app.core.draft_loader config/drafts/2025_main.csv --force

  # Load all drafts for a year
  python -m app.core.draft_loader --year 2025

  # Show verbose output
  python -m app.core.draft_loader config/drafts/2025_main.csv -v
        """
    )
    
    parser.add_argument(
        "draft_file",
        nargs="?",
        help="Path to JSON or CSV draft configuration file"
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Load all draft files for this year"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate only, don't insert into database"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete existing picks before inserting"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to database (default: {DEFAULT_DB_PATH})"
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=DEFAULT_CONFIG_DIR,
        help=f"Directory for draft configs (default: {DEFAULT_CONFIG_DIR})"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not args.draft_file and not args.year:
        parser.error("Either draft_file or --year is required")
    
    if args.draft_file and args.year:
        parser.error("Cannot specify both draft_file and --year")
    
    # Execute
    if args.year:
        results = load_all_drafts_for_year(
            year=args.year,
            config_dir=args.config_dir,
            db_path=args.db,
            dry_run=args.dry_run,
            force=args.force
        )
        
        # Print summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        
        total_inserted = 0
        total_skipped = 0
        all_success = True
        
        for filename, result in results.items():
            status = "OK" if result.success else "FAILED"
            print(f"  {filename}: {status} ({result.inserted_count} inserted, {result.skipped_count} skipped)")
            total_inserted += result.inserted_count
            total_skipped += result.skipped_count
            if not result.success:
                all_success = False
                for err in result.errors:
                    print(f"    ERROR: {err}")
        
        print(f"\nTotal: {total_inserted} inserted, {total_skipped} skipped")
        sys.exit(0 if all_success else 1)
    
    else:
        file_path = Path(args.draft_file)
        
        if file_path.suffix == '.csv':
            result = load_draft_from_csv(
                csv_path=file_path,
                db_path=args.db,
                dry_run=args.dry_run,
                force=args.force
            )
        else:
            result = load_draft_from_json(
                json_path=file_path,
                db_path=args.db,
                dry_run=args.dry_run,
                force=args.force
            )
        
        # Print result
        print(f"\n{'='*60}")
        if result.success:
            print(f"SUCCESS: {result.inserted_count} picks inserted, {result.skipped_count} skipped")
        else:
            print("FAILED:")
            for err in result.errors:
                print(f"  ERROR: {err}")
        
        if result.warnings:
            print("\nWarnings:")
            for w in result.warnings:
                print(f"  WARNING: {w}")
        
        sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
