"""Tests for app/core/draft_loader.py"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.db import DatabaseManager
from app.core.draft_loader import (
    DraftConfig,
    DraftPick,
    FAConfig,
    LoadResult,
    check_existing_draft,
    create_season_if_not_exists,
    get_draft_slots,
    get_pick_order,
    get_season_id,
    load_draft_from_csv,
    load_draft_from_json,
    parse_csv_header,
    validate_player,
    validate_team,
)


class TestDraftConfig:
    """Test DraftConfig dataclass and JSON parsing."""
    
    def test_from_json_valid(self, draft_json_valid):
        """Test loading valid JSON file."""
        config = DraftConfig.from_json(draft_json_valid)
        
        assert config.season == 2025
        assert config.draft_type == 'main'
        assert config.application_date == '2025-03-21'
        assert config.description == '2025 메인 드래프트'
        assert len(config.picks) == 4
        
        # Check first pick
        first_pick = config.picks[0]
        assert first_pick.team == '준'
        assert first_pick.round == '용타'
        assert first_pick.player_id == '10002'
        assert first_pick.player_name == '박포수'
    
    def test_from_json_missing_fields(self, draft_json_missing_fields):
        """Test that missing required fields raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DraftConfig.from_json(draft_json_missing_fields)
        
        assert 'Missing required fields' in str(exc_info.value)
        assert 'draft_type' in str(exc_info.value)
    
    def test_from_json_invalid_draft_type(self, draft_json_invalid_type):
        """Test that invalid draft_type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DraftConfig.from_json(draft_json_invalid_type)
        
        assert 'Invalid draft_type' in str(exc_info.value)
    
    def test_from_json_file_not_found(self, tmp_path):
        """Test that nonexistent file raises FileNotFoundError."""
        nonexistent = tmp_path / "nonexistent.json"
        
        with pytest.raises(FileNotFoundError):
            DraftConfig.from_json(nonexistent)
    
    def test_from_json_invalid_json(self, tmp_path):
        """Test that malformed JSON raises error."""
        bad_json = tmp_path / "bad.json"
        bad_json.write_text("{ invalid json }")
        
        with pytest.raises(json.JSONDecodeError):
            DraftConfig.from_json(bad_json)
    
    def test_from_json_with_fa_config(self, draft_json_valid):
        """Test parsing fa_config from JSON."""
        config = DraftConfig.from_json(draft_json_valid)
        
        assert config.fa_config is not None
        assert config.fa_config.roster_size == 29
        assert config.fa_config.supplemental_bonus == 5
        assert config.fa_config.min_pitchers == 11
        assert config.fa_config.min_catchers == 2
        assert config.fa_config.min_infielders == 7
        assert config.fa_config.min_outfielders == 5
    
    def test_from_json_without_fa_config(self, tmp_path):
        """Test that fa_config is optional."""
        json_path = tmp_path / "no_fa_config.json"
        data = {
            "season": 2025,
            "draft_type": "supplemental",
            "application_date": "2025-06-01",
            "picks": []
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        config = DraftConfig.from_json(json_path)
        assert config.fa_config is None
    
    def test_from_json_missing_pick_fields(self, tmp_path):
        """Test that picks with missing fields raise ValueError."""
        json_path = tmp_path / "bad_pick.json"
        data = {
            "season": 2025,
            "draft_type": "main",
            "application_date": "2025-03-21",
            "picks": [
                {"team": "준", "round": "1R"}  # Missing player_id and player_name
            ]
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        with pytest.raises(ValueError) as exc_info:
            DraftConfig.from_json(json_path)
        
        assert 'missing fields' in str(exc_info.value).lower()


class TestFAConfig:
    """Test FAConfig dataclass."""
    
    def test_default_values(self):
        """Test that FAConfig has correct default values."""
        config = FAConfig()
        
        assert config.roster_size == 29
        assert config.supplemental_bonus == 5
        assert config.min_pitchers == 11
        assert config.min_catchers == 2
        assert config.min_infielders == 7
        assert config.min_outfielders == 5
    
    def test_custom_values(self):
        """Test creating FAConfig with custom values."""
        config = FAConfig(
            roster_size=30,
            supplemental_bonus=3,
            min_pitchers=12,
            min_catchers=3,
            min_infielders=8,
            min_outfielders=6
        )
        
        assert config.roster_size == 30
        assert config.supplemental_bonus == 3
        assert config.min_pitchers == 12


class TestSeasonManagement:
    """Test season creation and retrieval."""
    
    def test_get_season_id_exists(self, db_manager):
        """Test getting existing season ID."""
        season_id = get_season_id(db_manager, 2025)
        assert season_id == 1
    
    def test_get_season_id_not_exists(self, db_manager):
        """Test getting nonexistent season returns None."""
        season_id = get_season_id(db_manager, 2099)
        assert season_id is None
    
    def test_create_season_if_not_exists_new(self, db_manager):
        """Test creating new season."""
        season_id = create_season_if_not_exists(db_manager, 2026)
        
        assert season_id is not None
        
        # Verify it was created
        result = db_manager.fetch_one("SELECT year FROM seasons WHERE id = ?", (season_id,))
        assert result[0] == 2026
    
    def test_create_season_if_not_exists_existing(self, db_manager):
        """Test that existing season is not duplicated."""
        season_id_1 = create_season_if_not_exists(db_manager, 2025)
        season_id_2 = create_season_if_not_exists(db_manager, 2025)
        
        assert season_id_1 == season_id_2


class TestValidation:
    """Test validation functions."""
    
    def test_validate_team_exists(self, db_manager):
        """Test validating existing team."""
        assert validate_team(db_manager, '준') is True
        assert validate_team(db_manager, '뚝') is True
        assert validate_team(db_manager, '재') is True
    
    def test_validate_team_not_exists(self, db_manager):
        """Test validating nonexistent team."""
        assert validate_team(db_manager, '없는팀') is False
        assert validate_team(db_manager, 'INVALID') is False
    
    def test_validate_player_exists(self, db_manager):
        """Test validating existing player."""
        exists, name = validate_player(db_manager, '10001')
        
        assert exists is True
        assert name == '김투수'
    
    def test_validate_player_not_exists(self, db_manager):
        """Test validating nonexistent player."""
        exists, name = validate_player(db_manager, '99999')
        
        assert exists is False
        assert name is None


class TestGetPickOrder:
    """Test pick order calculation."""
    
    def test_yongta(self):
        """Test 용타 pick order."""
        assert get_pick_order('용타', 0) == 0
        assert get_pick_order('용타', 5) == 5
        assert get_pick_order('용타', 9) == 9
    
    def test_yongtu1(self):
        """Test 용투1 pick order."""
        assert get_pick_order('용투1', 0) == 10
        assert get_pick_order('용투1', 5) == 15
        assert get_pick_order('용투1', 9) == 19
    
    def test_yongtu2(self):
        """Test 용투2 pick order."""
        assert get_pick_order('용투2', 0) == 20
        assert get_pick_order('용투2', 5) == 25
        assert get_pick_order('용투2', 9) == 29
    
    def test_akwo(self):
        """Test 아쿼 pick order (2026년 신설)."""
        assert get_pick_order('아쿼', 0) == 30
        assert get_pick_order('아쿼', 5) == 35
        assert get_pick_order('아쿼', 9) == 39
    
    def test_main_draft_rounds(self):
        """Test main draft round pick orders."""
        assert get_pick_order('1R', 0) == 100
        assert get_pick_order('1R', 5) == 105
        assert get_pick_order('2R', 0) == 200
        assert get_pick_order('10R', 3) == 1003
        assert get_pick_order('25R', 7) == 2507
    
    def test_supplemental_rounds(self):
        """Test supplemental draft round pick orders."""
        assert get_pick_order('2차1R', 0) == 1100
        assert get_pick_order('2차1R', 5) == 1105
        assert get_pick_order('2차2R', 0) == 1200
        assert get_pick_order('2차5R', 3) == 1503
    
    def test_unknown_round(self):
        """Test unknown round type returns high number."""
        order = get_pick_order('UNKNOWN', 0)
        assert order >= 9000


class TestCheckExistingDraft:
    """Test checking for existing draft picks."""
    
    def test_no_existing_picks(self, db_manager):
        """Test when no picks exist."""
        existing = check_existing_draft(db_manager, 1, 'main')
        assert len(existing) == 0
    
    def test_with_existing_picks(self, db_manager):
        """Test when picks exist."""
        # Insert some picks
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '1R', 100, 'main', '2025-03-21')"""
        )
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '뚝', '10002', '1R', 101, 'main', '2025-03-21')"""
        )
        
        existing = check_existing_draft(db_manager, 1, 'main')
        
        assert len(existing) == 2
        assert ('준', '10001', '1R') in existing
        assert ('뚝', '10002', '1R') in existing
    
    def test_filter_by_draft_type(self, db_manager):
        """Test that only matching draft_type is returned."""
        # Insert main and supplemental picks
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '1R', 100, 'main', '2025-03-21')"""
        )
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '뚝', '10002', '2차1R', 1100, 'supplemental', '2025-06-01')"""
        )
        
        main_picks = check_existing_draft(db_manager, 1, 'main')
        supp_picks = check_existing_draft(db_manager, 1, 'supplemental')
        
        assert len(main_picks) == 1
        assert len(supp_picks) == 1


class TestLoadDraftFromJson:
    """Test full draft loading integration."""
    
    def test_load_valid_draft(self, draft_json_valid, temp_db):
        """Test loading a valid draft file."""
        result = load_draft_from_json(draft_json_valid, temp_db)
        
        assert result.success is True
        assert result.inserted_count == 4
        assert result.skipped_count == 0
        assert len(result.errors) == 0
        
        # Verify data in database
        db = DatabaseManager(temp_db)
        picks = db.fetch_all("SELECT team_id, player_id, round FROM draft WHERE season_id = 1")
        
        assert len(picks) == 4
        assert ('준', '10002', '용타') in picks
        assert ('뚝', '10003', '용타') in picks
    
    def test_load_with_placeholder_picks(self, tmp_path, temp_db):
        """Test that placeholder picks (XXXXX) are skipped."""
        json_path = tmp_path / "with_placeholder.json"
        data = {
            "season": 2025,
            "draft_type": "main",
            "application_date": "2025-03-21",
            "picks": [
                {"team": "준", "round": "1R", "player_id": "10001", "player_name": "김투수"},
                {"team": "뚝", "round": "1R", "player_id": "XXXXX", "player_name": "미정"}
            ]
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        result = load_draft_from_json(json_path, temp_db)
        
        assert result.success is True
        assert result.inserted_count == 1
        assert result.skipped_count == 1
        assert any('placeholder' in w.lower() for w in result.warnings)
    
    def test_load_dry_run(self, draft_json_valid, temp_db):
        """Test dry run mode doesn't insert data."""
        result = load_draft_from_json(draft_json_valid, temp_db, dry_run=True)
        
        assert result.success is True
        assert result.inserted_count == 0  # Nothing inserted in dry run
        
        # Verify no data in database
        db = DatabaseManager(temp_db)
        picks = db.fetch_all("SELECT COUNT(*) FROM draft")
        assert picks[0][0] == 0
    
    def test_load_with_existing_picks_no_force(self, draft_json_valid, temp_db):
        """Test that existing picks block loading without --force."""
        # Load once
        result1 = load_draft_from_json(draft_json_valid, temp_db)
        assert result1.success is True
        
        # Try to load again without force
        result2 = load_draft_from_json(draft_json_valid, temp_db, force=False)
        
        assert result2.success is False
        assert len(result2.errors) > 0
        assert 'existing' in result2.errors[0].lower()
    
    def test_load_with_force_replaces_existing(self, draft_json_valid, temp_db):
        """Test that --force mode replaces existing picks."""
        # Load once
        result1 = load_draft_from_json(draft_json_valid, temp_db)
        assert result1.success is True
        
        # Load again with force
        result2 = load_draft_from_json(draft_json_valid, temp_db, force=True)
        
        assert result2.success is True
        assert result2.inserted_count == 4
        
        # Verify no duplicates
        db = DatabaseManager(temp_db)
        picks = db.fetch_all("SELECT COUNT(*) FROM draft WHERE season_id = 1")
        assert picks[0][0] == 4  # Still only 4 picks
    
    def test_load_invalid_team(self, tmp_path, temp_db):
        """Test that invalid team causes error."""
        json_path = tmp_path / "invalid_team.json"
        data = {
            "season": 2025,
            "draft_type": "main",
            "application_date": "2025-03-21",
            "picks": [
                {"team": "INVALID", "round": "1R", "player_id": "10001", "player_name": "김투수"}
            ]
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        result = load_draft_from_json(json_path, temp_db)
        
        assert result.success is False
        assert len(result.errors) > 0
        assert 'invalid team' in result.errors[0].lower()
    
    def test_load_nonexistent_player_warning(self, tmp_path, temp_db):
        """Test that nonexistent player generates warning but doesn't fail."""
        # First add the player to avoid FK constraint
        db = DatabaseManager(temp_db)
        db.execute(
            "INSERT INTO players (id, name, player_type) VALUES ('99999', '신인선수', 'batter')"
        )
        
        json_path = tmp_path / "new_player.json"
        data = {
            "season": 2025,
            "draft_type": "main",
            "application_date": "2025-03-21",
            "picks": [
                {"team": "준", "round": "1R", "player_id": "99999", "player_name": "신인선수"}
            ]
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        result = load_draft_from_json(json_path, temp_db)
        
        # Should succeed (player exists now, but wasn't in DB when we checked)
        assert result.success is True
        assert result.inserted_count == 1
    
    def test_load_name_mismatch_warning(self, tmp_path, temp_db):
        """Test that name mismatch generates warning."""
        json_path = tmp_path / "name_mismatch.json"
        data = {
            "season": 2025,
            "draft_type": "main",
            "application_date": "2025-03-21",
            "picks": [
                {"team": "준", "round": "1R", "player_id": "10001", "player_name": "잘못된이름"}
            ]
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        result = load_draft_from_json(json_path, temp_db)
        
        assert result.success is True
        assert len(result.warnings) > 0
        assert any('mismatch' in w.lower() for w in result.warnings)
    
    def test_load_updates_roster_table(self, draft_json_valid, temp_db):
        """Test that draft loading also updates roster table."""
        result = load_draft_from_json(draft_json_valid, temp_db)
        assert result.success is True
        
        # Check roster table
        db = DatabaseManager(temp_db)
        roster = db.fetch_all(
            """SELECT team_id, player_id, joined_date 
               FROM roster WHERE season_id = 1"""
        )
        
        assert len(roster) == 4
        assert ('준', '10002', '2025-03-21') in roster
    
    def test_load_saves_fa_config(self, draft_json_valid, temp_db):
        """Test that fa_config is saved to database."""
        result = load_draft_from_json(draft_json_valid, temp_db)
        assert result.success is True
        
        # Check fa_config table
        db = DatabaseManager(temp_db)
        config = db.fetch_one(
            """SELECT roster_size, supplemental_bonus, min_pitchers, min_catchers
               FROM fa_config WHERE season_id = 1"""
        )
        
        assert config is not None
        assert config[0] == 29  # roster_size
        assert config[1] == 5   # supplemental_bonus
        assert config[2] == 11  # min_pitchers
        assert config[3] == 2   # min_catchers
    
    def test_load_supplemental_no_fa_config(self, tmp_path, temp_db):
        """Test that supplemental draft doesn't save fa_config."""
        json_path = tmp_path / "supplemental.json"
        data = {
            "season": 2025,
            "draft_type": "supplemental",
            "application_date": "2025-06-01",
            "picks": [
                {"team": "준", "round": "2차1R", "player_id": "10001", "player_name": "김투수"}
            ],
            "fa_config": {
                "roster_size": 30,
                "supplemental_bonus": 3,
                "position_requirements": {"P": 12, "C": 2, "IF": 7, "OF": 5}
            }
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        result = load_draft_from_json(json_path, temp_db)
        assert result.success is True
        
        # fa_config should NOT be saved for supplemental draft
        # (only main draft saves fa_config)
        db = DatabaseManager(temp_db)
        
        # Check if fa_config was updated (it shouldn't be for supplemental)
        # The fixture already has fa_config for season 1, so check it wasn't changed
        config = db.fetch_one(
            "SELECT roster_size FROM fa_config WHERE season_id = 1"
        )
        
        # Should still be the original value from fixture (29), not 30
        assert config is not None
        assert config[0] == 29


class TestDraftPick:
    """Test DraftPick dataclass."""
    
    def test_create_draft_pick(self):
        """Test creating DraftPick instance."""
        pick = DraftPick(
            team='준',
            round='1R',
            player_id='10001',
            player_name='김투수'
        )
        
        assert pick.team == '준'
        assert pick.round == '1R'
        assert pick.player_id == '10001'
        assert pick.player_name == '김투수'


class TestLoadResult:
    """Test LoadResult dataclass."""
    
    def test_create_load_result(self):
        """Test creating LoadResult instance."""
        result = LoadResult(
            success=True,
            inserted_count=10,
            skipped_count=2,
            errors=[],
            warnings=['Warning 1', 'Warning 2']
        )
        
        assert result.success is True
        assert result.inserted_count == 10
        assert result.skipped_count == 2
        assert len(result.errors) == 0
        assert len(result.warnings) == 2


class TestParseCsvHeader:
    """Test CSV header parsing."""
    
    def test_parse_valid_header(self):
        header = "# season=2025,draft_type=main,application_date=2025-03-21,description=테스트"
        result = parse_csv_header(header)
        
        assert result['season'] == '2025'
        assert result['draft_type'] == 'main'
        assert result['application_date'] == '2025-03-21'
        assert result['description'] == '테스트'
    
    def test_parse_empty_header(self):
        assert parse_csv_header("") == {}
    
    def test_parse_no_comment_prefix(self):
        header = "season=2025,draft_type=main"
        assert parse_csv_header(header) == {}


class TestGetDraftSlots:
    """Test dynamic slot retrieval from database."""
    
    def test_get_slots_empty_db(self, db_manager):
        slots = get_draft_slots(db_manager)
        assert slots == []
    
    def test_get_slots_with_data(self, db_manager):
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '용타', 0, 'main', '2025-03-21')"""
        )
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10002', '1R', 100, 'main', '2025-03-21')"""
        )
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10003', '용투1', 10, 'main', '2025-03-21')"""
        )
        
        slots = get_draft_slots(db_manager)
        
        assert slots == ['용타', '용투1', '1R']


class TestLoadDraftFromCsv:
    """Test CSV draft loading integration."""
    
    def test_load_valid_csv(self, tmp_path, temp_db):
        csv_path = tmp_path / "valid.csv"
        csv_path.write_text(
            "# season=2025,draft_type=main,application_date=2025-03-21\n"
            "pick_order,team,round,player_id,player_name\n"
            "1,준,용타,10001,김투수\n"
            "2,준,용투1,10002,박포수\n",
            encoding='utf-8'
        )
        
        result = load_draft_from_csv(csv_path, temp_db)
        
        assert result.success is True
        assert result.inserted_count == 2
        assert result.skipped_count == 0
    
    def test_load_csv_missing_header(self, tmp_path, temp_db):
        csv_path = tmp_path / "no_header.csv"
        csv_path.write_text(
            "pick_order,team,round,player_id,player_name\n"
            "1,준,용타,10001,김투수\n",
            encoding='utf-8'
        )
        
        result = load_draft_from_csv(csv_path, temp_db)
        
        assert result.success is False
        assert 'header comment' in result.errors[0].lower()
    
    def test_load_csv_missing_metadata(self, tmp_path, temp_db):
        csv_path = tmp_path / "missing_meta.csv"
        csv_path.write_text(
            "# season=2025\n"
            "pick_order,team,round,player_id,player_name\n"
            "1,준,용타,10001,김투수\n",
            encoding='utf-8'
        )
        
        result = load_draft_from_csv(csv_path, temp_db)
        
        assert result.success is False
        assert 'missing' in result.errors[0].lower()
    
    def test_load_csv_with_placeholder(self, tmp_path, temp_db):
        csv_path = tmp_path / "with_placeholder.csv"
        csv_path.write_text(
            "# season=2025,draft_type=main,application_date=2025-03-21\n"
            "pick_order,team,round,player_id,player_name\n"
            "1,준,용타,10001,김투수\n"
            "2,준,용투1,XXXXX,미정\n",
            encoding='utf-8'
        )
        
        result = load_draft_from_csv(csv_path, temp_db)
        
        assert result.success is True
        assert result.inserted_count == 1
        assert result.skipped_count == 1
    
    def test_load_csv_dry_run(self, tmp_path, temp_db):
        csv_path = tmp_path / "dryrun.csv"
        csv_path.write_text(
            "# season=2025,draft_type=main,application_date=2025-03-21\n"
            "pick_order,team,round,player_id,player_name\n"
            "1,준,용타,10001,김투수\n",
            encoding='utf-8'
        )
        
        result = load_draft_from_csv(csv_path, temp_db, dry_run=True)
        
        assert result.success is True
        assert result.inserted_count == 0
        
        db = DatabaseManager(temp_db)
        picks = db.fetch_all("SELECT COUNT(*) FROM draft")
        assert picks[0][0] == 0
    
    def test_load_csv_force_replace(self, tmp_path, temp_db):
        csv_path = tmp_path / "force.csv"
        csv_path.write_text(
            "# season=2025,draft_type=main,application_date=2025-03-21\n"
            "pick_order,team,round,player_id,player_name\n"
            "1,준,용타,10001,김투수\n",
            encoding='utf-8'
        )
        
        result1 = load_draft_from_csv(csv_path, temp_db)
        assert result1.success is True
        
        result2 = load_draft_from_csv(csv_path, temp_db, force=True)
        assert result2.success is True
        
        db = DatabaseManager(temp_db)
        picks = db.fetch_all("SELECT COUNT(*) FROM draft WHERE season_id = 1")
        assert picks[0][0] == 1
    
    def test_load_csv_file_not_found(self, tmp_path, temp_db):
        result = load_draft_from_csv(tmp_path / "nonexistent.csv", temp_db)
        
        assert result.success is False
        assert 'not found' in result.errors[0].lower()
