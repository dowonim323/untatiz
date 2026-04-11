"""Tests for app/services/war_calculator.py"""

from __future__ import annotations

import pandas as pd
import pytest

from app.services.war_calculator import (
    FAConfig,
    PlayerFAWar,
    calculate_player_fa_war,
    get_all_fa_players,
    get_fa_config,
    get_position_group,
    get_war,
    has_supplemental_draft,
    select_fa_roster,
)


class TestGetPositionGroup:
    """Test position group mapping."""
    
    def test_pitcher(self):
        assert get_position_group('P') == 'P'
        assert get_position_group('p') == 'P'
    
    def test_catcher(self):
        assert get_position_group('C') == 'C'
        assert get_position_group('c') == 'C'
    
    def test_infielders(self):
        assert get_position_group('1B') == 'IF'
        assert get_position_group('2B') == 'IF'
        assert get_position_group('SS') == 'IF'
        assert get_position_group('3B') == 'IF'
        assert get_position_group('ss') == 'IF'
    
    def test_outfielders(self):
        assert get_position_group('LF') == 'OF'
        assert get_position_group('CF') == 'OF'
        assert get_position_group('RF') == 'OF'
        assert get_position_group('lf') == 'OF'
    
    def test_dh_and_unknown(self):
        assert get_position_group('DH') == 'NONE'
        assert get_position_group('dh') == 'NONE'
        assert get_position_group('UNKNOWN') == 'NONE'
    
    def test_none(self):
        assert get_position_group(None) == 'NONE'


class TestGetFAConfig:
    """Test FA configuration loading."""
    
    def test_load_existing_config(self, db_manager):
        config = get_fa_config(db_manager, season_id=1)
        
        assert config.roster_size == 29
        assert config.supplemental_bonus == 5
        assert config.min_pitchers == 11
        assert config.min_catchers == 2
        assert config.min_infielders == 7
        assert config.min_outfielders == 5
    
    def test_load_nonexistent_config(self, db_manager):
        # Season 999 doesn't exist
        config = get_fa_config(db_manager, season_id=999)
        
        # Should return default values
        assert config.roster_size == 29
        assert config.supplemental_bonus == 5
        assert config.min_pitchers == 11
class TestCalculatePlayerFAWar:
    """Test FA WAR calculation for individual players."""
    
    def test_never_drafted_full_war(self, db_manager):
        """Player never drafted → full WAR is FA WAR."""
        current_war = 2.5
        fa_war = calculate_player_fa_war(
            db_manager, '10001', current_war, 1, '2025-03-21'
        )
        
        assert fa_war == 2.5
    
    def test_drafted_never_released(self, db_manager):
        """Player drafted but never released → FA WAR = 0."""
        # Add draft record
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '1R', 100, 'main', '2025-03-21')"""
        )
        
        current_war = 2.5
        fa_war = calculate_player_fa_war(
            db_manager, '10001', current_war, 1, '2025-03-21'
        )
        
        assert fa_war == 0.0
    
    def test_released_once(self, db_manager):
        """Player drafted, then released → FA WAR = current - release_war."""
        # Draft player
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '1R', 100, 'main', '2025-03-21')"""
        )
        
        # Add WAR data for transaction date
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-05-01', 1.5)"""
        )
        
        # Release player (from team to FA)
        db_manager.execute(
            """INSERT INTO transactions (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
               VALUES ('10001', 1, '준', NULL, '2025-05-01', 1.5)"""
        )
        
        current_war = 3.2
        fa_war = calculate_player_fa_war(
            db_manager, '10001', current_war, 1, '2025-03-21'
        )
        
        assert fa_war == 3.2
    
    def test_released_and_reacquired(self, db_manager):
        """Player released, then re-acquired → FA WAR only for FA period."""
        # Draft player
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '1R', 100, 'main', '2025-03-21')"""
        )
        
        # Add WAR data for transaction dates
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-05-01', 1.5)"""
        )
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-06-01', 2.3)"""
        )
        
        # Release player (WAR = 1.5)
        db_manager.execute(
            """INSERT INTO transactions (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
               VALUES ('10001', 1, '준', NULL, '2025-05-01', 1.5)"""
        )
        
        # Re-acquire player (WAR = 2.3)
        db_manager.execute(
            """INSERT INTO transactions (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
               VALUES ('10001', 1, NULL, '뚝', '2025-06-01', 2.3)"""
        )
        
        current_war = 3.5
        fa_war = calculate_player_fa_war(
            db_manager, '10001', current_war, 1, '2025-03-21'
        )
        
        assert fa_war == 1.5
    
    def test_multiple_release_cycles(self, db_manager):
        """Player released, re-acquired, released again → sum of FA periods."""
        # Draft player
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '1R', 100, 'main', '2025-03-21')"""
        )
        
        # Add WAR data for transaction dates
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-04-15', 1.0)"""
        )
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-05-01', 1.8)"""
        )
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-06-01', 2.5)"""
        )
        
        # First release (WAR = 1.0)
        db_manager.execute(
            """INSERT INTO transactions (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
               VALUES ('10001', 1, '준', NULL, '2025-04-15', 1.0)"""
        )
        
        # Re-acquire (WAR = 1.8)
        db_manager.execute(
            """INSERT INTO transactions (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
               VALUES ('10001', 1, NULL, '뚝', '2025-05-01', 1.8)"""
        )
        
        # Second release (WAR = 2.5)
        db_manager.execute(
            """INSERT INTO transactions (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
               VALUES ('10001', 1, '뚝', NULL, '2025-06-01', 2.5)"""
        )
        
        current_war = 3.5
        fa_war = calculate_player_fa_war(
            db_manager, '10001', current_war, 1, '2025-03-21'
        )
        
        assert fa_war == 2.7
    
    def test_never_drafted_with_acquisition(self, db_manager):
        """Player never drafted, then acquired from FA."""
        # Add WAR data for transaction date
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-05-01', 1.5)"""
        )
        
        # Acquire from FA (no draft record)
        db_manager.execute(
            """INSERT INTO transactions (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
               VALUES ('10001', 1, NULL, '준', '2025-05-01', 1.5)"""
        )
        
        current_war = 3.0
        fa_war = calculate_player_fa_war(
            db_manager, '10001', current_war, 1, '2025-03-21'
        )
        
        assert fa_war == 0.0

    def test_application_date_keeps_pre_acquisition_war_as_fa(self, db_manager):
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '2차1R', 100, 'supplemental', '2025-06-01')"""
        )
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-06-01', 1.2)"""
        )

        fa_war = calculate_player_fa_war(
            db_manager,
            '10001',
            2.0,
            1,
            '2025-03-21',
            '2025-06-15',
        )

        assert fa_war == 0.0

    def test_target_date_before_application_date_keeps_full_fa_war(self, db_manager):
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '2차1R', 100, 'supplemental', '2025-06-01')"""
        )

        fa_war = calculate_player_fa_war(
            db_manager,
            '10001',
            0.8,
            1,
            '2025-03-21',
            '2025-05-31',
        )

        assert fa_war == 0.8


class TestSelectFARoster:
    """Test FA roster selection with position requirements."""
    
    def test_basic_selection(self):
        """Test basic roster selection meeting position requirements."""
        players = [
            PlayerFAWar('p1', 'Pitcher1', 'P', 'P', 3.0, 3.0),
            PlayerFAWar('p2', 'Pitcher2', 'P', 'P', 2.5, 2.5),
            PlayerFAWar('c1', 'Catcher1', 'C', 'C', 2.0, 2.0),
            PlayerFAWar('if1', 'IF1', '1B', 'IF', 1.8, 1.8),
            PlayerFAWar('of1', 'OF1', 'LF', 'OF', 1.5, 1.5),
        ]
        
        config = FAConfig(
            roster_size=5,
            supplemental_bonus=0,
            min_pitchers=2,
            min_catchers=1,
            min_infielders=1,
            min_outfielders=1
        )
        
        roster = select_fa_roster(players, config, is_supplemental_active=False)
        
        assert len(roster) == 5
        
        # Check position requirements met
        position_counts = {}
        for p in roster:
            position_counts[p.position_group] = position_counts.get(p.position_group, 0) + 1
        
        assert position_counts.get('P', 0) >= 2
        assert position_counts.get('C', 0) >= 1
        assert position_counts.get('IF', 0) >= 1
        assert position_counts.get('OF', 0) >= 1
    
    def test_supplemental_bonus(self):
        """Test that supplemental draft adds bonus slots."""
        players = [
            PlayerFAWar(f'p{i}', f'Player{i}', 'P', 'P', float(i), float(i))
            for i in range(1, 40)
        ]
        
        config = FAConfig(
            roster_size=29,
            supplemental_bonus=5,
            min_pitchers=11,
            min_catchers=2,
            min_infielders=7,
            min_outfielders=5
        )
        
        # Without supplemental
        roster_no_supp = select_fa_roster(players, config, is_supplemental_active=False)
        assert len(roster_no_supp) == 29
        
        # With supplemental
        roster_with_supp = select_fa_roster(players, config, is_supplemental_active=True)
        assert len(roster_with_supp) == 34  # 29 + 5
    
    def test_insufficient_players_for_position(self):
        """Test when not enough players available for position requirement."""
        players = [
            PlayerFAWar('p1', 'Pitcher1', 'P', 'P', 3.0, 3.0),
            # Only 1 pitcher, but min_pitchers = 2
        ]
        
        config = FAConfig(
            roster_size=5,
            supplemental_bonus=0,
            min_pitchers=2,
            min_catchers=1,
            min_infielders=1,
            min_outfielders=1
        )
        
        roster = select_fa_roster(players, config, is_supplemental_active=False)
        
        # Should select what's available (1 pitcher)
        assert len(roster) == 1
        assert roster[0].position_group == 'P'
    
    def test_best_players_selected(self):
        """Test that highest FA WAR players are selected."""
        players = [
            PlayerFAWar('p1', 'P1', 'P', 'P', 5.0, 5.0),
            PlayerFAWar('p2', 'P2', 'P', 'P', 4.0, 4.0),
            PlayerFAWar('p3', 'P3', 'P', 'P', 3.0, 3.0),
            PlayerFAWar('p4', 'P4', 'P', 'P', 2.0, 2.0),
        ]
        
        config = FAConfig(
            roster_size=2,
            supplemental_bonus=0,
            min_pitchers=2,
            min_catchers=0,
            min_infielders=0,
            min_outfielders=0
        )
        
        roster = select_fa_roster(players, config, is_supplemental_active=False)
        
        assert len(roster) == 2
        assert roster[0].fa_war == 5.0
        assert roster[1].fa_war == 4.0


class TestGetAllFAPlayers:
    """Test getting all FA players from DataFrames."""
    
    def test_get_fa_batters(self, db_manager, sample_bat_df):
        """Test extracting FA batters."""
        # Don't roster any players - all should be FA
        fa_players = get_all_fa_players(
            db_manager, sample_bat_df, pd.DataFrame(), 1, '2025-03-21', '2025-03-21'
        )
        
        # All batters should be FA
        batter_ids = set(sample_bat_df.index)
        fa_ids = {p.player_id for p in fa_players}
        
        assert batter_ids.issubset(fa_ids)
    
    def test_get_fa_pitchers(self, db_manager, sample_pit_df):
        """Test extracting FA pitchers."""
        fa_players = get_all_fa_players(
            db_manager, pd.DataFrame(), sample_pit_df, 1, '2025-03-21', '2025-03-21'
        )
        
        # All pitchers should be FA
        pitcher_ids = set(sample_pit_df.index)
        fa_ids = {p.player_id for p in fa_players}
        
        assert pitcher_ids.issubset(fa_ids)
    
    def test_exclude_rostered_players(self, db_manager, sample_bat_df):
        """Test that rostered players are excluded."""
        # Roster one player
        db_manager.execute(
            """INSERT INTO roster (team_id, player_id, season_id, joined_date)
               VALUES ('준', '10002', 1, '2025-03-20')"""
        )

        fa_players = get_all_fa_players(
            db_manager, sample_bat_df, pd.DataFrame(), 1, '2025-03-21', '2025-03-21'
        )
        
        fa_ids = {p.player_id for p in fa_players}
        
        # Player 10002 should NOT be in FA
        assert '10002' not in fa_ids
        
        # Other players should be FA
        assert '10003' in fa_ids
    
    def test_skip_zero_war_players(self, db_manager):
        """Test that players with 0 or negative FA WAR are skipped."""
        bat_df = pd.DataFrame({
            'Name': ['Player1', 'Player2'],
            'POS': ['C', '1B'],
            'oWAR': [0.0, -0.5]
        }, index=['10001', '10002'])
        
        fa_players = get_all_fa_players(
            db_manager, bat_df, pd.DataFrame(), 1, '2025-03-21', '2025-03-21'
        )
        
        # No players should be selected (0 or negative WAR)
        assert len(fa_players) == 0

    def test_get_fa_players_uses_total_war_for_two_way_player(self, db_manager):
        db_manager.execute(
            "UPDATE players SET player_type = 'bat', position = 'OF' WHERE id = '10001'"
        )
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-03-21', 3.0)"""
        )
        bat_df = pd.DataFrame({'Name': ['TwoWay'], 'POS': ['OF'], 'oWAR': [1.0]}, index=['10001'])
        pit_df = pd.DataFrame({'Name': ['TwoWay'], 'WAR': [2.0]}, index=['10001'])

        fa_players = get_all_fa_players(
            db_manager,
            bat_df,
            pit_df,
            1,
            '2025-03-21',
            '2025-03-21',
        )

        assert len(fa_players) == 1
        assert fa_players[0].current_war == 3.0

    def test_get_fa_players_excludes_same_day_acquisition_from_fa(self, db_manager):
        db_manager.execute(
            """INSERT INTO war_daily (player_id, season_id, date, war)
               VALUES ('10001', 1, '2025-05-01', 1.5)"""
        )
        db_manager.execute(
            """INSERT INTO roster (team_id, player_id, season_id, joined_date)
               VALUES ('준', '10001', 1, '2025-05-01')"""
        )
        bat_df = pd.DataFrame({'Name': ['Player1'], 'POS': ['C'], 'oWAR': [1.5]}, index=['10001'])

        fa_players = get_all_fa_players(
            db_manager,
            bat_df,
            pd.DataFrame(),
            1,
            '2025-03-21',
            '2025-05-01',
        )

        assert {p.player_id for p in fa_players} == set()

    def test_get_fa_players_fallback_sums_two_way_war(self, db_manager):
        db_manager.execute(
            "UPDATE players SET player_type = 'bat', position = 'OF' WHERE id = '10001'"
        )

        bat_df = pd.DataFrame({'Name': ['TwoWay'], 'POS': ['OF'], 'oWAR': [1.25]}, index=['10001'])
        pit_df = pd.DataFrame({'Name': ['TwoWay'], 'WAR': [2.75]}, index=['10001'])

        fa_players = get_all_fa_players(
            db_manager,
            bat_df,
            pit_df,
            1,
            '2025-03-21',
            '2025-03-21',
        )

        assert len(fa_players) == 1
        assert fa_players[0].current_war == 4.0
        assert fa_players[0].position == 'OF'


class TestHasSupplementalDraft:
    """Test supplemental draft detection."""
    
    def test_no_supplemental_draft(self, db_manager):
        """Test when no supplemental draft exists."""
        result = has_supplemental_draft(db_manager, 1, '2025-06-01')
        assert result is False
    
    def test_supplemental_not_yet_active(self, db_manager):
        """Test when supplemental draft exists but not yet active."""
        # Add supplemental draft with future date
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '2차1R', 1100, 'supplemental', '2025-07-01')"""
        )
        
        result = has_supplemental_draft(db_manager, 1, '2025-06-01')
        assert result is False
    
    def test_supplemental_active(self, db_manager):
        """Test when supplemental draft is active."""
        # Add supplemental draft with past date
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '2차1R', 1100, 'supplemental', '2025-06-01')"""
        )
        
        result = has_supplemental_draft(db_manager, 1, '2025-06-15')
        assert result is True
    
    def test_supplemental_on_exact_date(self, db_manager):
        """Test on exact supplemental application date."""
        db_manager.execute(
            """INSERT INTO draft (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
               VALUES (1, '준', '10001', '2차1R', 1100, 'supplemental', '2025-06-01')"""
        )
        
        result = has_supplemental_draft(db_manager, 1, '2025-06-01')
        assert result is True
    def test_get_war_basic(self, sample_bat_df, sample_pit_df, 
                          sample_player_id_df, sample_player_activation_df,
                          sample_war_basis_df):
        """Test get_war calculation."""
        live_war, current_war = get_war(
            sample_bat_df,
            sample_pit_df,
            sample_player_id_df,
            sample_player_activation_df,
            sample_war_basis_df
        )
        
        # Check shapes match
        assert live_war.shape == sample_player_id_df.shape
        assert current_war.shape == sample_player_id_df.shape
        
        # Check live WAR for pitcher (10001)
        # Should be WAR from pit_df
        assert live_war.loc['용투1', '준'] == 3.2
        
        # Check live WAR for batter (10002)
        # Should be oWAR from bat_df
        assert live_war.loc['용타', '준'] == 1.5
    
    def test_get_war_with_inactive_player(self, sample_bat_df, sample_pit_df,
                                          sample_player_id_df, sample_player_activation_df,
                                          sample_war_basis_df):
        """Test get_war with inactive player uses basis value."""
        live_war, current_war = get_war(
            sample_bat_df,
            sample_pit_df,
            sample_player_id_df,
            sample_player_activation_df,
            sample_war_basis_df
        )
        
        # Player at position 2R, team 준 is inactive
        # Current WAR should equal basis value
        assert current_war.loc['2R', '준'] == sample_war_basis_df.loc['2R', '준']
