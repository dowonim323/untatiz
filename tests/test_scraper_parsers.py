from __future__ import annotations

import pandas as pd

from app.scraper.parsers import _build_games_dataframe, _normalize_position
from app.services.war_calculator import get_position_group


def test_normalize_position_maps_mlbpark_codes_to_existing_position_strings():
    assert _normalize_position(1) == "P"
    assert _normalize_position(6) == "SS"
    assert _normalize_position(14) == "OF"
    assert _normalize_position(None) is None


def test_position_group_accepts_generic_if_of_codes():
    assert get_position_group("IF") == "IF"
    assert get_position_group("OF") == "OF"


def test_build_games_dataframe_matches_legacy_schedule_shape_and_counts():
    games = [
        {
            "s_state": 3,
            "away_team": 5002,
            "home_team": 2002,
            "away_team_info": '{"t_code": 5002, "t_name": "LG"}',
            "home_team_info": '{"t_code": 2002, "t_name": "KIA"}',
            "away_score": 2,
            "home_score": 5,
        },
        {
            "s_state": 2,
            "away_team": 9002,
            "home_team": 6002,
            "away_team_info": '{"t_code": 9002, "t_name": "SSG"}',
            "home_team_info": '{"t_code": 6002, "t_name": "두산"}',
        },
        {
            "s_state": 4,
            "away_team": 7002,
            "home_team": 3001,
            "away_team_info": '{"t_code": 7002, "t_name": "한화"}',
            "home_team_info": '{"t_code": 3001, "t_name": "롯데"}',
        },
    ]

    games_df, started = _build_games_dataframe(games, "04/10")

    assert started == 2
    assert isinstance(games_df, pd.DataFrame)
    assert games_df.values.tolist() == [
        ["경기 날짜 : 04/10", "", ""],
        ["업데이트 : 1/2경기", "", ""],
        ["LG", "2 : 5", "KIA"],
        ["SSG", "진행 중", "두산"],
        ["한화", "우천취소", "롯데"],
    ]
