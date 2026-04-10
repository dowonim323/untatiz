from __future__ import annotations

import json
from typing import Any, TYPE_CHECKING

import pandas as pd

from app.core.utils import TEAM_ID_MAP, get_business_date, get_business_year

if TYPE_CHECKING:
    from app.scraper.client import RequestsRateLimiter


class StatizBlockedError(Exception):
    pass


class StatizLoginRequiredError(Exception):
    pass


BAT_COLUMNS = ["ID", "Name", "Team", "POS", "G", "PA", "AVG", "oWAR", "WAR"]
PIT_COLUMNS = ["ID", "Name", "Team", "G", "IP", "ERA", "WHIP", "WAR"]

MLBPARK_API_ROOT = "https://mlbpark.donga.com/mp/api/stats"
MLBPARK_STATS_ROOT = "https://mlbpark.donga.com/mp/stats"

MATCH_STATE_BEFORE = 1
MATCH_STATE_PLAYING = 2
MATCH_STATE_END = 3
MATCH_STATE_CANCEL = 4
MATCH_STATE_RAIN_COLD = 5

POSITION_CODE_MAP = {
    1: "P",
    2: "C",
    3: "1B",
    4: "2B",
    5: "3B",
    6: "SS",
    7: "LF",
    8: "CF",
    9: "RF",
    10: "DH",
    11: "DH",
    12: "DH",
    13: "IF",
    14: "OF",
}


def _wait(rate_limiter: "RequestsRateLimiter | None") -> None:
    if rate_limiter is not None:
        rate_limiter.wait()


def _request_json(
    driver: Any,
    endpoint: str,
    data: dict[str, Any],
    referer: str,
    rate_limiter: "RequestsRateLimiter | None" = None,
) -> dict[str, Any]:
    if driver is None or not hasattr(driver, "post_json"):
        raise ValueError("MLBPARK driver is not ready")

    _wait(rate_limiter)
    try:
        payload = driver.post_json(
            f"{MLBPARK_API_ROOT}{endpoint}",
            data=data,
            referer=referer,
        )
    except Exception:
        if rate_limiter is not None:
            rate_limiter.on_forbidden()
        raise

    if rate_limiter is not None:
        rate_limiter.on_success()
    return payload


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_position(position_code: Any) -> str | None:
    position_int = _coerce_int(position_code)
    if position_int is None:
        return None
    return POSITION_CODE_MAP.get(position_int)


def _get_team_name(team_name: Any, team_code: Any) -> str:
    if team_name is not None and str(team_name).strip():
        return str(team_name).strip()

    code_int = _coerce_int(team_code)
    if code_int is None:
        return ""
    return TEAM_ID_MAP.get(str(code_int), str(code_int))


def _get_season_stats(
    driver: Any,
    rate_limiter: "RequestsRateLimiter | None",
    year: int,
) -> dict[str, Any]:
    cache_key = str(year)
    cache = getattr(driver, "_season_stats_cache", None)
    if isinstance(cache, dict) and cache_key in cache:
        return cache[cache_key]

    payload = _request_json(
        driver,
        "/player/seasonStats.php",
        {"year": str(year)},
        referer=f"{MLBPARK_STATS_ROOT}/batter.php?year={year}",
        rate_limiter=rate_limiter,
    )

    if isinstance(cache, dict):
        cache[cache_key] = payload
    return payload


def _get_player_position(
    driver: Any,
    player_id: str,
    rate_limiter: "RequestsRateLimiter | None",
) -> str | None:
    cache = getattr(driver, "_player_info_cache", None)
    if isinstance(cache, dict) and player_id in cache:
        cached = cache[player_id]
        if isinstance(cached, dict):
            cached_position = cached.get("position")
            if cached_position is None or isinstance(cached_position, str):
                return cached_position

    payload = _request_json(
        driver,
        "/player/info.php",
        {"p_no": player_id},
        referer=f"{MLBPARK_STATS_ROOT}/playerDetail.php?pNo={player_id}",
        rate_limiter=rate_limiter,
    )
    player = payload.get("player", {}) if isinstance(payload, dict) else {}
    position = _normalize_position(player.get("position"))

    if isinstance(cache, dict):
        cache[player_id] = {"position": position}
    return position


def load_statiz_bat(
    driver: Any,
    rate_limiter: Any = None,
    year: int | None = None,
) -> pd.DataFrame:
    year = year or get_business_year()
    payload = _get_season_stats(driver, rate_limiter, year)
    result_list = payload.get("resultList", {}) if isinstance(payload, dict) else {}
    records = result_list.get("playerSeasonBatterRecordList", []) if isinstance(result_list, dict) else []

    rows: list[dict[str, Any]] = []
    for record in records:
        player_id = str(record.get("p_no", "")).strip()
        player_name = str(record.get("p_name", "")).strip()
        if not player_id or not player_name:
            continue

        rows.append(
            {
                "ID": player_id,
                "Name": player_name,
                "Team": _get_team_name(record.get("t_name"), record.get("t_code")),
                "POS": _get_player_position(driver, player_id, rate_limiter),
                "G": _coerce_int(record.get("game_count")),
                "PA": _coerce_int(record.get("batting_pa")),
                "AVG": _coerce_float(record.get("batting_avg")),
                "oWAR": _coerce_float(record.get("batting_waroff")),
                "WAR": _coerce_float(record.get("batting_war")),
            }
        )

    if not rows:
        return pd.DataFrame(columns=pd.Index(BAT_COLUMNS))

    bat = pd.DataFrame(rows)
    bat = bat.dropna(subset=["ID", "Name"])
    bat = bat.set_index("ID", drop=False)
    bat = bat.sort_values(by="oWAR", ascending=False, na_position="last")
    return bat


def load_statiz_pit(
    driver: Any,
    rate_limiter: Any = None,
    year: int | None = None,
) -> pd.DataFrame:
    year = year or get_business_year()
    payload = _get_season_stats(driver, rate_limiter, year)
    result_list = payload.get("resultList", {}) if isinstance(payload, dict) else {}
    records = result_list.get("playerSeasonPitcherRecordList", []) if isinstance(result_list, dict) else []

    rows: list[dict[str, Any]] = []
    for record in records:
        player_id = str(record.get("p_no", "")).strip()
        player_name = str(record.get("p_name", "")).strip()
        if not player_id or not player_name:
            continue

        rows.append(
            {
                "ID": player_id,
                "Name": player_name,
                "Team": _get_team_name(record.get("t_name"), record.get("t_code")),
                "G": _coerce_int(record.get("game_count")),
                "IP": _coerce_float(record.get("pitching_ip")),
                "ERA": _coerce_float(record.get("pitching_era")),
                "WHIP": _coerce_float(record.get("pitching_whip")),
                "WAR": _coerce_float(record.get("pitching_war")),
            }
        )

    if not rows:
        return pd.DataFrame(columns=pd.Index(PIT_COLUMNS))

    pit = pd.DataFrame(rows)
    pit = pit.dropna(subset=["ID", "Name"])
    pit = pit.set_index("ID", drop=False)
    pit = pit.sort_values(by="WAR", ascending=False, na_position="last")
    return pit


def get_updated_teams(
    driver: Any,
    rate_limiter: Any = None,
    year: int | None = None,
) -> set[str]:
    year = year or get_business_year()
    payload = _get_season_stats(driver, rate_limiter, year)
    result_list = payload.get("resultList", {}) if isinstance(payload, dict) else {}
    updated: set[str] = set()
    if isinstance(result_list, dict):
        for key in ("playerSeasonBatterRecordList", "playerSeasonPitcherRecordList"):
            for record in result_list.get(key, []):
                team = _get_team_name(record.get("t_name"), record.get("t_code"))
                if team:
                    updated.add(team)
    return updated


def _extract_schedule_games(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result_list = payload.get("resultList", []) if isinstance(payload, dict) else []
    if isinstance(result_list, list):
        return [game for game in result_list if isinstance(game, dict)]
    if isinstance(result_list, dict):
        games = result_list.get("gameScheduleList", [])
        return [game for game in games if isinstance(game, dict)]
    return []


def _decode_team_info(raw_team_info: Any) -> dict[str, Any]:
    if not raw_team_info or not isinstance(raw_team_info, str):
        return {}
    try:
        decoded = json.loads(raw_team_info)
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _game_team_name(game: dict[str, Any], side: str) -> str:
    info = _decode_team_info(game.get(f"{side}_team_info"))
    if info:
        return _get_team_name(info.get("t_name"), info.get("t_code"))
    return _get_team_name(None, game.get(f"{side}_team"))


def _build_games_dataframe(games: list[dict[str, Any]], date_label: str) -> tuple[pd.DataFrame, int]:
    if not games:
        return pd.DataFrame([["오늘은 경기가 없습니다."]]), 0

    rows: list[list[Any]] = []
    game_number = 0
    updated_number = 0
    started_number = 0

    for game in games:
        away_team = _game_team_name(game, "away")
        home_team = _game_team_name(game, "home")
        if not away_team or not home_team:
            continue

        state = _coerce_int(game.get("s_state")) or 0
        away_score = _coerce_int(game.get("away_score"))
        home_score = _coerce_int(game.get("home_score"))

        if state == MATCH_STATE_CANCEL:
            rows.append([away_team, "우천취소", home_team])
            continue

        if state in {MATCH_STATE_END, MATCH_STATE_RAIN_COLD}:
            score_text = f"{away_score or 0} : {home_score or 0}"
            rows.append([away_team, score_text, home_team])
            game_number += 1
            updated_number += 1
            started_number += 1
            continue

        if state == MATCH_STATE_PLAYING:
            rows.append([away_team, "진행 중", home_team])
            game_number += 1
            started_number += 1
            continue

        rows.append([away_team, "시작 전", home_team])
        game_number += 1

    if not rows:
        return pd.DataFrame([["오늘은 경기가 없습니다."]]), 0

    rows.insert(0, [f"업데이트 : {updated_number}/{game_number}경기", "", ""])
    rows.insert(0, [f"경기 날짜 : {date_label}", "", ""])
    return pd.DataFrame(rows), started_number


def update_games(
    driver: Any,
    rate_limiter: Any = None,
    return_type: str = "df",
    year: int | None = None,
) -> tuple[pd.DataFrame, int] | pd.DataFrame:
    today = get_business_date()
    date_key = today.strftime("%Y%m%d")
    date_label = today.strftime("%m/%d")

    payload = _request_json(
        driver,
        "/game/schedule.php",
        {"dateKey": date_key},
        referer=f"{MLBPARK_STATS_ROOT}/schedule.php?date={date_key}",
        rate_limiter=rate_limiter,
    )
    games = _extract_schedule_games(payload)
    df, started_number = _build_games_dataframe(games, date_label)
    if return_type == "started":
        return df, started_number
    return df


__all__ = [
    "load_statiz_bat",
    "load_statiz_pit",
    "get_updated_teams",
    "update_games",
    "BAT_COLUMNS",
    "PIT_COLUMNS",
    "StatizBlockedError",
    "StatizLoginRequiredError",
    "_build_games_dataframe",
    "_normalize_position",
]
