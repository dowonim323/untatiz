"""Statiz HTML parsers - extract data from statiz.co.kr pages."""

from __future__ import annotations

import time
from typing import Any, Set, Tuple, TYPE_CHECKING

import pandas as pd
from bs4 import BeautifulSoup

from app.core.utils import get_business_year, get_date, get_team_from_svg

if TYPE_CHECKING:
    from statiz_utils import RateLimiter


class StatizBlockedError(Exception):
    """Statiz에서 IP 차단 또는 접근 거부 시 발생하는 예외."""
    pass


class StatizLoginRequiredError(Exception):
    """Statiz 로그인이 필요할 때 발생하는 예외."""
    pass


def _check_page_errors(page_source: str, url: str) -> None:
    """페이지 소스에서 403/로그인 필요 에러를 감지하고 예외 발생.
    
    Args:
        page_source: 페이지 HTML 소스
        url: 요청한 URL
        
    Raises:
        StatizBlockedError: 403 Forbidden 감지 시
        StatizLoginRequiredError: 로그인 필요 메시지 감지 시
    """
    # 403 Forbidden 감지 (데이터센터 IP 차단)
    if "403 Forbidden" in page_source:
        raise StatizBlockedError(
            f"403 Forbidden - Statiz에서 IP가 차단되었습니다. "
            f"Residential 프록시를 사용하세요. URL: {url[:80]}"
        )
    
    # 로그인 필요 감지
    if "로그인 후 이용 가능" in page_source or "location.href='/member/?m=login" in page_source:
        raise StatizLoginRequiredError(
            f"로그인이 필요합니다. 세션이 만료되었거나 로그인에 실패했습니다. URL: {url[:80]}"
        )


# Batting stats columns (statiz.co.kr format)
BAT_COLUMNS = [
    "Rank", "Name", "ID", "Team", "POS", "WAR", "G", "oWAR", "dWAR", 
    "PA", "ePA", "AB", "R", "H", "2B", "3B", "HR", "TB", 
    "RBI", "SB", "CS", "BB", "HP", "IB", "SO", "GDP", "SH", "SF", 
    "AVG", "OBP", "SLG", "OPS", "R/ePA", "wRC+", "WAR"
]

# Pitching stats columns (statiz.co.kr format)
PIT_COLUMNS = [
    "Rank", "Name", "ID", "Team", "POS", "WAR", "G", "GS", "GR", 
    "GF", "CG", "SHO", "W", "L", "S", "HD", "IP", "ER", "R", 
    "rRA", "TBF", "H", "2B", "3B", "HR", "BB", "HP", "IB", "SO", 
    "ROE", "BK", "WP", "ERA", "RA9", "rRA9", "rRA9pf", "FIP", "WHIP", "WAR"
]


def load_statiz_bat(
    driver: Any,
    rate_limiter: "RateLimiter | None" = None,
    year: int | None = None
) -> pd.DataFrame:
    """Load batter statistics from statiz.co.kr.
    
    Args:
        driver: Selenium WebDriver (must be logged in)
        rate_limiter: Optional rate limiter
        year: Season year (default 2025)
        
    Returns:
        pd.DataFrame: Batter statistics indexed by player ID
    """
    year = year or get_business_year()
    if rate_limiter:
        rate_limiter.wait()
    
    url = (
        f"https://statiz.co.kr/stats/?m=main&m2=batting&m3=default"
        f"&so=WAR&ob=DESC&year={year}"
        f"&sy=&ey=&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha="
        f"&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba="
        f"&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls="
        f"&sf1=&sk1=&sv1=&sf2=&sk2=&sv2="
    )
    
    driver.get(url)
    time.sleep(2)
    
    page_source = driver.page_source
    _check_page_errors(page_source, url)
    
    soup = BeautifulSoup(page_source, 'html.parser')
    tables = soup.find_all("table")
    if not tables:
        return pd.DataFrame(columns=BAT_COLUMNS)
    
    bat_chart = tables[0]
    
    bat = pd.DataFrame(columns=BAT_COLUMNS)
    rows = bat_chart.find_all("tr")
    
    for i in range(2, len(rows)):
        tr = rows[i]
        if tr.find("th") is not None:
            continue
        
        row = {}
        column_idx = 0
        tds = tr.find_all("td")
        
        for j in range(32):
            if j >= len(tds):
                break
                
            td_text = tds[j].text
            
            if j == 1:  # Name column (also extract ID from link)
                a_tag = tds[j].find('a')
                player_id = a_tag['href'].split('p_no=')[-1] if a_tag else ""
                row[BAT_COLUMNS[column_idx]] = td_text
                column_idx += 1
                row[BAT_COLUMNS[column_idx]] = player_id
                column_idx += 1
            elif j == 2:  # Team/Position column
                img = tds[j].find('img')
                team = get_team_from_svg(img['src'], year) if img else ""
                row[BAT_COLUMNS[column_idx]] = team
                column_idx += 1
                spans = tds[j].find_all('span')
                pos = spans[-1].text if spans else ""
                row[BAT_COLUMNS[column_idx]] = pos
                column_idx += 1
            else:
                row[BAT_COLUMNS[column_idx]] = td_text
                column_idx += 1
        
        row_df = pd.DataFrame([row], columns=BAT_COLUMNS)
        bat = pd.concat([bat, row_df], ignore_index=True)
    
    # Remove duplicate columns
    bat = bat.loc[:, ~bat.columns.duplicated()]
    bat = bat.dropna(subset=['Name', 'ID'])
    bat = bat[(bat['Name'].astype(str).str.strip() != '') & (bat['ID'].astype(str).str.strip() != '')]
    
    # Convert oWAR to float
    bat['oWAR'] = bat['oWAR'].replace('', 0).astype(float)
    
    # Set index and sort
    bat = bat.set_index(keys='ID')
    bat = bat.sort_values(by='oWAR', ascending=False)
    bat['Rank'] = range(1, len(bat) + 1)
    
    return bat


def load_statiz_pit(
    driver: Any,
    rate_limiter: "RateLimiter | None" = None,
    year: int | None = None
) -> pd.DataFrame:
    """Load pitcher statistics from statiz.co.kr.
    
    Args:
        driver: Selenium WebDriver (must be logged in)
        rate_limiter: Optional rate limiter
        year: Season year (default 2025)
        
    Returns:
        pd.DataFrame: Pitcher statistics indexed by player ID
    """
    year = year or get_business_year()
    if rate_limiter:
        rate_limiter.wait()
    
    url = (
        f"https://statiz.co.kr/stats/?m=main&m2=pitching&m3=default"
        f"&so=WAR&ob=DESC&year={year}"
        f"&sy=&ey=&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha="
        f"&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba="
        f"&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls="
        f"&sf1=&sk1=&sv1=&sf2=&sk2=&sv2="
    )
    
    driver.get(url)
    time.sleep(2)
    
    page_source = driver.page_source
    _check_page_errors(page_source, url)
    
    soup = BeautifulSoup(page_source, 'html.parser')
    tables = soup.find_all("table")
    if not tables:
        return pd.DataFrame(columns=PIT_COLUMNS)
    
    pit_chart = tables[0]
    
    pit = pd.DataFrame(columns=PIT_COLUMNS)
    rows = pit_chart.find_all("tr")
    
    for i in range(2, len(rows)):
        tr = rows[i]
        if tr.find("th") is not None:
            continue
        
        row = {}
        column_idx = 0
        tds = tr.find_all("td")
        
        for j in range(36):
            if j >= len(tds):
                break
                
            td_text = tds[j].text
            
            if j == 1:  # Name column (also extract ID from link)
                a_tag = tds[j].find('a')
                player_id = a_tag['href'].split('p_no=')[-1] if a_tag else ""
                row[PIT_COLUMNS[column_idx]] = td_text
                column_idx += 1
                row[PIT_COLUMNS[column_idx]] = player_id
                column_idx += 1
            elif j == 2:  # Team/Position column
                img = tds[j].find('img')
                team = get_team_from_svg(img['src'], year) if img else ""
                row[PIT_COLUMNS[column_idx]] = team
                column_idx += 1
                spans = tds[j].find_all('span')
                pos = spans[-1].text if spans else ""
                row[PIT_COLUMNS[column_idx]] = pos
                column_idx += 1
            else:
                row[PIT_COLUMNS[column_idx]] = td_text
                column_idx += 1
        
        row_df = pd.DataFrame([row], columns=PIT_COLUMNS)
        pit = pd.concat([pit, row_df], ignore_index=True)
    
    # Remove duplicate columns
    pit = pit.loc[:, ~pit.columns.duplicated()]
    pit = pit.dropna(subset=['Name', 'ID'])
    pit = pit[(pit['Name'].astype(str).str.strip() != '') & (pit['ID'].astype(str).str.strip() != '')]
    
    # Convert WAR to float
    pit['WAR'] = pit['WAR'].replace('', 0).astype(float)
    
    # Set index and sort
    pit = pit.set_index(keys='ID')
    pit = pit.sort_values(by='WAR', ascending=False)
    pit['Rank'] = range(1, len(pit) + 1)
    
    return pit


def get_updated_teams(
    driver: Any,
    rate_limiter: "RateLimiter | None" = None,
    year: int | None = None
) -> Set[str]:
    """Get set of teams that have updated stats for today.
    
    Checks daily pitching situation stats to determine which teams
    have had their data updated.
    
    Args:
        driver: Selenium WebDriver (must be logged in)
        rate_limiter: Optional rate limiter
        year: Season year (default 2025)
        
    Returns:
        Set[str]: Set of team names with updated stats
    """
    year = year or get_business_year()
    today = get_date()
    today_month = int(today.split('/')[0])
    today_day = int(today.split('/')[1])
    
    if rate_limiter:
        rate_limiter.wait()
    
    date_str = f"{str(today_month).zfill(2)}-{str(today_day).zfill(2)}"
    url = (
        f"https://statiz.co.kr/stats/?m=main&m2=pitching&m3=situation1"
        f"&so=ERA&ob=ASC&year={year}"
        f"&sy=&ey=&te=&po=&lt=10100&reg=A&pe=I"
        f"&ds={date_str}&de={date_str}"
        f"&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba="
        f"&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls="
        f"&sf1=&sk1=&sv1=&sf2=&sk2=&sv2="
    )
    
    driver.get(url)
    time.sleep(2)
    
    page_source = driver.page_source
    _check_page_errors(page_source, url)
    
    soup = BeautifulSoup(page_source, 'html.parser')
    tables = soup.find_all("table")
    if not tables:
        return set()
    
    pit_chart = tables[0]
    
    updated = set()
    rows = pit_chart.find_all("tr")
    
    for i in range(2, len(rows)):
        tr = rows[i]
        if tr.find("th") is not None:
            continue
        tds = tr.find_all("td")
        if len(tds) > 2:
            img = tds[2].find('img')
            if img:
                team = get_team_from_svg(img['src'], year)
                if team:
                    updated.add(team)
    
    return updated


def update_games(
    driver: Any,
    rate_limiter: "RateLimiter | None" = None,
    return_type: str = "df",
    year: int | None = None
) -> Tuple[pd.DataFrame, int] | pd.DataFrame:
    """Get today's game schedule and update status.
    
    Scrapes the schedule page for today's games and checks which
    have been updated in the stats.
    
    Args:
        driver: Selenium WebDriver (must be logged in)
        rate_limiter: Optional rate limiter
        return_type: "df" for DataFrame only, "started" for tuple with started count
        year: Season year (default 2025)
        
    Returns:
        If return_type == "df": pd.DataFrame with game info
        If return_type == "started": Tuple[pd.DataFrame, int] (games df, started game count)
    """
    year = year or get_business_year()
    today = get_date()
    today_month = int(today.split('/')[0])
    today_day = int(today.split('/')[1])
    
    # Get schedule
    if rate_limiter:
        rate_limiter.wait()
    
    url = f"https://statiz.co.kr/schedule/?year={year}&month={today_month}"
    driver.get(url)
    time.sleep(2)
    
    page_source = driver.page_source
    _check_page_errors(page_source, url)
    
    soup = BeautifulSoup(page_source, 'html.parser')
    tables = soup.find_all("table")
    if not tables:
        games = [["오늘은 경기가 없습니다."]]
        df = pd.DataFrame(games)
        return df if return_type == "df" else (df, 0)
    
    calendar = tables[0]
    rows = calendar.find_all("tr")
    
    # Find today's cell
    temp = None
    for i in range(1, len(rows)):
        tr = rows[i]
        if tr.find("th") is not None:
            continue
        for j in range(7):
            tds = tr.find_all("td")
            if j >= len(tds):
                continue
            td = tds[j]
            day_span = td.find('span', class_='day')
            if day_span is None:
                continue
            day = day_span.text
            if int(day) == today_day:
                temp = td
                break
        if temp:
            break
    
    if temp is None:
        games = [["오늘은 경기가 없습니다."]]
        df = pd.DataFrame(games)
        return df if return_type == "df" else (df, 0)
    
    # Parse games from today's cell
    games = []
    for li in temp.find_all('li'):
        link = li.find('a', href=True)
        href = link['href'] if link and link.has_attr('href') else ''
        game_info = []
        for span in li.find_all('span'):
            game_info.append(span.text)
        if game_info:
            games.append((game_info, href))
    
    game_number = 0
    updated_number = 0
    started_number = 0
    
    if not games:
        games = [["오늘은 경기가 없습니다."]]
    else:
        for i, game_entry in enumerate(games):
            game, href = game_entry
            if len(game) == 4:
                is_summary = 'summary' in href
                is_gamelogs = 'gamelogs' in href
                is_preview = 'preview' in href
                if is_summary and not is_gamelogs:
                    games[i] = [game[0], f"{game[1]} : {game[2]}", game[3]]
                    game_number += 1
                    updated_number += 1
                    started_number += 1
                elif is_gamelogs:
                    games[i] = [game[0], "진행 중", game[3]]
                    game_number += 1
                    started_number += 1
                elif is_preview:
                    games[i] = [game[0], "시작 전", game[3]]
                    game_number += 1
                else:
                    games[i] = [game[0], "업데이트 전", game[3]]
                    game_number += 1
                    started_number += 1
            elif len(game) == 3 and game[1] == "우천취소":
                games[i] = [game[0], "우천취소", game[2]]
            elif len(game) == 3:
                is_gamelogs = 'gamelogs' in href
                is_preview = 'preview' in href
                if is_gamelogs:
                    games[i] = [game[0], "진행 중", game[2]]
                    started_number += 1
                elif is_preview:
                    games[i] = [game[0], "시작 전", game[2]]
                else:
                    games[i] = [game[0], "업데이트 전", game[2]]
                    started_number += 1
                game_number += 1
        
        games.insert(0, [f"업데이트 : {updated_number}/{game_number}경기", "", ""])
    
    games.insert(0, [f"경기 날짜 : {today}", "", ""])
    
    df = pd.DataFrame(games)
    
    if return_type == "df":
        return df
    else:
        return df, started_number


__all__ = [
    "load_statiz_bat",
    "load_statiz_pit",
    "get_updated_teams",
    "update_games",
    "BAT_COLUMNS",
    "PIT_COLUMNS",
    "StatizBlockedError",
    "StatizLoginRequiredError",
]
