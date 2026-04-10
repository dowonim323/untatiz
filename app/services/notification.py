"""Notification service for Untatiz - Discord webhook and news generation."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, cast
from urllib import request as urllib_request

import requests
import pandas as pd
import pytz

from app.config.settings import load_config, AppConfig
from app.core.db import DatabaseManager
from app.services.team_contribution import build_team_contribution_snapshot


def get_date_slash() -> str:
    """Get current date in MM/DD format (KST, after 2PM shows today, before shows yesterday)."""
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    if now.hour < 14:
        date_to_display = now - timedelta(days=1)
    else:
        date_to_display = now
    return date_to_display.strftime('%m/%d')


def _format_mmdd(date_iso: str) -> str:
    return date_iso[5:7] + "/" + date_iso[8:10]


def _get_active_season_id(db: DatabaseManager) -> int:
    row = db.fetch_one("SELECT id FROM seasons WHERE is_active = 1")
    if row and row[0] is not None:
        return int(row[0])
    return 1


def _get_latest_team_snapshot_dates(db: DatabaseManager, season_id: int) -> tuple[str, str | None]:
    today_row = db.fetch_one(
        "SELECT MAX(date) FROM team_war_daily WHERE season_id = ?",
        (season_id,),
    )
    today_iso = today_row[0] if today_row else None
    if not today_iso:
        raise ValueError("No current team_war_daily data available for news generation")

    previous_row = db.fetch_one(
        "SELECT MAX(date) FROM team_war_daily WHERE season_id = ? AND date < ?",
        (season_id, today_iso),
    )
    previous_iso = previous_row[0] if previous_row else None
    return today_iso, previous_iso


def generate_news(config: Optional[AppConfig] = None) -> str:
    if config is None:
        config = load_config()
    
    openai_api_key = config.openai_api_key or "dummy"
    db = DatabaseManager(config.db_path)
    league_name = config.league_name

    season_id = _get_active_season_id(db)
    today_iso, previous_iso = _get_latest_team_snapshot_dates(db, season_id)
    today = _format_mmdd(today_iso)
    previous = _format_mmdd(previous_iso) if previous_iso else today

    with db.connection() as conn:
        league = pd.read_sql_query(
            """
            SELECT current.team_id, current.total_war AS today_war, current.rank AS today_rank,
                   previous.total_war AS previous_war,
                   previous.rank AS previous_rank,
                   current.war_diff AS war_diff
            FROM team_war_daily current
            LEFT JOIN team_war_daily previous
              ON previous.team_id = current.team_id
             AND previous.season_id = current.season_id
             AND previous.date = ?
            WHERE current.season_id = ?
              AND current.date = ?
              AND current.team_id != '퐈'
            ORDER BY current.total_war DESC
            """,
            conn,
            params=[previous_iso, season_id, today_iso],
        )

    league = league.rename(columns={'team_id': '팀'})
    league[f'{previous} 순위'] = league['previous_rank'].fillna(
        league['previous_war'].rank(method='min', ascending=False, na_option='bottom')
    ).astype(int)
    league[f'{today} 순위'] = league['today_rank'].fillna(league['today_war'].rank(method='min', ascending=False)).astype(int)
    league['변화량 순위'] = league['war_diff'].rank(method='min', ascending=False).astype(int)
    league[previous] = league['previous_war'].fillna(0).map(lambda x: f"{float(x):.2f}")
    league[today] = league['today_war'].map(lambda x: f"{float(x):.2f}")
    league['변화량'] = league['war_diff'].fillna(0).map(lambda x: f"+{float(x):.2f}" if float(x) >= 0 else f"{float(x):.2f}")
    league = league[['팀', f'{previous} 순위', f'{today} 순위', '변화량 순위', previous, today, '변화량']]

    team_list = league['팀'].tolist()
    teams_json = {}
    with db.connection() as conn:
        for team_name in team_list:
            current_snapshot = build_team_contribution_snapshot(conn, team_name, season_id, today_iso)
            previous_snapshot = (
                build_team_contribution_snapshot(conn, team_name, season_id, previous_iso)
                if previous_iso
                else []
            )
            previous_by_player = {row['player_id']: row for row in previous_snapshot}

            team_rows = []
            for row in current_snapshot:
                previous_row = previous_by_player.get(row['player_id'], {})
                current_war = float(row['WAR'])
                previous_war = float(previous_row.get('WAR', 0.0) or 0.0)
                team_rows.append(
                    {
                        'index': row['draft_round'],
                        'Name': row['Name'],
                        'today_war': current_war,
                        'previous_war': previous_war,
                        'war_diff': round(current_war - previous_war, 2),
                    }
                )

            team_df = pd.DataFrame(team_rows)
            if team_df.empty:
                teams_json[team_name] = []
                continue

            team_df = team_df.sort_values(['today_war', 'Name'], ascending=[False, True], kind='mergesort').head(12)
            team_df[f'{previous} 순위'] = team_df['previous_war'].rank(method='min', ascending=False, na_option='bottom').fillna(0).astype(int)
            team_df[f'{today} 순위'] = team_df['today_war'].rank(method='min', ascending=False, na_option='bottom').fillna(0).astype(int)
            team_df['변화량 순위'] = team_df['war_diff'].rank(method='min', ascending=False, na_option='bottom').fillna(0).astype(int)
            team_df[previous] = team_df['previous_war'].fillna(0).map(lambda x: f"{float(x):.2f}")
            team_df[today] = team_df['today_war'].fillna(0).map(lambda x: f"{float(x):.2f}")
            team_df['변화량'] = team_df['war_diff'].fillna(0).map(lambda x: f"+{float(x):.2f}" if float(x) >= 0 else f"{float(x):.2f}")
            teams_json[team_name] = cast(
                list[dict[str, Any]],
                json.loads(
                    cast(
                        str,
                        team_df[
                            ['index', 'Name', f'{previous} 순위', f'{today} 순위', '변화량 순위', previous, today, '변화량']
                        ].to_json(orient='records', force_ascii=False),
                    )
                ),
            )

    league_json = cast(
        list[dict[str, Any]],
        json.loads(cast(str, league.to_json(orient='records', force_ascii=False))),
    )
    
    data = {
        "league_data": league_json,
        "teams_data": teams_json,
    }
    
    # Create prompt
    prompt = f"""
### Instructions ###
1. 역할
- 당신은 {league_name}의 일일 뉴스 기사를 작성하는 스포츠 기자입니다.
- 오늘({today})과 직전 스냅샷({previous})의 데이터를 비교해, 단순한 수치 나열이 아니라 오늘 리그 판도가 어떻게 달라졌는지 설명하는 기사를 작성하세요.
- 기사 톤은 지나치게 딱딱한 신문기사보다 읽기 쉬운 인터넷 스포츠 기사에 가깝게 유지하되, 내용은 반드시 데이터에 근거해야 합니다.
- 독자가 순위표의 움직임을 재미있게 따라갈 수 있도록, 흐름과 대비가 잘 드러나게 작성하세요.
- WAR 변화는 그 자체를 나열하는 대상이 아니라, 리그와 팀의 흐름을 설명하는 근거로 사용하세요.

2. 출력 형식
- 반드시 JSON 객체 하나만 반환하세요.
- 필드는 아래 두 개만 포함합니다.
  - `title`: 문자열, 형식은 `{today} {league_name}: [짧은 제목]`
  - `body`: 문자열, 여러 문장을 줄바꿈으로 이어 붙인 기사 본문
- `title`, `body` 외 다른 키를 추가하지 마세요.
- JSON 바깥의 설명, 마크다운, 코드블록, 서문, 후문은 절대 출력하지 마세요.

3. 기사 작성 원칙
- 기사의 최우선 목적은 오늘 리그 전체 흐름과 팀별 의미 있는 변화를 설명하는 것입니다.
- 먼저 리그 전체에서 어떤 상승·하락 흐름이 나타났는지 요약하고, 그다음 그 흐름을 만든 핵심 팀들을 설명하세요.
- 각 팀 설명에서는 먼저 그 팀 변화의 의미를 말하고, 이후 핵심 선수의 WAR 변화로 근거를 제시하세요.
- 선수별 WAR 변화를 독립적으로 길게 나열하지 마세요.
- 누가 얼마나 변했는가보다 그 변화가 리그와 팀 판도에 어떤 의미를 갖는가를 우선해서 서술하세요.

4. 기사 구성 순서
- 1문단: 오늘 리그 전체 흐름 요약
- 2~5문단: 가장 중요한 팀 4개를 중심으로 설명
- 마지막 문단: 다음 경기 관전 포인트
- 각 팀 문단에는 팀명, 직전 스냅샷 순위와 오늘 순위, 변화량의 의미, 핵심 선수 1~2명의 WAR 변화를 반드시 포함하세요.

5. 팀과 선수 선택 기준
- 팀은 아래 우선순위로 정확히 4개 선택하세요.
  1) 순위 변동이 있는 팀
  2) WAR 변화량 절댓값이 큰 팀
  3) 동률이면 오늘 순위가 더 높은 팀
- 선수는 각 팀당 1~2명 선택하세요.
- 선수는 아래 우선순위로 고르세요.
  1) WAR 변화량 절댓값이 큰 선수
  2) 동률이면 오늘 순위가 더 높은 선수
  3) 이후 이름 기준
- 팀명은 항상 `팀 [이름]` 형식으로 표기하세요.

6. 데이터 해석 원칙
- 아래 제공된 데이터에 없는 사실은 절대 추가하지 마세요.
- 경기 결과, 부상, 감독 코멘트, 실제 경기 장면, 향후 일정 확정 정보 등은 추정해서 쓰지 마세요.
- 모든 팀명, 선수명, 순위, WAR, 변화량은 반드시 Data 섹션에서 직접 확인 가능한 값만 사용하세요.
- WAR 변화가 큰 선수는 해당 팀 흐름을 설명하는 핵심 요인으로 해석할 수 있지만, 구체적인 경기 내용을 지어내지는 마세요.
- 수치 표현은 과장하지 마세요.

7. index 해석 가이드
- `index`는 드래프트 라운드 또는 용병 구분을 나타냅니다.
- "1라운드", "2라운드" 등은 신인 지명 순서를 뜻합니다.
- "용타", "용투1", "용투2"는 외국인 타자·투수를 의미합니다.
- 용병 또는 상위 라운드 지명 선수는 기대치가 높은 자원이라는 맥락에서 의미를 설명할 수 있습니다.
- 단, 외부 평판이나 실제 경기 내용을 단정적으로 덧붙이지 마세요.

8. 문체 원칙
- 문체는 격식을 과하게 차린 신문기사보다, 읽기 쉬운 인터넷 스포츠 기사에 가깝게 작성하세요.
- 문장은 자연스럽고 생동감 있게 쓰되, 기사 형식은 유지하세요.
- 가벼운 재치와 표현의 센스는 적극적으로 허용합니다.
- 재치는 문장 곳곳에 약하게 스며들 수 있지만, 기사 전체의 중심은 여전히 데이터 기반 분석에 두세요.
- 다만 표현이 사실을 과장하거나 수치보다 인상을 앞세우면 안 됩니다.
- 숫자를 건조하게 나열하지 말고, 변화의 의미가 자연스럽게 읽히도록 서술하세요.

### Data Reference Guide ###
- 컬럼 이름에 "순위"가 포함된 값은 순위입니다.
- 그 외 숫자 값은 WAR입니다.
- 팀 데이터: '팀', '{previous} 순위', '{today} 순위', '변화량 순위', '{previous}', '{today}', '변화량'
- 선수 데이터: 'index', 'Name', '{previous} 순위', '{today} 순위', '변화량 순위', '{previous}', '{today}', '변화량'

### Data ###

팀 데이터:
{json.dumps(data['league_data'], ensure_ascii=False, indent=2)}

선수 데이터:
{json.dumps(data['teams_data'], ensure_ascii=False, indent=2)}

과도한 수사, 반복 표현, 단순 수치 나열을 피하세요.
핵심 변화가 리그와 팀 흐름에 어떤 의미를 만드는지 중심으로, 읽기 쉽고 말맛이 살아 있는 인터넷 스포츠 기사 문체로 작성하세요.
"""

    completion_base_url = (config.openai_base_url or "https://api.openai.com/v1").rstrip("/")
    req = urllib_request.Request(
        f"{completion_base_url}/chat/completions",
        data=json.dumps({
            "model": config.openai_model,
            "messages": [
                {"role": "system", "content": "당신은 데이터에 근거해 야구 뉴스를 작성하는 전문 스포츠 기자입니다. 제공된 데이터만 사용해 정확하고 읽기 쉬운 한국어 기사형 JSON 객체를 생성합니다. 문체는 지나치게 딱딱한 신문기사보다, 흐름이 잘 읽히고 말맛이 살아 있는 인터넷 스포츠 기사에 가깝게 유지합니다."},
                {"role": "user", "content": prompt},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "news_article",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "body": {"type": "string"}
                        },
                        "required": ["title", "body"],
                        "additionalProperties": False
                    }
                }
            },
            "max_completion_tokens": 1500,
            "temperature": 0.8,
        }).encode('utf-8'),
        headers={
            "Authorization": f"Bearer {openai_api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib_request.urlopen(req, timeout=120) as resp:
        response_payload = json.loads(resp.read().decode('utf-8'))
    news_content = response_payload["choices"][0]["message"]["content"]
    structured = json.loads(news_content)
    news_title = structured["title"].strip()
    news_body = structured["body"].strip()
    
    # Extract date from title
    parts = news_title.split(f' {league_name}: ')
    if len(parts) >= 2:
        date_str = parts[0]
        news_title = parts[1]
    else:
        date_str = today
    
    # Save news to JSON file
    _save_news_to_file(config, date_str, news_title, news_body)
    
    return news_content


def _save_news_to_file(config: AppConfig, date_str: str, title: str, body: str) -> None:
    """Save news to JSON file."""
    try:
        news_dir = config.news_dir
        news_dir.mkdir(parents=True, exist_ok=True)
        
        news_file = news_dir / "news.json"
        all_news = {}

        if news_file.exists():
            with open(news_file, 'r', encoding='utf-8') as f:
                try:
                    all_news = json.load(f)
                except json.JSONDecodeError:
                    all_news = {}

        all_news[date_str] = {
            "title": title,
            "content": body
        }

        with open(news_file, 'w', encoding='utf-8') as f:
            json.dump(all_news, ensure_ascii=False, indent=2, fp=f)

    except Exception as e:
        print(f"뉴스 저장 중 오류 발생: {e}")


def send_discord_webhook(
    webhook_url: str,
    embeds: list[Dict[str, Any]],
    content: Optional[str] = None,
) -> bool:
    """Send message to Discord via webhook.
    
    Args:
        webhook_url: Discord webhook URL
        embeds: List of embed objects
        content: Optional text content (for @everyone mention, etc.)
        
    Returns:
        bool: True if successful
    """
    if not webhook_url:
        print("[Webhook] No webhook URL configured")
        return False
    
    payload: Dict[str, Any] = {"embeds": embeds}
    if content:
        payload["content"] = content
    
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
            timeout=10
        )
        
        if response.status_code in [200, 204]:
            print("[Webhook] Message sent successfully")
            return True
        else:
            print(f"[Webhook] Failed with status {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"[Webhook] Error: {str(e)}")
        return False


def notify_update_complete(
    webhook_url: str,
    config: Optional[AppConfig] = None,
    generate_news_article: bool = True,
) -> bool:
    """Send notification when update is complete.
    
    Called by scraper after successful update. Sends:
    1. Update complete embed with GOAT/BOAT
    2. Generated news article (optional)
    
    Args:
        webhook_url: Discord webhook URL
        config: Optional AppConfig instance
        generate_news_article: Whether to generate and send news
        
    Returns:
        bool: True if all notifications sent successfully
    """
    if config is None:
        config = load_config()
    
    db = DatabaseManager(config.db_path)
    date = get_date_slash()
    
    # Check if update is complete using new schema
    try:
        with db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT war_status, total_games, target_date FROM scraper_status WHERE id = 1")
            row = cursor.fetchone()
            
            if not row or row[0] != 'completed':
                print("[Notify] Update not complete, skipping notification")
                return False
            
            total = row[1] or 0
            target_date = row[2]
            if target_date:
                date = _format_mmdd(target_date)
    except Exception as e:
        print(f"[Notify] Error checking update status: {e}")
        return False
    
    # Get GOAT/BOAT from new schema
    goat_text = ""
    boat_text = ""
    try:
        with db.connection() as conn:
            cursor = conn.cursor()
            # Get today's GOAT (highest positive war_diff)
            cursor.execute("""
                SELECT p.name, ft.name as team_name, dr.war_diff
                FROM daily_records dr
                JOIN players p ON dr.player_id = p.id
                LEFT JOIN fantasy_teams ft ON dr.team_id = ft.id
                WHERE dr.date = ? AND dr.record_type = 'GOAT'
                ORDER BY dr.war_diff DESC
                LIMIT 1
            """, (target_date,))
            goat_row = cursor.fetchone()
            if goat_row:
                team_name = goat_row[1] or "FA"
                goat_text = f"팀 {team_name}  {goat_row[0]}"
            
            # Get today's BOAT (lowest negative war_diff)
            cursor.execute("""
                SELECT p.name, ft.name as team_name, dr.war_diff
                FROM daily_records dr
                JOIN players p ON dr.player_id = p.id
                LEFT JOIN fantasy_teams ft ON dr.team_id = ft.id
                WHERE dr.date = ? AND dr.record_type = 'BOAT'
                ORDER BY dr.war_diff ASC
                LIMIT 1
            """, (target_date,))
            boat_row = cursor.fetchone()
            if boat_row:
                team_name = boat_row[1] or "FA"
                boat_text = f"팀 {team_name}  {boat_row[0]}"
    except Exception as e:
        print(f"[Notify] Error getting GOAT/BOAT: {e}")
    
    success = True
    
    # Send update complete embed
    embed = {
        "title": f"{date} 결과 업데이트 완료",
        "description": f"총 {total} 경기" if total > 0 else "",
        "color": 0x00FF00,  # Green
        "fields": []
    }
    
    if goat_text:
        embed["fields"].append({"name": "Daily GOAT", "value": goat_text, "inline": True})
    if boat_text:
        embed["fields"].append({"name": "Daily BOAT", "value": boat_text, "inline": False})
    embed["fields"].append({"name": "UNTATIZ", "value": "[바로가기](https://untatiz.dowonim.com/)"})
    
    if not send_discord_webhook(webhook_url, [embed], content="@everyone"):
        success = False
    
    # Generate and send news
    if generate_news_article:
        try:
            news_content = generate_news(config)

            news_title = ""
            news_body = ""
            try:
                structured_news = json.loads(news_content)
                news_title = structured_news.get("title", "")
                news_body = structured_news.get("body", "")
            except json.JSONDecodeError:
                news_lines = news_content.split('\n')
                news_title = news_lines[0]
                news_body = '\n'.join(news_lines[2:]) if len(news_lines) > 1 else ""
            
            news_embed = {
                "title": f"{date} 코민코 리그 뉴스",
                "description": f"[{news_title}](https://untatiz.dowonim.com/category/news)",
                "color": 0x0000FF,  # Blue
            }
            
            if not send_discord_webhook(webhook_url, [news_embed]):
                success = False
                
        except Exception as e:
            print(f"[Notify] Error generating news: {e}")
            success = False
    
    return success


def check_and_notify(webhook_url: str, config: Optional[AppConfig] = None) -> bool:
    """Check if update is complete and send notification if needed.
    
    This is a convenience function that can be called periodically
    to check for updates and send notifications.
    
    Args:
        webhook_url: Discord webhook URL
        config: Optional AppConfig instance
        
    Returns:
        bool: True if notification was sent
    """
    if config is None:
        config = load_config()
    
    db = DatabaseManager(config.db_path)
    date = get_date_slash()
    
    # Check update status using new schema
    try:
        with db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT war_status, target_date FROM scraper_status WHERE id = 1")
            row = cursor.fetchone()
            
            if not row:
                return False
            
            war_status = row[0]
            target_date = row[1]
            
            # Check if it's today's update
            if target_date:
                from datetime import datetime
                try:
                    dt = datetime.strptime(target_date, "%Y-%m-%d")
                    db_date = dt.strftime("%m/%d")
                    if db_date != date:
                        return False
                except ValueError:
                    return False
            else:
                return False
            
            # Check if update is complete
            if war_status != 'completed':
                return False
            
            # Check if no games today
            if war_status == 'no_games':
                return False
            
            # Send notification
            return notify_update_complete(webhook_url, config)
        
    except Exception as e:
        print(f"[CheckNotify] Error: {e}")
        return False


__all__ = [
    "generate_news",
    "send_discord_webhook",
    "notify_update_complete",
    "check_and_notify",
    "get_date_slash",
]
