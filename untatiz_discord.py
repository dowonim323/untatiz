import discord
from discord.ext import commands, tasks
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime, time, timezone, timedelta
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import sys
import os
import json
from matplotlib import font_manager, rc
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
import dataframe_image as dfi
import sqlite3
from openai import OpenAI

# 로그 파일 경로 설정
log_directory = "/home/ubuntu/untatiz/log/"
log_filename = "discord.log"
log_path = log_directory + log_filename

# 로그 설정
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    handlers=[logging.FileHandler(log_path),
                              logging.StreamHandler(sys.stdout)])

# stdout과 stderr 리디렉션 설정
class StreamToLogger:
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass

# 표준 출력과 오류를 로그로 리디렉션
sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)

# 디스코드 봇 설정 로드
with open('/home/ubuntu/untatiz/api/discord.json') as json_file:
    discord_config = json.load(json_file)
TOKEN = discord_config["token"]
CHANNEL_ID = discord_config["channel_id"]

# OpenAI API 키 로드
with open('/home/ubuntu/untatiz/api/openai.json') as json_file:
    openai_config = json.load(json_file)
openai_client = OpenAI(api_key=openai_config["api_key"])

# 디스코드 클라이언트 설정
intents = discord.Intents.all()
client = discord.Client(intents=intents)
client = commands.Bot(command_prefix='/', intents=intents)

def load_table(table_name):
    """
    Load a table from untatiz_db.db as a pandas DataFrame
    
    Args:
        table_name (str): Name of the table to load from SQLite database
        
    Returns:
        pandas.DataFrame: DataFrame containing the table data
        
    Raises:
        ValueError: If table_name does not exist in the database
    """
    import sqlite3
    
    conn = sqlite3.connect("/home/ubuntu/untatiz/db/untatiz_db.db")
    cursor = conn.cursor()
    
    # Get list of tables in database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [table[0] for table in cursor.fetchall()]
    
    if table_name not in tables:
        conn.close()
        raise ValueError(f"Table '{table_name}' does not exist in database")
        
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df
    
def get_date():
    """현재 날짜를 KST 기준으로 MMDD 형식으로 반환합니다."""
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    date_to_display = now - timedelta(days=1)
    return date_to_display.strftime('%m%d')

def get_date_today():
    """현재 날짜를 KST 기준으로 2PM 이전과 이후를 구분하여 MMDD 형식으로 반환합니다."""
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    if now.hour < 14:
        date_to_display = now - timedelta(days=1)
    else:
        date_to_display = now
    return date_to_display.strftime('%m%d')

def get_date_slash():
    """현재 날짜를 KST 기준으로 2PM 이전과 이후를 구분하여 MM/DD 형식으로 반환합니다."""
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    if now.hour < 14:
        date_to_display = now - timedelta(days=1)
    else:
        date_to_display = now
    return date_to_display.strftime('%m/%d')

def generate_news():
    
    today = get_date_slash()
    yesterday = (datetime.strptime(today, "%m/%d") - timedelta(days=1)).strftime("%m/%d")

    league = load_table("teams")[["팀", yesterday, today]]
    league['변화량'] = league[today] - league[yesterday]

    league_without_fa = league[league["팀"] != "퐈"]
    league_without_fa = league_without_fa.sort_values(by='변화량', ascending=False).reset_index(drop=True)
    league_without_fa.insert(1, '변화량 순위', range(1, len(league_without_fa) + 1))
    league_without_fa = league_without_fa.sort_values(by=today, ascending=False).reset_index(drop=True)
    league_without_fa.insert(1, today + " 순위", range(1, len(league_without_fa) + 1))
    league_without_fa = league_without_fa.sort_values(by=yesterday, ascending=False).reset_index(drop=True)
    league_without_fa.insert(1, yesterday + " 순위", range(1, len(league_without_fa) + 1))
    fa_data = league[league["팀"] == "퐈"]
    fa_data.insert(0, today + " 순위", np.nan)
    fa_data.insert(0, yesterday + " 순위", np.nan)
    league = pd.concat([league_without_fa, fa_data]).reset_index(drop=True)
    league = league.sort_values(by=today + " 순위", ascending=True).reset_index(drop=True)
    league[yesterday] = league[yesterday].apply(lambda x: f"{float(x):.2f}").astype(str)
    league[today] = league[today].apply(lambda x: f"{float(x):.2f}").astype(str)
    league['변화량'] = league['변화량'].apply(lambda x: f"+{float(x):.2f}" if float(x) >= 0 else f"{float(x):.2f}").astype(str)

    team_list = [team for team in league["팀"].tolist() if team != '퐈']

    teams = []

    for team in team_list:
        data = load_table(team)[["index", "Name", yesterday, today]]
        data['변화량'] = data[today] - data[yesterday]
        
        data = data.sort_values(by='변화량', ascending=False).reset_index(drop=True)
        data.insert(2, '변화량 순위', range(1, len(data) + 1))
        data = data.sort_values(by=today, ascending=False).reset_index(drop=True)
        data.insert(2, today + " 순위", range(1, len(data) + 1))
        data = data.sort_values(by=yesterday, ascending=False).reset_index(drop=True)
        data.insert(2, yesterday + " 순위", range(1, len(data) + 1))
        data = data.sort_values(by='변화량 순위', ascending=True).reset_index(drop=True)
        data[yesterday] = data[yesterday].apply(lambda x: f"{float(x):.2f}").astype(str)
        data[today] = data[today].apply(lambda x: f"{float(x):.2f}").astype(str)
        data['변화량'] = data['변화량'].apply(lambda x: f"+{float(x):.2f}" if float(x) >= 0 else f"{float(x):.2f}").astype(str)
        teams.append(data)

    league_json = league.to_dict(orient='records')

    # 팀별 정보 JSON 변환
    teams_json = {}
    for i, team_name in enumerate(team_list):
        teams_json[team_name] = teams[i].to_dict(orient='records')
    # 전체 데이터 구조화
    data = {
        "league_data": league_json,
        "teams_data": teams_json,
    }
    
    # 프롬프트 작성
    prompt = f"""
다음은 지재옥 리그의 팀 순위와 선수별 WAR(Wins Above Replacement) 데이터입니다:

리그 순위 정보:
{json.dumps(data['league_data'], ensure_ascii=False, indent=2)}

각 팀별 선수 WAR 데이터:
{json.dumps(data['teams_data'], ensure_ascii=False, indent=2)}

## 임무
지재옥 리그의 폭발적 관심을 끌 수 있는 자극적이고 화제성 높은 일일 뉴스 기사를 작성해주세요. 독자들의 클릭을 유도하고 공유하고 싶게 만드는 선정적인 기사를 작성하세요.

## 기준 날짜 정보
- **오늘 날짜**: {today} (기사 작성일)
- **어제 날짜**: {yesterday} (비교 기준일)
- 모든 분석은 오늘({today}) 데이터와 어제({yesterday}) 데이터를 비교한 결과입니다.
- 기사는 반드시 {today} 날짜에 발생한 변화에 대한 것임을 명시하세요.

## 데이터 참조 가이드
- **리그 순위 정보**: 'league_data' JSON에는 다음 정보가 포함됩니다:
  * "팀": 팀 이름
  * "{yesterday} 순위": 어제 기준 팀 순위
  * "{today} 순위": 오늘 기준 팀 순위
  * "변화량 순위": 변화량 기준 팀 순위
  * "{yesterday}": 어제 기준 팀 WAR
  * "{today}": 오늘 기준 팀 WAR
  * "변화량": 전날 대비 팀 WAR 변화량

- **선수별 WAR 데이터**: 'teams_data' JSON에는 다음 정보가 포함됩니다:
  * "index": 드래프트 라운드 정보
  * "Name": 선수 이름
  * "{yesterday} 순위": 어제 기준 팀 내 선수 순위
  * "{today} 순위": 오늘 기준 팀 내 선수 순위
  * "변화량 순위": 변화량 기준 팀 내 선수 순위
  * "{yesterday}": 어제 기준 선수 WAR
  * "{today}": 오늘 기준 선수 WAR
  * "변화량": 전날 대비 선수 WAR 변화량

## 데이터 해석 지침
- 모든 선수 데이터는 WAR 기반 수치입니다.
- '팀 퐈'는 FA(자유계약) 선수들의 가상 팀이므로 팀 분석에서 제외하되, 비교 지표로는 사용 가능합니다.
- **순위 변화에 주목하세요**: "{yesterday} 순위"와 "{today} 순위"를 비교하여 순위 상승/하락을 강조하세요.
- **WAR 변화량에 주목하세요**: "변화량" 값을 보고 급격한 상승이나 하락을 강조하세요.
- 각 팀에서 변화량이 가장 높은 선수들과 가장 낮은 선수들을 중심으로 분석하세요.
- 팀 성적 변화와 해당 팀 소속 선수들의 변화량을 연결지어 분석하세요. 팀 성적이 올랐다면 상승에 기여한 선수를 언급하고, 팀 성적이 내려갔다면 하락에 기여한 선수를 언급하세요.
- 선수들의 "index" 값은 해당 선수가 드래프트에서 뽑힌 라운드를 의미합니다:
  * "용타1", "용투1", "용투2"는 용병 슬롯으로, 매우 높은 성적이 기대되는 핵심 선수입니다.
  * "용타1", "용투1", "용투2" 이외에 "1라운드", "2라운드" 등 "n라운드" 선수들은 용병이 아닌 국내 선수입니다.
  * "1라운드", "2라운드", "3라운드" 등 상위 라운드 선수들도 높은 성적이 기대됩니다.
  * 하위 라운드(높은 숫자) 선수들은 상대적으로 기대치가 낮습니다.

## 작성 전략 및 톤
- 극적인 표현과 대비를 강조하세요: 폭락, 급상승, 충격적인 하락, 기적의 반전 등
- 팀과 선수 간 갈등 구도를 암시하세요: 혼자 분전하는 OOO, 팀을 무너뜨린 핵심 선수 등
- 극단적 관점에서 분석하세요: 역대급 몰락, 전설적인 활약, 기적의 반전 등
- 용병과 상위 라운드 선수의 부진은 더욱 극적으로 강조하세요: 용병의 충격적 몰락, 1라운드의 기대를 저버린 처참한 성적 등
- 하위 라운드 선수의 활약은 더욱 놀랍게 표현하세요: 15라운드 깜짝 영웅, 마지막 라운드에서 찾은 보물 등

## 기사 내용 요구사항
1. 충격적인 도입부로 시작: 가장 극적인 변화(순위 변동, 선수 급상승/급하락)를 강조
2. 자극적인 팀 분석 - league_data 참조
   - 순위 변동을 충격적인 몰락, 전격 상승 등으로 표현 (반드시 "{yesterday} 순위"와 "{today} 순위" 비교)
   - 접전 팀들을 사활을 건 대결, 명예를 건 자존심 싸움 등으로 묘사
   - 각 팀에서 가장 큰 변화를 보인 선수를 영웅 또는 빌런으로 극단적 묘사
   - WAR 변화량("변화량")을 기준으로 가장 큰 변화를 보인 팀을 강조
3. 개인 성적 변화를 극적으로 서술 - teams_data 참조, 변화량 순위 기준 상위 3명과 하위 3명 안에서만 기술
   - **변화량이 가장 높은 선수들**: 
     * 원래 성적이 좋았던 선수(높은 {yesterday} 팀내 순위)가 계속 상승: 왕좌를 더 높이 올리는, 독보적 지배력 과시, 절대 강자의 질주 등으로 묘사
     * 원래 성적이 저조했던 선수(낮은 {yesterday} 팀내 순위)의 급상승: 불꽃 튀는 반격, 신의 한 수, 환골탈태, 기적의 부활 등으로 묘사
   - **변화량이 가장 낮은 선수들**: 
     * 원래 성적이 좋았던 선수(높은 {yesterday} 팀내 순위)의 하락: 치명적 추락, 충격적 몰락, 제국의 붕괴, 거인의 추락 등으로 묘사
     * 원래 성적이 저조했던 선수(낮은 {yesterday} 팀내 순위)가 계속 부진: 끝없는 나락, 절망의 수렁, 탈출구 없는 미로 등으로 묘사
   - 드래프트 지위를 반영한 묘사:
     * 용병/상위 라운드 선수 부진: 핵심 용병의 처참한 붕괴, 1라운드의 치명적 실패 등 더 강하게 비판
     * 용병/상위 라운드 선수 활약: 기대에 부응하는 용병의 폭발, 1라운드 가치 입증 등으로 묘사
     * 하위 라운드 선수 활약: 깜짝 발견된 20라운드의 숨은 보석, 최후반 픽의 기적적 반란 등 더 놀랍게 강조

## 작성 스타일 가이드
- 모든 문장의 종결어미는 "~했습니다", "~입니다", "~되었습니다"와 같은 격식체로 통일
- 팀명은 항상 '팀 [이름]' 형식으로 표기하되, 필요시 별명 추가 (예: "불꽃 군단 팀 언")
- 선수 언급 시 극적인 수식어 추가 (예: 절벽에서 회생한 OOO, 추락하는 별 OOO)
- 선수 순위 언급 시 팀내 순위임을 명시 (예: 팀 내 O위를 기록하던 OOO)
- 모든 선수 이름과 수식어는 따옴표로 감싸지 말고 그대로 표기
- 글에서 따옴표를 사용 금지 (팀 이름과 선수명도 따옴표 사용 금지)
- 선수 언급 시 반드시 전날 대비 수치 변화량을 구체적으로 명시 (예: "+0.5", "-0.3")
- 군더더기 없는 짧은 문장으로 긴장감 조성
- 한 문단으로 연결된 줄글 형태로 작성
- 글 전체가 10문장을 초과하지 않도록 작성
- 정해진 분량 내에서 최대한 많은 팀의 소식을 전하도록 작성
- 제목을 포함하여 글 전체에 마크다운 문법을 사용하지 말고 텍스트로만 작성

## 기사 제목 형식
[MM/DD] 지재옥 리그: [가장 자극적이고 호기심을 자극하는 문구]

## 결론 작성
마지막은 다음 경기일에 대한 궁금증을 유발하는 문장으로 마무리하세요.
"""

    # API 호출
    response = openai_client.chat.completions.create(
        model="gpt-4o",  # 또는 사용 가능한 모델
        messages=[
            {"role": "system", "content": "당신은 전문적인 스포츠 기자입니다. 데이터를 바탕으로 정확하고 통찰력 있는 야구 뉴스 기사를 작성합니다."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=1500,
        temperature=0.7
    )
    
    news_content = response.choices[0].message.content

    # 뉴스 콘텐츠에서 제목과 내용 분리
    news_lines = news_content.split('\n')
    news_title = news_lines[0]
    news_body = '\n'.join(news_lines[2:]) if len(news_lines) > 1 else ""
    
    # 날짜 정보 추출 (MM/DD 형식)
    parts = news_title.split(' 지재옥 리그: ')
    if len(parts) >= 2:
        date_str = parts[0]
        news_title = parts[1]
    else:
        date_str = today
    
    # JSON 형식으로 뉴스 데이터 구성
    news_data = {
        "date": date_str,
        "title": news_title,
        "content": news_body
    }
    
    # news 폴더에 news.json 파일로 저장
    try:
        # 폴더 없으면 생성
        os.makedirs('news', exist_ok=True)

        all_news = {}

        if os.path.exists('news/news.json'):
            with open('news/news.json', 'r', encoding='utf-8') as f:
                try:
                    all_news = json.load(f)
                except json.JSONDecodeError:
                    all_news = {}

        all_news[date_str] = {
            "title": news_title,
            "content": news_body
        }

        # JSON 파일로 저장
        with open('news/news.json', 'w', encoding='utf-8') as f:
            json.dump(all_news, ensure_ascii=False, indent=2, fp=f)

    except Exception as e:
        print(f"뉴스 저장 중 오류 발생: {e}")
    
    return news_content

class ShowMoreView(discord.ui.View):
    def __init__(self, full_content):
        super().__init__(timeout=None)
        self.full_content = full_content

    @discord.ui.button(label="뉴스 읽기", style=discord.ButtonStyle.primary)
    async def show_more(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title=None, description=self.full_content, color=discord.Color.blue())
        await interaction.response.send_message(embed=embed, ephemeral=True)

@client.event
async def on_ready():
    """봇이 준비되었을 때 실행되는 함수입니다."""
    global update_status
    update_status = 0
    print(f'logged in as {client.user}')
    check_update.start()

@tasks.loop(minutes=1)
async def check_update():
    """정기적으로 업데이트를 체크합니다."""
    import re
    global update_status
    try:
        channel = client.get_channel(CHANNEL_ID)
        update = load_table("update_info")

        if re.compile(r'\d{2}/\d{2}').search(update.iloc[1,0]).group() == get_date_slash():
            date = re.search(r'\d{2}/\d{2}', update.iloc[1,0]).group(0)
            
            if update_status == 0 and update.iloc[-1,1] == "업데이트 완료":
                update_status = 1
                print("update skipped at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " and paused")
                now = datetime.now()
                target_time = now.replace(hour=14, minute=30, second=0, microsecond=0)
                if target_time <= now:
                    target_time += timedelta(days=1)
                wait_seconds = (target_time - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                print("update rebooted at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                check_update.restart()
                return
            
            if update.iloc[2,0] == "오늘은 경기가 없습니다.":
                update_status = 1
                print("update skipped at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " and paused")
                now = datetime.now()
                target_time = now.replace(hour=14, minute=30, second=0, microsecond=0)
                if target_time <= now:
                    target_time += timedelta(days=1)
                wait_seconds = (target_time - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                print("update rebooted at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                check_update.restart()
                return
            
            elif update.iloc[-1,1] == "업데이트 완료":
                print("update started at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                total = int(re.search(r'(\d+)/(\d+)', update.iloc[2,0]).group(2))
                
                goat = load_table("GOAT")
                boat = load_table("BOAT")
                goat = goat[goat["날짜"] == date].iloc[0]
                boat = boat[boat["날짜"] == date].iloc[0]
                goat = "팀 " + goat["소속팀"] + "  " + goat["이름"]
                boat = "팀 " + boat["소속팀"] + "  " + boat["이름"]
                
                embed = discord.Embed(title=date + " 결과 업데이트 완료", description="총 " + str(total) + " 경기", color=discord.Color.green())
                embed.add_field(name='Daily GOAT', value=goat, inline=True)
                embed.add_field(name='Daily BOAT', value=boat, inline=False)
                embed.add_field(name='UNTATIZ', value = '[바로가기](https://untatiz.dowonim.com/)')
                
                allowed_mentions = discord.AllowedMentions(everyone = True)
                await channel.send(content = "@everyone", allowed_mentions = allowed_mentions)
                await channel.send(embed=embed)
                
                full_message = generate_news()
                news_lines = full_message.split('\n')
                news_title = news_lines[0]
                news_body = '\n'.join(news_lines[2:]) if len(news_lines) > 1 else ""
                
                embed = discord.Embed(title=news_title, description="", color=discord.Color.blue())
                view = ShowMoreView(news_body)
                await channel.send(embed=embed, view=view)
                
                print("update finished at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " and paused")
                now = datetime.now()
                target_time = now.replace(hour=14, minute=30, second=0, microsecond=0)
                if target_time <= now:
                    target_time += timedelta(days=1)
                wait_seconds = (target_time - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                print("update rebooted at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                check_update.restart()
                return

        update_status = 1
        print("update skipped at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

    except Exception as e:
        print(f"An error occurred: {e}")
        await asyncio.sleep(60)

client.run(TOKEN)
