import discord
from discord.ext import commands, tasks
import pandas as pd
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

# 로그 파일 경로 설정
log_directory = "/home/imdw/untatiz/log/"
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
with open('/home/imdw/untatiz/api/discord.json') as json_file:
    discord_config = json.load(json_file)
TOKEN = discord_config["token"]
CHANNEL_ID = discord_config["channel_id"]
DB_PATH = "/home/imdw/untatiz/db/discord_db.xlsx"
IMAGE_PATH = "/home/imdw/untatiz/graph/"
doc_service = gspread.service_account("/home/imdw/untatiz/api/untatiz-75f1c6db233b.json").open_by_url("https://docs.google.com/spreadsheets/d/1dBXiLWcMnTToACuOySMume3xzkruL4iL_zLOYwtSaQY/edit?usp=sharing")

# 디스코드 클라이언트 설정
intents = discord.Intents.all()
client = discord.Client(intents=intents)
client = commands.Bot(command_prefix='/', intents=intents)

# 허용된 팀 이름 목록
allowed_teams = ['언', '앙', '삼', '준', '역', '뚝', '홍', '엉', '코', '옥']

# 구독자 딕셔너리
subscribers = {}

def load_subscribers():
    """구독자 데이터를 로드합니다."""
    global subscribers
    try:
        df = pd.read_excel(DB_PATH)
        for index, row in df.iterrows():
            subscribers[row['user_id']] = row['teams'].split(',')
    except FileNotFoundError:
        subscribers = {}

def save_subscribers():
    """구독자 데이터를 저장합니다."""
    df = pd.DataFrame([(user_id, ','.join(teams)) for user_id, teams in subscribers.items()], columns=['user_id', 'teams'])
    df.to_excel(DB_PATH, index=False)
    
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

def load_gspread(json_path, url):
    """Google 스프레드시트를 로드합니다."""
    import gspread
    gc = gspread.service_account(json_path)
    return gc.open_by_url(url)

def untatiz_graph():
    """언탯티즈 리그 그래프를 생성하고 저장합니다."""
    font_path = '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'
    font = font_manager.FontProperties(fname=font_path).get_name()
    rc('font', family=font)
    plt.rcParams['axes.unicode_minus'] = False
    
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    if now.hour < 14:
        date_to_display = now - timedelta(days=1)
    else:
        date_to_display = now
    today = date_to_display.strftime('%m%d')
    
    save_dir = os.path.join('/home/imdw/untatiz/graph', today)
    os.makedirs(save_dir, exist_ok=True)

    teams = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="teams")
    teams = teams[teams["팀"] != "퐈"]
    teams.set_index('팀', inplace=True)
    teams_transposed = teams.T
    teams_transposed.index = pd.to_datetime(teams_transposed.index, format='%m/%d')
    
    date_count = len(teams_transposed.index)
    plt.figure(figsize=(10 + date_count / 8, 6))

    last_values = teams_transposed.iloc[-1, :].sort_values(ascending=False)

    for team in last_values.index:
        plt.plot(teams_transposed.index, teams_transposed[team], marker='o', label=team)

    plt.title('지재옥 리그')
    plt.xlabel('날짜')
    plt.ylabel('WAR')
    plt.legend(title='팀', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    
    plt.xticks(teams_transposed.index)
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%m/%d'))
    plt.gcf().autofmt_xdate()
    
    plt.savefig(os.path.join(save_dir, "리그_" + today + ".png"), dpi=300, bbox_inches='tight')
    plt.close()
    
    for team in ['언', '앙', '삼', '준', '역', '뚝', '홍', '엉', '코', '옥']:
        data = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name=team)
        data = data.iloc[:, 2:]
        data.set_index('Name', inplace=True)
        data_transposed = data.T
        data_transposed.index = pd.to_datetime(data_transposed.index, format='%m/%d')
        
        date_count = len(data.index)
        plt.figure(figsize=(10 + date_count / 8, 12))
        
        last_values = data_transposed.iloc[-1, :].sort_values(ascending=False)
        
        for player in last_values.index:
            plt.plot(data_transposed.index, data_transposed[player], marker='o', label=player)

        plt.title('팀 ' + team)
        plt.xlabel('날짜')
        plt.ylabel('WAR')
        plt.legend(title='이름', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True)
        
        plt.xticks(data_transposed.index)
        plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%m/%d'))
        plt.gcf().autofmt_xdate()
        
        plt.tight_layout()
        
        plt.savefig(os.path.join(save_dir, "팀 " + team + "_" + today + ".png"), dpi=300, bbox_inches='tight')
        plt.close()

@client.event
async def on_ready():
    """봇이 준비되었을 때 실행되는 함수입니다."""
    print(f'logged in as {client.user}')
    load_subscribers()
    check_update.start()
    channel = client.get_channel(CHANNEL_ID)

@tasks.loop(minutes=1)
async def check_update():
    """정기적으로 업데이트를 체크합니다."""
    import re
    try:
        channel = client.get_channel(CHANNEL_ID)
        update = doc_service.worksheet("업데이트").get_all_values()

        if re.compile(r'\d{2}/\d{2}').search(update[1][0]).group() == get_date_slash():
            date = re.search(r'\d{2}/\d{2}', update[1][0]).group(0)
            
            if update[2][0] == "오늘은 경기가 없습니다.":
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
            
            elif update[-1][1] == "업데이트 완료":
                print("update started at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                total = int(re.search(r'(\d+)/(\d+)', update[2][0]).group(2))
                
                goat = doc_service.worksheet("GOAT").get_all_values()
                boat = doc_service.worksheet("BOAT").get_all_values()
                goat = pd.DataFrame(goat[1:], columns=goat[0])
                boat = pd.DataFrame(boat[1:], columns=boat[0])
                goat = goat[goat["날짜"] == date].iloc[0]
                boat = boat[boat["날짜"] == date].iloc[0]
                goat = goat["팀"] + "  " + goat["이름"]
                boat = boat["팀"] + "  " + boat["이름"]
                
                embed = discord.Embed(title=date + " 결과 업데이트 완료", description="총 " + str(total) + " 경기", color=discord.Color.green())
                embed.add_field(name='Daily GOAT', value=goat, inline=True)
                embed.add_field(name='Daily BOAT', value=boat, inline=False)
                embed.add_field(name='언탯티즈', value = '[차트](https://docs.google.com/spreadsheets/d/1dBXiLWcMnTToACuOySMume3xzkruL4iL_zLOYwtSaQY/edit?usp=sharing)')
                
                untatiz_graph()
                
                allowed_mentions = discord.AllowedMentions(everyone = True)
                await channel.send(content = "@everyone", allowed_mentions = allowed_mentions)
                await channel.send(embed=embed)
                
                date_str = get_date_today()
                image_file = f"{IMAGE_PATH}{date_str}/리그_{date_str}.png"
                await channel.send(file=discord.File(image_file))
                
                sheet = doc_service.worksheet("팀 순위")
                data = sheet.get_all_values()
                
                formatted_data = []
                for i, row in enumerate(data):
                    if 1 <= i <= 9:
                        formatted_row = " " + "\t".join(row)
                    elif i == 11:
                        formatted_row = "  " + "\t".join(row)
                    else:
                        formatted_row = "\t".join(row)
                    formatted_data.append(formatted_row)
                
                sheet_content = "\n".join(formatted_data)
                await channel.send(f"```{sheet_content}```")
                
                for user_id, team_names in subscribers.items():
                    user = client.get_user(user_id)
                    if user:
                        for team_name in team_names:
                            date_str = get_date_today()
                            image_file = f"{IMAGE_PATH}{date_str}/팀 {team_name}_{date_str}.png"
                            await user.send(file=discord.File(image_file))
                            sheet = doc_service.worksheet(f"팀 {team_name}")
                            data = sheet.get_all_values()
                            df = pd.DataFrame(data[1:], columns=data[0])
                            sheet_content = df.to_string(index=False)
                            await user.send(f"```{sheet_content}```")
                
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

        print("update skipped at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

    except Exception as e:
        print(f"An error occurred: {e}")
        await asyncio.sleep(60)

@client.command(aliases=['구독'])
async def subscribe(ctx, *, team_name=None):
    """팀 구독 명령어"""
    if team_name is None:
        await ctx.send(f"{ctx.author.mention}, 팀 이름을 입력해주세요. 허용된 팀 이름: {', '.join(allowed_teams)}")
        return
    
    if team_name not in allowed_teams:
        await ctx.send(f"{ctx.author.mention}, 올바른 팀 이름을 입력해주세요. 허용된 팀 이름: {', '.join(allowed_teams)}")
        return

    user_id = ctx.author.id
    if user_id not in subscribers:
        subscribers[user_id] = [team_name]
    else:
        if team_name in subscribers[user_id]:
            await ctx.send(f"{ctx.author.mention}, 이미 팀 {team_name} 구독 중입니다.")
            return
        subscribers[user_id].append(team_name)

    save_subscribers()
    await ctx.send(f"{ctx.author.mention}, 팀 {team_name} 구독되었습니다!")

@client.command(aliases=['구독취소', '구취'])
async def subscribe_cancel(ctx, *, team_name=None):
    """팀 구독 취소 명령어"""
    user_id = ctx.author.id
    
    if user_id not in subscribers or not subscribers[user_id]:
        await ctx.send(f"{ctx.author.mention}, 구독되어 있지 않습니다.")
        return
    
    if team_name is None:
        await ctx.send(f"{ctx.author.mention}, 팀 이름을 입력해주세요. 허용된 팀 이름: {', '.join(subscribers[user_id])}")
        return
    
    if team_name == '전부':
        subscribers.pop(user_id, None)
        await ctx.send(f"{ctx.author.mention}, 모든 팀 구독이 취소되었습니다!")
    elif team_name in subscribers[user_id]:
        subscribers[user_id].remove(team_name)
        if not subscribers[user_id]:
            subscribers.pop(user_id)
        await ctx.send(f"{ctx.author.mention}, 팀 {team_name} 구독이 취소되었습니다!")
    else:
        await ctx.send(f"{ctx.author.mention}, 구독 중이지 않은 팀입니다. 현재 구독 중인 팀: {', '.join(subscribers[user_id])}")
    
    save_subscribers()
        
@client.command(aliases=['리그'])
async def league(ctx):
    """리그 순위 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/리그_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 순위")
    data = sheet.get_all_values()
    
    formatted_data = []
    for i, row in enumerate(data):
        if 1 <= i <= 9:
            formatted_row = " " + "\t".join(row)
        elif i == 11:
            formatted_row = "  " + "\t".join(row)
        else:
            formatted_row = "\t".join(row)
        formatted_data.append(formatted_row)
    
    sheet_content = "\n".join(formatted_data)
    await ctx.send(f"```{sheet_content}```")
       
@client.command(aliases=['순위', '팀순위', '리그순위'])
async def chart(ctx):
    """팀 순위 데이터를 전송합니다."""
    sheet = doc_service.worksheet("팀 순위")
    data = sheet.get_all_values()
    
    formatted_data = []
    for i, row in enumerate(data):
        if 1 <= i <= 9:
            formatted_row = " " + "\t".join(row)
        elif i == 11:
            formatted_row = "  " + "\t".join(row)
        else:
            formatted_row = "\t".join(row)
        formatted_data.append(formatted_row)
    
    sheet_content = "\n".join(formatted_data)
    await ctx.send(f"```{sheet_content}```")
    
@client.command(aliases=['그래프', '순위그래프', '리그그래프', '팀그래프'])
async def graph(ctx):
    """리그 순위 그래프를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/리그_{date_str}.png"
    await ctx.send(file=discord.File(image_file))

@client.command(aliases=['언', '팀언'])
async def graph_eon(ctx):
    """팀 '언'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 언_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 언")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['앙', '팀앙'])
async def graph_ang(ctx):
    """팀 '앙'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 앙_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 앙")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['삼', '팀삼'])
async def graph_sam(ctx):
    """팀 '삼'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 삼_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 삼")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['준', '팀준'])
async def graph_jun(ctx):
    """팀 '준'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 준_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 준")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['역', '팀역'])
async def graph_yeok(ctx):
    """팀 '역'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 역_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 역")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['뚝', '팀뚝'])
async def graph_ttuk(ctx):
    """팀 '뚝'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 뚝_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 뚝")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['홍', '팀홍'])
async def graph_hong(ctx):
    """팀 '홍'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 홍_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 홍")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['엉', '팀엉'])
async def graph_eong(ctx):
    """팀 '엉'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 엉_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 엉")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['코', '팀코'])
async def graph_co(ctx):
    """팀 '코'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 코_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 코")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['옥', '팀옥'])
async def graph_ok(ctx):
    """팀 '옥'의 그래프와 데이터를 전송합니다."""
    date_str = get_date()
    image_file = f"{IMAGE_PATH}{date_str}/팀 옥_{date_str}.png"
    await ctx.send(file=discord.File(image_file))
    
    sheet = doc_service.worksheet("팀 옥")
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    sheet_content = df.to_string(index=False)
    await ctx.send(f"```{sheet_content}```")

@client.command(aliases=['팀'])
async def wrong_command(ctx):
    """잘못된 명령어에 대한 응답을 처리합니다."""
    await ctx.send("명령어 예시 : ./언 또는 ./팀언")

client.run(TOKEN)
