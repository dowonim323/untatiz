from selenium import webdriver as wd
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from pyvirtualdisplay import Display
import logging
import sys
import time
from datetime import datetime, timedelta
import pytz
import gspread
from gspread_formatting import color, cellFormat
from gspread_formatting import *
import os
import pandas as pd
import numpy as np
import requests
import shutil
from openpyxl import load_workbook
import re
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
import sqlite3
from stem import Signal
from stem.control import Controller
import json
from fake_useragent import UserAgent

os.environ['WDM_LOG'] = '0'

# 자격 증명 정보 로드
try:
    with open("api/credentials.json", "r") as f:
        credentials = json.load(f)
        TOR_PASSWORD = credentials["tor"]["password"]
except Exception as e:
    print(f"Error loading credentials: {str(e)}")
    TOR_PASSWORD = None  # 로드 실패 시 None으로 설정

# 로그 파일 경로 설정
log_directory = "/home/ubuntu/untatiz/log/"
log_filename = "untatiz.log"
log_path = log_directory + log_filename

# 로그 설정
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    handlers=[logging.FileHandler(log_path),
                              logging.StreamHandler(sys.stdout)])

logging.getLogger("stem").setLevel(logging.ERROR)

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

def changeIP():
    try:
        if TOR_PASSWORD is None:
            raise ValueError("Tor password not loaded")
            
        with Controller.from_port(port = 9051) as controller:
            controller.authenticate(password=TOR_PASSWORD)
            controller.signal(Signal.NEWNYM)
            
            while not controller.is_newnym_available():
                time.sleep(controller.get_newnym_wait() + 1)
            
            # Get and print the new IP address
            session = requests.session()
            session.proxies = {'http': 'socks5://127.0.0.1:9050', 'https': 'socks5://127.0.0.1:9050'}
            response = session.get('https://httpbin.org/ip')
            new_ip = response.json()['origin']
            print(f"IP switched to: {new_ip}")

    except Exception as e:
        print(f"Error changing IP: {str(e)}")

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

# KST 시간을 기준으로 날짜를 반환하는 함수
def get_date():
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    if now.hour < 14:
        date_to_display = now - timedelta(days=1)
    else:
        date_to_display = now
    return date_to_display.strftime('%m/%d')

# 데이터 로드 함수
def load_data():
    player_name = load_table("draft_name")
    player_id = load_table("draft_id")
    player_transaction = load_table("player_transaction")

    player_id = player_id.astype('str')
    player_transaction = player_transaction.astype('str')
    player_transaction["WAR"] = player_transaction["WAR"].astype(float)

    player_name = player_name.set_index("팀")
    player_id = player_id.set_index("팀")

    player_activation = player_id.copy()
    player_activation.loc[:, :] = True

    war_basis = player_id.copy()
    war_basis.loc[:, :] = 0

    for index in player_transaction.index:
        name = player_transaction.iloc[index]["name"]
        id = player_transaction.iloc[index]["id"]
        old = player_transaction.iloc[index]["old"].lstrip("팀 ")
        new = player_transaction.iloc[index]["new"].lstrip("팀 ")
        war = float(player_transaction.iloc[index]["WAR"])

        if old != "퐈":
            data = player_id.loc[old]
            old_position = data[data == id].index[0]
            player_activation.loc[old, old_position] = False
            war_basis.loc[old, old_position] = war - war_basis.loc[old, old_position]

        if new != "퐈":
            if (player_id.loc[new] == id).any():
                if not player_activation.loc[new, player_id.loc[new, player_id.loc[new] == id].index[0]]:
                    new_position = player_id.loc[new, player_id.loc[new] == id].index[0]
                    player_activation.loc[new, new_position] = True
                    war_basis.loc[new, new_position] = war - war_basis.loc[new, new_position]
            else:
                if not pd.isna(player_id.loc[new].iloc[-1]):
                    new_col = "추가" + str(len(player_id.columns) - 27)
                    player_id[new_col] = np.nan
                    player_name[new_col] = np.nan
                    player_activation[new_col] = np.nan
                    war_basis[new_col] = np.nan

                    player_id[new_col] = player_id[new_col].astype(object)
                    player_name[new_col] = player_name[new_col].astype(object)
                    player_activation[new_col] = player_activation[new_col].astype(object)
                    war_basis[new_col] = war_basis[new_col].astype(object)

                new_position = player_id.loc[new, player_id.loc[new].isna()].index[0]
                player_id.loc[new, new_position] = id
                player_name.loc[new, new_position] = name
                player_activation.loc[new, new_position] = True
                war_basis.loc[new, new_position] = war

    return player_name, player_id, player_activation, war_basis, player_transaction

# 타자 데이터 로드 함수
def load_statiz_bat(driver):
    driver.get("https://statiz.sporki.com/stats/?m=main&m2=batting&m3=default&so=WAR&ob=DESC&year=2025&sy=&ey=&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2=")
    bat_chart = BeautifulSoup(driver.page_source, 'html.parser').find_all("table")[0]

    driver.get("https://statiz.sporki.com/stats/?m=main&m2=batting&m3=value&so=WAR&ob=DESC&year=2025&sy=&ey=&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2=")
    bat_value_chart = BeautifulSoup(driver.page_source, 'html.parser').find_all("table")[0]

    team_dic = {
        "/data/team/ci/2025/2002.svg": "KIA",
        "/data/team/ci/2025/12001.svg": "KT",
        "/data/team/ci/2025/10001.svg": "키움",
        "/data/team/ci/2025/11001.svg": "NC",
        "/data/team/ci/2025/5002.svg": "LG",
        "/data/team/ci/2025/1001.svg": "삼성",
        "/data/team/ci/2025/6002.svg": "두산",
        "/data/team/ci/2025/7002.svg": "한화",
        "/data/team/ci/2025/9002.svg": "SSG",
        "/data/team/ci/2025/3001.svg": "롯데"
    }

    column = ["Rank", "Name", "ID", "Team", "POS", "WAR", "oWAR", "dWAR", "G", "PA", "ePA", "AB", "R", "H", "2B", "3B", "HR", "TB", "RBI", "SB", "CS", "BB", "HP", "IB", "SO", "GDP", "SH", "SF", "AVG", "OBP", "SLG", "OPS", "R/ePA", "wRC+", "WAR"]
    bat = pd.DataFrame(columns=column)
    templen = len(bat_chart.find_all("tr"))

    for i in range(2, templen):
        tempTr = bat_chart.find_all("tr")[i]
        if tempTr.find("th") is not None:
            continue
        row = {}
        column_idx = 0
        for j in range(32):
            tempTd = tempTr.find_all("td")[j].text
            if j == 1:  # Name 열
                ID = tempTr.find_all("td")[j].find('a')['href'].split('p_no=')[-1]
                row[column[column_idx]] = tempTd
                column_idx += 1
                row[column[column_idx]] = ID
                column_idx += 1
            elif j == 2:
                team = tempTr.find_all("td")[j].find('img')['src']
                row[column[column_idx]] = team_dic[team]
                column_idx += 1
                pos = tempTr.find_all("td")[j].find_all('span')[-1].text
                row[column[column_idx]] = pos
                column_idx += 1
            else:
                row[column[column_idx]] = tempTd
                column_idx += 1
        row_df = pd.DataFrame([row], columns=column)
        bat = pd.concat([bat, row_df], ignore_index=True)

    bat = bat.loc[:, ~bat.columns.duplicated()]
    
    bat['oWAR'] = bat['oWAR'].replace('', 0).astype(float)

    bat = bat.set_index(keys='ID')

    bat = bat.sort_values(by='oWAR', ascending=False)

    bat['Rank'] = range(1, len(bat) + 1)

    return bat

# 투수 데이터 로드 함수
def load_statiz_pit(driver):
    driver.get("https://statiz.sporki.com/stats/?m=main&m2=pitching&m3=default&so=WAR&ob=DESC&year=2025&sy=&ey=&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2=")
    pit_chart = BeautifulSoup(driver.page_source, 'html.parser').find_all("table")[0]

    team_dic = {
        "/data/team/ci/2025/2002.svg": "KIA",
        "/data/team/ci/2025/12001.svg": "KT",
        "/data/team/ci/2025/10001.svg": "키움",
        "/data/team/ci/2025/11001.svg": "NC",
        "/data/team/ci/2025/5002.svg": "LG",
        "/data/team/ci/2025/1001.svg": "삼성",
        "/data/team/ci/2025/6002.svg": "두산",
        "/data/team/ci/2025/7002.svg": "한화",
        "/data/team/ci/2025/9002.svg": "SSG",
        "/data/team/ci/2025/3001.svg": "롯데"
    }

    column = ["Rank", "Name", "ID", "Team", "POS", "WAR", "G", "GS", "GR", "GF", "CG", "SHO", "W", "L", "S", "HD", "IP", "ER", "R", "rRA", "TBF", "H", "2B", "3B", "HR", "BB", "HP", "IB", "SO", "ROE", "BK", "WP", "ERA", "RA9", "rRA9", "rRA9pf", "FIP", "WHIP", "WAR"]
    pit = pd.DataFrame(columns=column)
    templen = len(pit_chart.find_all("tr"))

    for i in range(2, templen):
        tempTr = pit_chart.find_all("tr")[i]
        if tempTr.find("th") is not None:
            continue
        row = {}
        column_idx = 0
        for j in range(36):
            tempTd = tempTr.find_all("td")[j].text
            if j == 1:  # Name 열
                ID = tempTr.find_all("td")[j].find('a')['href'].split('p_no=')[-1]
                row[column[column_idx]] = tempTd
                column_idx += 1
                row[column[column_idx]] = ID
                column_idx += 1
            elif j == 2:
                team = tempTr.find_all("td")[j].find('img')['src']
                row[column[column_idx]] = team_dic[team]
                column_idx += 1
                pos = tempTr.find_all("td")[j].find_all('span')[-1].text
                row[column[column_idx]] = pos
                column_idx += 1
            else:
                row[column[column_idx]] = tempTd
                column_idx += 1
        row_df = pd.DataFrame([row], columns=column)
        pit = pd.concat([pit, row_df], ignore_index=True)

    pit = pit.loc[:, ~pit.columns.duplicated()]

    pit['WAR'] = pit['WAR'].replace('', 0).astype(float)

    pit = pit.set_index(keys='ID')

    pit = pit.sort_values(by='WAR', ascending=False)

    pit['Rank'] = range(1, len(pit) + 1)

    return pit

# 선수 활성화 상태 확인 함수
def isactive(id, player_id, player_activation):
    positions = player_id.apply(lambda col: col[col == id].index.tolist()).dropna()
    activation = False
    for col in positions.index:
        for row in positions[col]:
            if player_activation.at[row, col]: activation = True
    return activation

# 우측 하단 셀 주소 반환 함수
def right_bottom(doc, sheet):
    worksheet = doc.worksheet(sheet)
    all_values = worksheet.get_all_values()

    last_row = len(all_values)
    last_column = len(all_values[0])

    def col_to_letter(col):
        letter = ''
        while col > 0:
            col, remainder = divmod(col - 1, 26)
            letter = chr(65 + remainder) + letter
        return letter

    return f"{col_to_letter(last_column)}{last_row}"

# WAR 계산 함수
def get_war(bat, pit, player_id, player_activation, war_basis):
    live_war = player_id.copy()

    for team in live_war.index:
        for column in live_war.columns:
            ID = live_war.loc[team, column]
            if np.isnan(pd.to_numeric(ID)):
                live_war.loc[team, column] = np.nan
            elif ID == 0:
                live_war.loc[team, column] = 0
            else:
                #bat_war = float(bat.loc[ID, "WAR*"]) if ID in bat.index else 0
                bat_war = float(bat.loc[ID, "oWAR"]) if ID in bat.index else 0
                pit_war = float(pit.loc[ID, "WAR"]) if ID in pit.index else 0
                live_war.loc[team, column] = bat_war + pit_war

    current_war = live_war.copy()

    for team in current_war.index:
        for column in current_war.columns:
            war = live_war.loc[team, column]
            if np.isnan(player_activation.loc[team, column]):
                current_war.loc[team, column] = np.nan
            elif player_activation.loc[team, column]:
                current_war.loc[team, column] = war - war_basis.loc[team, column]
            else:
                current_war.loc[team, column] = war_basis.loc[team, column]

    return live_war, current_war

# DB 업데이트 함수
def update_db(player_name, player_id, player_activation, live_war, current_war, bat, pit, games):
    for team in player_id.index:
        data = pd.DataFrame({"ID": player_id.loc[team], "Name": player_name.loc[team]})
        data = data[np.isnan(pd.to_numeric(data["ID"])) == False]

        db = load_table(team)
        db = db.set_index("index")
        db.index.name = None
        db = db.drop(["ID", "Name"], axis=1)

        date = get_date()

        if date in db.columns:
            db = db.drop(date, axis=1)

        data = data.join(db)

        today = current_war.loc[[team]]
        today.index.name = None
        today = today.T
        today = today[np.isnan(pd.to_numeric(today[team])) == False]
        today = today.rename(columns={team: date})

        data = data.join(today)

        data.iloc[:, 2:] = data.iloc[:, 2:].replace('', np.nan).astype(float)

        save_sheet(data.reset_index(), team)

        data_diff = pd.DataFrame({"ID": player_id.loc[team], "Name": player_name.loc[team]})
        data_diff = data_diff[np.isnan(pd.to_numeric(data_diff["ID"])) == False]

        db_diff = load_table(team + "_변화")
        db_diff = db_diff.set_index("index")
        db_diff.index.name = None
        db_diff = db_diff.drop(["ID", "Name"], axis=1)

        if date in db_diff.columns:
            db_diff = db_diff.drop(date, axis=1)

        data_diff = data_diff.join(db_diff)

        data["gap"] = data.apply(lambda x: x.iloc[-1] - (0 if x.iloc[-2] == np.nan else x.iloc[-2]), axis=1)
        gap = data.iloc[:, [-1]]
        gap = gap.rename(columns={"gap": date})

        data_diff = data_diff.join(gap)

        save_sheet(data_diff.reset_index(), team + "_변화")

    db = load_table("teams")
    db = db.set_index("팀")

    date = get_date()

    if date in db.columns:
        db = db.drop(date, axis=1)

    bat_fa = bat[bat.index.to_series().apply(lambda x: isactive(x, player_id, player_activation)) == False]
    pit_fa = pit[pit.index.to_series().apply(lambda x: isactive(x, player_id, player_activation)) == False]

    OF_fa = bat_fa[bat_fa["POS"].isin(["LF", "CR", "RF"])]
    IF_fa = bat_fa[bat_fa["POS"].isin(["1B", "2B", "3B", "SS"])]
    C_fa = bat_fa[bat_fa["POS"].isin(["C"])]

    else_fa = pd.concat([OF_fa.iloc[6:].assign(**{'WAR/oWAR': OF_fa['oWAR']})[["Name", "Team", "POS", "WAR/oWAR"]],
                            IF_fa.iloc[8:].assign(**{'WAR/oWAR': IF_fa['oWAR']})[["Name", "Team", "POS", "WAR/oWAR"]],
                            C_fa.iloc[3:].assign(**{'WAR/oWAR': C_fa['oWAR']})[["Name", "Team", "POS", "WAR/oWAR"]],
                            pit_fa.iloc[12:].assign(**{'WAR/oWAR': pit_fa['WAR']})[["Name", "Team", "POS", "WAR/oWAR"]]])
    else_fa = else_fa.sort_values(by='WAR/oWAR', ascending=False)

    war_fa = OF_fa.iloc[0:5]["oWAR"].sum() + IF_fa.iloc[0:7]["oWAR"].sum() + C_fa.iloc[0:2]["oWAR"].sum() + pit_fa.iloc[0:11]["WAR"].sum() + else_fa.iloc[0:3]["WAR/oWAR"].sum()

    today = pd.DataFrame(current_war.sum(axis=1), columns=[date])
    today.loc["퐈"] = [war_fa]

    db = db.join(today)

    save_sheet(db.reset_index(), "teams")
    
    db_diff = load_table("teams_diff")
    db_diff = db_diff.set_index("팀")

    if date in db_diff.columns:
        db_diff = db_diff.drop(date, axis=1)

    db["gap"] = db.apply(lambda x: x.iloc[-1] - (0.0 if x.iloc[-2] == "" else x.iloc[-2]), axis=1)
    gap = db.iloc[:, [-1]]
    gap = gap.rename(columns={"gap": date})

    db_diff = db_diff.join(gap)

    save_sheet(db_diff.reset_index(), "teams_diff")
    
    save_sheet(live_war.iloc[:, 0:28].reset_index(), "draft_live_war")
    
    data = bat[["Name", "oWAR"]].rename(columns={"oWAR": date})
    bat_mapping = bat.reset_index()[['ID', 'Name']].drop_duplicates().set_index('ID')
    data = data.drop("Name", axis=1)
        
    db = load_table("bat")
    db = db.set_index("ID")

    if date in db.columns:
        db = db.drop(date, axis=1)

    db = db.join(data, how="outer")
    mask = db['Name'].isna()
    db.loc[mask, 'Name'] = db.loc[mask].index.map(bat_mapping['Name'].get)
    
    save_sheet(db.reset_index(), "bat")
    
    data = pit[["Name", "WAR"]].rename(columns={"WAR": date})
    pit_mapping = pit.reset_index()[['ID', 'Name']].drop_duplicates().set_index('ID')
    data = data.drop("Name", axis=1)
        
    db = load_table("pit")
    db = db.set_index("ID")

    if date in db.columns:
        db = db.drop(date, axis=1)

    db = db.join(data, how="outer")
    mask = db['Name'].isna()
    db.loc[mask, 'Name'] = db.loc[mask].index.map(pit_mapping['Name'].get)
    
    save_sheet(db.reset_index(), "pit")
    
    db = load_table("roster").set_index("팀")

    if date in db.columns:
        db = db.drop(date, axis=1)
        
    active_players = pd.Series(index=player_activation.index, dtype=object)

    for team in player_activation.index:
        active_ids = []
        for round_name in player_activation.columns:
            if player_activation.loc[team, round_name]:
                player_id_value = player_id.loc[team, round_name]
                if pd.notnull(player_id_value):
                    active_ids.append(str(int(player_id_value)))
        active_players[team] = ','.join(active_ids)

    db[date] = active_players

    save_sheet(db.reset_index(), "roster")
        
    dfs = []

    for team in player_id.index:
        db_diff = load_table(team + "_변화")
        db_diff = db_diff.iloc[:, 2:]
        db_diff.insert(0, '소속팀', team)
        dates = db_diff.columns[2:]
        dfs_team = []

        for date in dates:
            df_temp = db_diff[["소속팀", "Name", date]].copy()
            df_temp.columns = ['소속팀', '이름', 'WAR 변동']
            df_temp.insert(1, "날짜", date)
            dfs_team.append(df_temp)

        dfs.extend(dfs_team)

    diff = pd.concat(dfs, ignore_index=True)

    diff['WAR 변동'] = diff.apply(lambda x: float(x["WAR 변동"]), axis=1)
    GOAT = diff.copy()
    GOAT = GOAT[GOAT['WAR 변동'] > 0]
    GOAT = GOAT.sort_values(by='WAR 변동', ascending=False)
    GOAT['WAR 변동'] = GOAT.apply(lambda x: f'{x["WAR 변동"]:.2f}', axis=1)
    save_sheet(GOAT, "GOAT")
    
    BOAT = diff.copy()
    BOAT = BOAT[BOAT['WAR 변동'] < 0]
    BOAT = BOAT.sort_values(by='WAR 변동', ascending=True)
    BOAT['WAR 변동'] = BOAT.apply(lambda x: f'{x["WAR 변동"]:.2f}', axis=1)
    save_sheet(BOAT, "BOAT")
    
    kst = pytz.timezone('Asia/Seoul')
    now = str(datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S'))
    update = pd.DataFrame([["업데이트 시간", now]])

    if check_update(bat, pit, games):
        war_update = pd.DataFrame([["WAR 업데이트", "업데이트 완료"]])
    elif games.iloc[-1, 0] == '오늘은 경기가 없습니다.':
        war_update = pd.DataFrame()
    else:
        war_update = pd.DataFrame([["WAR 업데이트", "업데이트 전"]])

    update = pd.concat([update, games, war_update], axis=0)
    update = update.fillna("")
    
    save_sheet(update, "update_info")

# 경기 결과 업데이트 함수
def update_games(driver, return_type="df"):
    today = get_date()
    today_month = int(today.split('/')[0])
    today_day = int(today.split('/')[1])

    driver.get("https://statiz.sporki.com/stats/?m=main&m2=pitching&m3=situation1&so=ERA&ob=ASC&year=2025&sy=&ey=&te=&po=&lt=10100&reg=A&pe=I&ds=" + str(today_month).zfill(2) + "-" + str(today_day).zfill(2) + "&de=" + str(today_month).zfill(2) + "-" + str(today_day).zfill(2) + "&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2=")
    pit_chart = BeautifulSoup(driver.page_source, 'html.parser').find_all("table")[0]

    team_dic = {
        "/data/team/ci/2025/2002.svg": "KIA",
        "/data/team/ci/2025/12001.svg": "KT",
        "/data/team/ci/2025/10001.svg": "키움",
        "/data/team/ci/2025/11001.svg": "NC",
        "/data/team/ci/2025/5002.svg": "LG",
        "/data/team/ci/2025/1001.svg": "삼성",
        "/data/team/ci/2025/6002.svg": "두산",
        "/data/team/ci/2025/7002.svg": "한화",
        "/data/team/ci/2025/9002.svg": "SSG",
        "/data/team/ci/2025/3001.svg": "롯데"
    }

    updated_teams = set()

    templen = len(pit_chart.find_all("tr"))

    for i in range(2, templen):
        tempTr = pit_chart.find_all("tr")[i]
        if tempTr.find("th") is not None:  continue
        updated_teams.add(team_dic[tempTr.find_all("td")[2].find('img')['src']])

    driver.get("https://statiz.sporki.com/schedule/?year=2025&month=" + str(today_month))
    calender = BeautifulSoup(driver.page_source, 'html.parser').find_all("table")[0]
    templen = len(calender.find_all("tr"))

    for i in range(1, templen):
        tempTr = calender.find_all("tr")[i]
        if tempTr.find("th") is not None:
            continue
        for j in range(7):
            tempTd = tempTr.find_all("td")[j]
            if tempTd.find('span', class_='day') is None:  continue
            day = tempTd.find('span', class_='day').text
            if int(day) == today_day:
                temp = tempTd
                break

    games = []
    for li in temp.find_all('li'):
        games.append([])
        for span in li.find_all('span'):
            games[-1].append(span.text)

    game_number = 0
    updated_number = 0
    started_number = 0

    if not games:
        games = [["오늘은 경기가 없습니다."]]
    else:
        for i, game in enumerate(games):
            if len(game) == 4:
                status = (game[0] in updated_teams) and (game[3] in updated_teams)
                if status:
                    games[i] = [game[0], str(game[1]) + " : " + str(game[2]), game[3]]
                    game_number += 1
                    updated_number += 1
                    started_number += 1
                else:
                    games[i] = [game[0], "업데이트 전", game[3]]
                    game_number += 1
                    started_number += 1
            elif len(game) == 3 and game[1] == "우천취소":
                games[i] = [game[0], "우천취소", game[2]]
            elif len(game) == 3:
                games[i] = [game[0], "업데이트 전", game[2]]
                game_number += 1
        games.insert(0, ["업데이트 : " + str(updated_number) + "/" + str(game_number) + "경기", "", ""])

    games.insert(0, ["경기 날짜 : " + str(today), "", ""])

    if return_type == "df":
        return pd.DataFrame(games)
    else:
        return pd.DataFrame(games), started_number

# DB 백업 함수
def backup_db():
    kst = pytz.timezone('Asia/Seoul')
    now = str(datetime.now(kst).strftime('%Y%m%d%H%M%S'))
    backup_dir = '/home/ubuntu/untatiz/backup/'
    shutil.copy2("/home/ubuntu/untatiz/db/untatiz_db.db", f'{backup_dir}{now}.db')

# 시트 저장 함수
def save_sheet(df, table_name):
    """DataFrame을 SQLite 테이블로 저장
    
    Args:
        df: 저장할 pandas DataFrame
        table_name: 저장할 테이블 이름
    """
    try:
        with sqlite3.connect("/home/ubuntu/untatiz/db/untatiz_db.db") as conn:
            # DataFrame을 SQLite 테이블로 저장
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists='replace',  # 기존 테이블이 있으면 덮어쓰기
                index=False
            )
    except Exception as e:
        print(f"Error saving table {table_name}: {str(e)}")
        raise

# 시간 상태 반환 함수
def get_time_status():
    now = datetime.now()
    hour = now.hour
    minute = now.minute
    second = now.second

    if 0 <= minute < 30:
        return 0
    else:
        return 1

# WAR 상태 반환 함수
def get_war_status():
    war_update = load_table("update_info").iloc[-1, 1]
    if war_update == "업데이트 완료": return 1
    elif war_update == "업데이트 전": return 0

# 팀 상태 반환 함수
def get_team_status(driver):
    update, started = update_games(driver, "started")
    update = update.values.tolist()
    if update[1][0] == '오늘은 경기가 없습니다.': return 0, 0, 0
    updated = int(re.search(r'(\d+)/(\d+)', update[1][0]).group(1))
    total = int(re.search(r'(\d+)/(\d+)', update[1][0]).group(2))
    return started, updated, total

# 업데이트된 팀 반환 함수
def updated_teams(driver):
    today = get_date()
    today_month = int(today.split('/')[0])
    today_day = int(today.split('/')[1])

    driver.get("https://statiz.sporki.com/stats/?m=main&m2=pitching&m3=situation1&so=ERA&ob=ASC&year=2024&sy=&ey=&te=&po=&lt=10100&reg=A&pe=I&ds=" + str(today_month).zfill(2) + "-" + str(today_day).zfill(2) + "&de=" + str(today_month).zfill(2) + "-" + str(today_day).zfill(2) + "&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2=")
    pit_chart = BeautifulSoup(driver.page_source, 'html.parser').find_all("table")[0]

    team_dic = {
        "/data/team/ci/2024/2002.svg": "KIA",
        "/data/team/ci/2024/12001.svg": "KT",
        "/data/team/ci/2024/10001.svg": "키움",
        "/data/team/ci/2024/11001.svg": "NC",
        "/data/team/ci/2024/5002.svg": "LG",
        "/data/team/ci/2024/1001.svg": "삼성",
        "/data/team/ci/2024/6002.svg": "두산",
        "/data/team/ci/2024/7002.svg": "한화",
        "/data/team/ci/2024/9002.svg": "SSG",
        "/data/team/ci/2024/3001.svg": "롯데"
    }

    updated_teams = set()

    templen = len(pit_chart.find_all("tr"))

    for i in range(2, templen):
        tempTr = pit_chart.find_all("tr")[i]
        if tempTr.find("th") is not None:  continue
        updated_teams.add(team_dic[tempTr.find_all("td")[2].find('img')['src']])

    return updated_teams

def check_update(bat, pit, games):
    
    today = get_date()
    yesterday = (datetime.strptime(today, "%m/%d") - timedelta(days=1)).strftime("%m/%d")

    bat_update = bat[["Team"]].join(load_table("bat").set_index("ID"), how="left")
    pit_update = pit[["Team"]].join(load_table("pit").set_index("ID"), how="left")

    for col in bat_update.columns:
        if col != "Team" and col != "Name":
            bat_update[col] = bat_update[col].apply(lambda x: f"{float(x):.2f}" if isinstance(x, float) else x)

    for col in pit_update.columns:
        if col != "Team" and col != "Name":
            pit_update[col] = pit_update[col].apply(lambda x: f"{float(x):.2f}" if isinstance(x, float) else x)

    bat_updated_teams = list(bat_update[bat_update[today] != bat_update[yesterday]]["Team"].unique())
    pit_updated_teams = list(pit_update[pit_update[today] != pit_update[yesterday]]["Team"].unique())
    
    filtered_games = games[(games[2] != '') & (games[1] != '우천취소')]
    teams_set = set(filtered_games[0].tolist() + filtered_games[2].tolist())
    
    # Check if all teams in teams_set are present in both bat_updated_teams and pit_updated_teams
    all_teams_in_bat = all(team in bat_updated_teams for team in teams_set)
    all_teams_in_pit = all(team in pit_updated_teams for team in teams_set)
    
    return all_teams_in_bat and all_teams_in_pit

# 크롬 옵션 설정
chrome_options = wd.ChromeOptions()
chrome_options.binary_location = "/usr/bin/chromium-browser"
chrome_options.add_argument('--headless')
chrome_options.add_argument("--proxy-server=socks5://127.0.0.1:9050")

ua = UserAgent()
user_agent = ua.random
chrome_options.add_argument('user-agent=' + user_agent)

# 시간 및 팀 상태 초기화
time_previous = 0
time_current = -1

team_previous = set()
team_current = set([0])

update_status = 1

ip_count = 10

while(True):
    try:
        while(True):
            
            if ip_count == 0:
                changeIP()
                ip_count = 10
            
            user_agent = ua.random
            chrome_options.add_argument('user-agent=' + user_agent)
            
            driver = wd.Chrome(service=Service("/usr/bin/chromedriver"), options=chrome_options)
            
            war_status = get_war_status()
            started, updated, total = get_team_status(driver)
            
            team_previous = team_current
            team_current = updated_teams(driver)
            
            ip_count -= 1
            
            driver.quit()
            
            time_previous = time_current
            time_current = get_time_status()
            
            if update_status == 0  and total > 0:
                update_status = 1
                print("update restarted at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                break
            if update_status == 0:
                print("update skipped at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                time.sleep(60)
                continue
            if total == 0:
                update_status = 0
                print("update paused at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                break
            if war_status == 0 and updated == total: break 
            if started > 0 and time_previous != time_current: break
            if started > 0 and team_previous != team_current: break
            
            print("update skipped at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
            time.sleep(60)
        
        driver = wd.Chrome(service=Service("/usr/bin/chromedriver"), options=chrome_options)
        
        print("update started at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        
        #update_id(driver)
        player_name, player_id, player_activation, war_basis, transaction = load_data()
        games = update_games(driver)
        bat = load_statiz_bat(driver)
        pit = load_statiz_pit(driver)
        live_war, current_war = get_war(bat, pit, player_id, player_activation, war_basis)
        update_db(player_name, player_id, player_activation, live_war, current_war, bat, pit, games)
        backup_db()
        
        print("update finished at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        
        driver.quit()
        
        time.sleep(60)
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        
        changeIP()
        ip_count = 10
        
        time.sleep(10)