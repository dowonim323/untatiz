from selenium import webdriver as wd
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

os.environ['WDM_LOG'] = '0'

# 로그 파일 경로 설정
log_directory = "/home/imdw/untatiz/log/"
log_filename = "untatiz.log"
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

display = Display(visible=0, size=(1920, 1080))
display.start()

# KST 시간을 기준으로 날짜를 반환하는 함수
def get_date():
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    if now.hour < 14:
        date_to_display = now - timedelta(days=1)
    else:
        date_to_display = now
    return date_to_display.strftime('%m/%d')

# 구글 스프레드시트 로드 함수
def load_gspread(json_path, url):
    gc = gspread.service_account(json_path)
    return gc.open_by_url(url)

# 데이터 로드 함수
def load_data():
    player_name = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="player_name")
    player_id = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="player_id")
    transaction = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="transaction")

    player_id = player_id.astype('str')
    transaction = transaction.astype('str')

    player_name = player_name.set_index("팀")
    player_id = player_id.set_index("팀")

    player_activation = player_id.copy()
    player_activation.loc[:, :] = True

    war_basis = player_id.copy()
    war_basis.loc[:, :] = 0

    for index in transaction.index:
        name = transaction.iloc[index]["name"]
        id = transaction.iloc[index]["id"]
        old = transaction.iloc[index]["old"].lstrip("팀 ")
        new = transaction.iloc[index]["new"].lstrip("팀 ")
        war = float(transaction.iloc[index]["WAR"])

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

    return player_name, player_id, player_activation, war_basis, transaction

# 플레이어 ID 검색 함수
def get_player_id(driver, name):
    driver.get("https://statiz.sporki.com/player/?m=search&s=" + name)
    html = driver.page_source
    bsObject = BeautifulSoup(html, 'html.parser')
    if bool(re.search(r'p_no=\d+$', driver.current_url)):
        return driver.current_url.split('p_no=')[-1]
    temp = bsObject.find_all("table")[0]
    templen = len(temp.find_all("tr"))
    if temp.find_all("tr")[1].text == '검색된 선수가 없습니다.':
        return "0"
    for i in range(1, templen):
        tempTr = temp.find_all("tr")[i]
        if tempTr.find("th") is not None:
            continue
    return "검색 필요"

# 플레이어 이름 검색 함수
def get_player_name(driver, id):
    driver.get("https://statiz.sporki.com/player/?m=playerinfo&p_no=" + id)
    html = driver.page_source
    bsObject = BeautifulSoup(html, 'html.parser')
    name_element = bsObject.select_one("body > div.warp > div.container > section > div.player_info_header > div.bio > div.p_info > div.name")
    if name_element:
        full_name = name_element.text.strip()
        korean_name = re.split(r'\s*\(\s*', full_name)[0]
        return korean_name
    return None

# ntfy 알림 함수
def ntfy(data):
    url = 'http://ntfy.dowonim.com/untatiz'
    headers = {
        'Content-Type': 'text/plain',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    requests.post(url, data=data, headers=headers)

# ID 업데이트 함수
def update_id(driver):
    player_name = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="player_name")
    player_id = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="player_id")
    transaction = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="transaction").replace(np.nan, "")

    transaction["WAR"] = transaction["WAR"].astype(float)
    transaction["WAR"] = transaction["WAR"].apply(lambda x: f"{x:.2f}")
    transaction = transaction.astype(str)

    player_id = player_id.astype('str')
    transaction = transaction.astype('str')

    player_name = player_name.set_index("팀")
    player_id = player_id.set_index("팀")

    error_state = 0

    id_to_name = player_id.copy()

    for i in range(0, id_to_name.shape[0]):
        for j in range(0, id_to_name.shape[1]):
            id = id_to_name.iat[i, j]
            if id:  # Ensure the cell is not empty
                name = get_player_name(driver, id)
                id_to_name.iat[i, j] = name

    for team in player_name.index:
        for column in player_name.columns:
            if player_name.loc[team, column] != id_to_name.loc[team, column]:
                if player_id.loc[team, column] == "0":
                    name = player_name.loc[team, column]
                    driver.get("https://statiz.sporki.com/player/?m=search&s=" + name)
                    html = driver.page_source
                    bsObject = BeautifulSoup(html, 'html.parser')
                    if bool(re.search(r'p_no=\d+$', driver.current_url)):
                        player_id.loc[team, column] = driver.current_url.split('p_no=')[-1]
                        ntfy("draft id of " + player_name.loc[team, column] + " is updated.")
                        continue
                    temp = bsObject.find_all("table")[0]
                    if temp.find_all("tr")[1].text == '검색된 선수가 없습니다.':
                        continue
                else:
                    id = get_player_id(driver, player_name.loc[team, column])
                    player_id.loc[team, column] = id
                    if id == "검색 필요":
                        ntfy(player_name.loc[team, column] + " draft_id_error")
                        error_state = 1

    for trans in transaction.index:
        if get_player_name(driver, transaction.loc[trans, "id"]) != transaction.loc[trans, "name"]:
            if transaction.loc[trans, "id"] == '0':
                name = transaction.loc[trans, "name"]
                driver.get("https://statiz.sporki.com/player/?m=search&s=" + name)
                html = driver.page_source
                bsObject = BeautifulSoup(html, 'html.parser')
                if bool(re.search(r'p_no=\d+$', driver.current_url)):
                    transaction.loc[trans, "id"] = driver.current_url.split('p_no=')[-1]
                    ntfy("transaction id of " + transaction.loc[trans, "name"] + " is updated.")
                    continue
                temp = bsObject.find_all("table")[0]
                if temp.find_all("tr")[1].text == '검색된 선수가 없습니다.':
                    continue
            else:
                id = get_player_id(driver, transaction.loc[trans, "name"])
                transaction.loc[trans, "id"] = id
                if id == "검색 필요":
                    ntfy(transaction.loc[trans, "name"] + " transaction_id_error")
                    error_state = 1

    player_id = player_id.reset_index()
    player_name = player_name.reset_index()

    save_sheet(player_id, "player_id")
    save_sheet(player_name, "player_name")
    save_sheet(transaction, "transaction")

    if error_state == 1:
        quit()

# 타자 데이터 로드 함수
def load_statiz_bat(driver):
    driver.get("https://statiz.sporki.com/stats/?m=main&m2=batting&m3=default&so=WAR&ob=DESC&year=2024&sy=&ey=&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2=")
    bat_chart = BeautifulSoup(driver.page_source, 'html.parser').find_all("table")[0]

    driver.get("https://statiz.sporki.com/stats/?m=main&m2=batting&m3=value&so=WAR&ob=DESC&year=2024&sy=&ey=&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2=")
    bat_value_chart = BeautifulSoup(driver.page_source, 'html.parser').find_all("table")[0]

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

    column = ["Rank", "Name", "ID", "Team", "POS", "WAR", "PA", "타격 RAA", "도루 RAA", "주루 RAA", "공격 RAA", "필딩 RAA", "포지션 RAA", "수비 RAA", "종합 RAA", "대체 Run", "RAR", "RPW", "WAAOff", "WAA", "oWAR", "dWAR", "WAR", "연봉(만원)", "WAR당 연봉"]

    bat_value = pd.DataFrame(columns=column)
    templen = len(bat_value_chart.find_all("tr"))

    for i in range(2, templen):
        tempTr = bat_value_chart.find_all("tr")[i]
        if tempTr.find("th") is not None:
            continue
        row = {}
        column_idx = 0
        for j in range(23):
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
        bat_value = pd.concat([bat_value, row_df], ignore_index=True)

    bat = pd.merge(bat, bat_value[["ID", "포지션 RAA", "RPW"]], how='outer', on='ID')

    bat.insert(bat.columns.get_indexer_for(['WAR'])[0], 'WAR*', 0.0)
    bat['포지션 RAA'] = bat['포지션 RAA'].astype(float)
    bat['RPW'] = bat['RPW'].astype(float)
    bat['oWAR'] = bat['oWAR'].astype(float)
    bat['포지션 RAA'] = bat['포지션 RAA'].fillna(0)
    bat['RPW'] = bat['RPW'].fillna(bat['RPW'].mean())

    bat['WAR*'] = bat.apply(lambda row: round(row['oWAR'] + row['포지션 RAA'] / row['RPW'], 2), axis=1)

    bat = bat.loc[:, ~bat.columns.duplicated()]

    bat = bat.set_index(keys='ID')

    bat = bat.sort_values(by='WAR*', ascending=False)

    bat['Rank'] = range(1, len(bat) + 1)

    return bat

# 투수 데이터 로드 함수
def load_statiz_pit(driver):
    driver.get("https://statiz.sporki.com/stats/?m=main&m2=pitching&m3=default&so=WAR&ob=DESC&year=2024&sy=&ey=&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=500&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2=")
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

    pit['WAR'] = pit['WAR'].astype(float)

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

# 자유계약 선수 업데이트 함수
def update_fa(bat, pit, player_id, player_activation, doc):
    bat_fa = bat[bat.index.to_series().apply(lambda x: isactive(x, player_id, player_activation)) == False]
    pit_fa = pit[pit.index.to_series().apply(lambda x: isactive(x, player_id, player_activation)) == False]

    total_fa = pd.concat([bat_fa.assign(**{'WAR/WAR*': bat_fa['WAR*']})[["Name", "Team", "POS", "WAR/WAR*"]],
                          pit_fa.assign(**{'WAR/WAR*': pit_fa['WAR']})[["Name", "Team", "POS", "WAR/WAR*"]]])

    bat_fa = bat_fa[["Rank", "Name", "Team", "POS", "WAR*", "AVG", "OBP", "SLG", "OPS", "wRC+", "G", "H", "HR", "RBI", "SB"]]
    pit_fa = pit_fa[["Rank", "Name", "Team", "WAR", "G", "GS", "IP", "ERA", "FIP", "WHIP"]]

    bat_fa = bat_fa.sort_values(by='WAR*', ascending=False)
    pit_fa = pit_fa.sort_values(by='WAR', ascending=False)
    total_fa = total_fa.sort_values(by='WAR/WAR*', ascending=False)

    bat_fa['Rank'] = range(1, len(bat_fa) + 1)
    pit_fa['Rank'] = range(1, len(pit_fa) + 1)

    total_fa.insert(0, 'Rank', 0)
    total_fa['Rank'] = range(1, len(total_fa) + 1)

    kst = pytz.timezone('Asia/Seoul')
    now = str(datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S'))
    update = pd.DataFrame([["last update", now], ["타자", str(len(bat_fa)) + " 명"], ["투수", str(len(pit_fa)) + " 명"], ["전체", str(len(total_fa)) + " 명"]])

    doc.worksheet("bat").update([bat_fa.columns.values.tolist()] + bat_fa.values.tolist())
    doc.worksheet("pit").update([pit_fa.columns.values.tolist()] + pit_fa.values.tolist())
    doc.worksheet("total").update([total_fa.columns.values.tolist()] + total_fa.values.tolist())
    doc.worksheet("update").update(update.values.tolist())

    doc.worksheet("bat").clear_basic_filter()
    doc.worksheet("pit").clear_basic_filter()
    doc.worksheet("total").clear_basic_filter()

    doc.worksheet("bat").set_basic_filter("B1:" + right_bottom(doc, "bat"))
    doc.worksheet("pit").set_basic_filter("B1:" + right_bottom(doc, "pit"))
    doc.worksheet("total").set_basic_filter("B1:" + right_bottom(doc, "total"))

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
                bat_war = float(bat.loc[ID, "WAR*"]) if ID in bat.index else 0
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
def update_db(player_name, player_id, current_war, bat, pit):
    for team in player_id.index:
        data = pd.DataFrame({"ID": player_id.loc[team], "Name": player_name.loc[team]})
        data = data[np.isnan(pd.to_numeric(data["ID"])) == False]

        db = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name=team)
        db = db.map(lambda x: f'{x:.2f}' if isinstance(x, (int, float, np.number)) and not pd.isnull(x) else x)
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

        today = today.map(lambda x: f'{x:.2f}')

        data = data.join(today)

        data = data.replace(np.nan, "")

        save_sheet(data.reset_index(), team)

        data_diff = pd.DataFrame({"ID": player_id.loc[team], "Name": player_name.loc[team]})
        data_diff = data_diff[np.isnan(pd.to_numeric(data_diff["ID"])) == False]

        db_diff = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name=team + " 변화")
        db_diff = db_diff.map(lambda x: f'{x:.2f}' if isinstance(x, (int, float, np.number)) and not pd.isnull(x) else x)
        db_diff = db_diff.set_index("index")
        db_diff.index.name = None
        db_diff = db_diff.drop(["ID", "Name"], axis=1)

        if date in db_diff.columns:
            db_diff = db_diff.drop(date, axis=1)

        data_diff = data_diff.join(db_diff)

        data["gap"] = data.apply(lambda x: f'{float(x.iloc[-1]) - (0 if x.iloc[-2] == "" else float(x.iloc[-2])):.2f}', axis=1)
        gap = data.iloc[:, [-1]]
        gap = gap.rename(columns={"gap": date})

        data_diff = data_diff.join(gap)
        data_diff = data_diff.replace(np.nan, "")

        save_sheet(data_diff.reset_index(), team + " 변화")

    db = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="teams")
    db = db.map(lambda x: f'{x:.2f}' if isinstance(x, (int, float, np.number)) and not pd.isnull(x) else x)
    db = db.set_index("팀")

    date = get_date()

    if date in db.columns:
        db = db.drop(date, axis=1)

    bat_fa = bat[bat.index.to_series().apply(lambda x: isactive(x, player_id, player_activation)) == False]
    pit_fa = pit[pit.index.to_series().apply(lambda x: isactive(x, player_id, player_activation)) == False]

    OF_fa = bat_fa[bat_fa["POS"].isin(["LF", "CR", "RF"])]
    IF_fa = bat_fa[bat_fa["POS"].isin(["1B", "2B", "3B", "SS"])]
    C_fa = bat_fa[bat_fa["POS"].isin(["C"])]

    else_fa = pd.concat([OF_fa.iloc[6:].assign(**{'WAR/WAR*': OF_fa['WAR*']})[["Name", "Team", "POS", "WAR/WAR*"]],
                         IF_fa.iloc[8:].assign(**{'WAR/WAR*': IF_fa['WAR*']})[["Name", "Team", "POS", "WAR/WAR*"]],
                         C_fa.iloc[3:].assign(**{'WAR/WAR*': C_fa['WAR*']})[["Name", "Team", "POS", "WAR/WAR*"]],
                         pit_fa.iloc[12:].assign(**{'WAR/WAR*': pit_fa['WAR']})[["Name", "Team", "POS", "WAR/WAR*"]]])
    else_fa = else_fa.sort_values(by='WAR/WAR*', ascending=False)

    war_fa = OF_fa.iloc[0:5]["WAR*"].sum() + IF_fa.iloc[0:7]["WAR*"].sum() + C_fa.iloc[0:2]["WAR*"].sum() + pit_fa.iloc[0:11]["WAR"].sum() + else_fa.iloc[0:8]["WAR/WAR*"].sum()

    today = pd.DataFrame(current_war.sum(axis=1), columns=[date])
    today = today.map(lambda x: f'{x:.2f}')

    today.loc["퐈"] = [f'{war_fa:.2f}']

    db = db.join(today)

    save_sheet(db.reset_index(), "teams")
    
    db_diff = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="teams diff")
    db_diff = db_diff.map(lambda x: f'{x:.2f}' if isinstance(x, (int, float, np.number)) and not pd.isnull(x) else x)
    db_diff = db_diff.set_index("팀")

    if date in db_diff.columns:
        db_diff = db_diff.drop(date, axis=1)

    db["gap"] = db.apply(lambda x: f'{float(x.iloc[-1]) - (0 if x.iloc[-2] == "" else float(x.iloc[-2])):.2f}', axis=1)
    gap = db.iloc[:, [-1]]
    gap = gap.rename(columns={"gap": date})

    db_diff = db_diff.join(gap)
    db_diff = db_diff.replace(np.nan, "")

    save_sheet(db_diff.reset_index(), "teams diff")

# 경기 결과 업데이트 함수
def update_games(driver):
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

    driver.get("https://statiz.sporki.com/schedule/?year=2024&month=" + str(today_month))
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
                else:
                    games[i] = [game[0], "업데이트 전", game[3]]
                    game_number += 1
            elif len(game) == 3 and game[1] == "우천취소":
                games[i] = [game[0], "우천취소", game[2]]
            elif len(game) == 3:
                games[i] = [game[0], "업데이트 전", game[2]]
                game_number += 1
        games.insert(0, ["업데이트 : " + str(updated_number) + "/" + str(game_number) + "경기", "", ""])

    games.insert(0, ["경기 날짜 : " + str(today), "", ""])

    return pd.DataFrame(games)

# 서비스 데이터 업데이트 함수
def update_service(doc_service, player_name, player_id, live_war, games):
    db = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="teams")
    db = db.iloc[:, [0, -2, -1]]
    date = db.columns[-1]
    db["변동"] = db.apply(lambda x: f'{float(x.iloc[-1]) - float(0 if x.iloc[-2] == "" else float(x.iloc[-2])):.2f}', axis=1)
    db = db.iloc[:, [0, 2, 3]]
    db.columns = ["팀", "WAR", "변동"]
    db = db.iloc[::-1]
    temp_fa = db[db["팀"] == "퐈"]
    db = db[db["팀"] != "퐈"]
    db = db.sort_values(by='WAR', ascending=False)
    db.insert(0, '순위', 0)
    db['순위'] = range(1, len(db) + 1)
    temp_fa.insert(0, '순위', 0)
    temp_fa['순위'] = [""]
    db = pd.concat([db, temp_fa], ignore_index=True)
    db = db.sort_values(by='WAR', ascending=False)
    db['WAR'] = db.apply(lambda x: f'{x["WAR"]:.2f}', axis=1)

    doc_service.worksheet("팀 순위").update([db.columns.values.tolist()] + db.values.tolist())
    
    db_diff = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name="teams diff").set_index("팀").iloc[:-1]
    
    mean = np.nanmean(db_diff.values)
    std = np.nanstd(db_diff.values)

    z_diff = db.set_index("팀")[["변동"]].map(lambda x: (float(x) - mean) / std)
    norm = Normalize(vmin=-3, vmax=3)
    sm = ScalarMappable(cmap='coolwarm', norm=norm)

    colored_diff = z_diff.map(lambda x: sm.to_rgba(x, bytes=True)[:3])
    colored_diff = colored_diff.map(lambda x: [color / 255 for color in x])

    sheet = doc_service.worksheet("팀 순위")
    batch = batch_updater(sheet.spreadsheet)
        
    for i in range(db.shape[0]):
        cell = f"D{i+2}"
        fmt = cellFormat(backgroundColor=color(colored_diff.iloc[i, 0][0], colored_diff.iloc[i, 0][1], colored_diff.iloc[i, 0][2]))
        if db.iloc[i]["팀"] == "퐈":
            fmt = cellFormat(backgroundColor=color(1, 1, 1))
        batch.format_cell_range(sheet, cell, fmt)
        
    batch.execute()

    dfs = []

    for team in player_id.index:
        db = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name=team)
        db = db.iloc[:, [0, 2, -2, -1]]
        db = db.fillna("")
        db["변동"] = db.apply(lambda x: f'{float(x.iloc[-1]) - (0 if x.iloc[-2] == "" else float(x.iloc[-2])):.2f}', axis=1)
        db = db.iloc[:, [0, 1, 3, 4]]
        db.columns = ["드래프트", "이름", "WAR", "변동"]
        db["WAR"] = db.apply(lambda x: float(x["WAR"]), axis=1)
        db = db.sort_values(by='WAR', ascending=False)
        db["WAR"] = db.apply(lambda x: f'{x["WAR"]:.2f}', axis=1)
        db.insert(0, '순위', 0)
        db['순위'] = range(1, len(db) + 1)

        doc_service.worksheet("팀 " + team).update([db.columns.values.tolist()] + db.values.tolist())
        
        db_diff = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name=team + " 변화").iloc[:, 3:]

        mean = np.nanmean(db_diff.values)
        std = np.nanstd(db_diff.values)

        z_diff = db.set_index("드래프트")[["변동"]].map(lambda x: (float(x) - mean) / std)
        norm = Normalize(vmin=-3, vmax=3)
        sm = ScalarMappable(cmap='coolwarm', norm=norm)

        colored_diff = z_diff.map(lambda x: sm.to_rgba(x, bytes=True)[:3])
        colored_diff = colored_diff.map(lambda x: [color / 255 for color in x])

        sheet = doc_service.worksheet("팀 " + team)
        batch = batch_updater(sheet.spreadsheet)
            
        for i in range(db.shape[0]):
            cell = f"E{i+2}"
            fmt = cellFormat(backgroundColor=color(colored_diff.iloc[i, 0][0], colored_diff.iloc[i, 0][1], colored_diff.iloc[i, 0][2]))
            batch.format_cell_range(sheet, cell, fmt)
            
        batch.execute()

        db_diff = pd.read_excel("/home/imdw/untatiz/db/untatiz_db.xlsx", sheet_name=team + " 변화")
        n = min(7, len(db_diff.columns) - 3)
        db_diff = db_diff.iloc[:, [2] + list(range(-n, 0))]
        db_diff.insert(0, '팀', "팀 " + team)
        dates = db_diff.columns[2:]
        dfs_team = []

        for date in dates:
            df_temp = db_diff[["팀", "Name", date]].copy()
            df_temp.columns = ['팀', '이름', 'WAR 변동']
            df_temp.insert(1, "날짜", date)
            dfs_team.append(df_temp)

        dfs.extend(dfs_team)

    diff = pd.concat(dfs, ignore_index=True)

    diff['WAR 변동'] = diff.apply(lambda x: float(x["WAR 변동"]), axis=1)
    GOAT = diff.copy()
    GOAT = GOAT[GOAT['WAR 변동'] > 0]
    GOAT = GOAT.sort_values(by='WAR 변동', ascending=False)
    GOAT['WAR 변동'] = GOAT.apply(lambda x: f'{x["WAR 변동"]:.2f}', axis=1)
    GOAT.insert(0, '순위', 0)
    GOAT['순위'] = range(1, len(GOAT) + 1)
    BOAT = diff.copy()
    BOAT = BOAT[BOAT['WAR 변동'] < 0]
    BOAT = BOAT.sort_values(by='WAR 변동', ascending=True)
    BOAT['WAR 변동'] = BOAT.apply(lambda x: f'{x["WAR 변동"]:.2f}', axis=1)
    BOAT.insert(0, '순위', 0)
    BOAT['순위'] = range(1, len(BOAT) + 1)

    doc_service.worksheet("GOAT").clear()
    doc_service.worksheet("BOAT").clear()

    doc_service.worksheet("GOAT").update([GOAT.columns.values.tolist()] + GOAT.values.tolist())
    doc_service.worksheet("BOAT").update([BOAT.columns.values.tolist()] + BOAT.values.tolist())
    
    draft = pd.DataFrame()

    for i in range(max(len(player_name), len(live_war))):
        if i < len(player_name):
            draft = pd.concat([draft, player_name.iloc[[i],0:28]], ignore_index=True)
        if i < len(live_war):
            draft = pd.concat([draft, live_war.iloc[[i],0:28]], ignore_index=True)

    draft.index = ["팀 언", "", "팀 앙", "", "팀 삼", "", "팀 준", "", "팀 역", "", "팀 뚝", "", "팀 홍", "", "팀 엉", "", "팀 코", "", "팀 옥", ""]

    draft = draft.map(lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else x)

    doc_service.worksheet("드래프트").update([[""] + list(draft.columns)] + [[draft.index[i]] + list(draft.iloc[i]) for i in range(len(draft))])

    df = live_war.iloc[:,0:28]

    z_df = df.copy()

    for column in df.columns:
        col_mean = df[column].mean()
        col_std = df[column].std()
        z_df[column] = (df[column] - col_mean) / col_std

    norm = Normalize(vmin=-3, vmax=3)
    sm = ScalarMappable(cmap='coolwarm', norm=norm)

    colored_df = z_df.map(lambda x: sm.to_rgba(x, bytes=True)[:3])
    colored_df = colored_df.map(lambda x: [color / 255 for color in x])

    sheet = doc_service.worksheet("드래프트")
    batch = batch_updater(sheet.spreadsheet)

    for row in range(1, 11):
        for col in range(1, 29):
            col_letter = ""
            n = col + 1
            while n > 0:
                n, remainder = divmod(n - 1, 26)
                col_letter = chr(65 + remainder) + col_letter
            cell = f"{col_letter}{2*row+1}"
            fmt = cellFormat(backgroundColor=color(colored_df.iloc[row-1,col-1][0], colored_df.iloc[row-1,col-1][1], colored_df.iloc[row-1,col-1][2]))
            batch.format_cell_range(sheet, cell, fmt)

    batch.execute()

    kst = pytz.timezone('Asia/Seoul')
    now = str(datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S'))
    update = pd.DataFrame([["업데이트 시간", now]])

    today = get_date()
    if len(GOAT[GOAT["날짜"] == today]) > 0 or len(BOAT[BOAT["날짜"] == today]) > 0:
        war_update = pd.DataFrame([["WAR 업데이트", "업데이트 완료"]])
    elif games.iloc[-1, 0] == '오늘은 경기가 없습니다.':
        war_update = pd.DataFrame()
    else:
        war_update = pd.DataFrame([["WAR 업데이트", "업데이트 전"]])

    update = pd.concat([update, games, war_update], axis=0)
    update = update.fillna("")

    doc_service.worksheet("업데이트").clear()
    doc_service.worksheet("업데이트").update(update.values.tolist())

# DB 백업 함수
def backup_db():
    kst = pytz.timezone('Asia/Seoul')
    now = str(datetime.now(kst).strftime('%Y%m%d%H%M%S'))
    backup_dir = '/home/imdw/untatiz/backup/'
    shutil.copy2("/home/imdw/untatiz/db/untatiz_db.xlsx", f'{backup_dir}{now}.xlsx')

# 시트 저장 함수
def save_sheet(df, sheet_name):
    book = load_workbook("/home/imdw/untatiz/db/untatiz_db.xlsx")
    with pd.ExcelWriter("/home/imdw/untatiz/db/untatiz_db.xlsx", engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        writer.workbook = book
        df.to_excel(writer, sheet_name=sheet_name, index=False)

# 트랜잭션 업데이트 함수
def update_transaction(doc_transaction):
    transaction = doc_transaction.worksheet("transaction").get_all_values()
    transaction = pd.DataFrame(transaction[1:], columns=transaction[0])
    transaction["WAR"] = transaction["WAR"].astype(float)
    transaction["WAR"] = transaction["WAR"].apply(lambda x: f"{x:.2f}")
    transaction = transaction.astype(str)
    doc_transaction.worksheet("transaction").update([transaction.columns.values.tolist()] + transaction.values.tolist())
    save_sheet(transaction, "transaction")

# 시간 상태 반환 함수
def get_time_status():
    now = datetime.now()
    hour = now.hour
    minute = now.minute
    second = now.second

    if hour == 23:
        if 0 <= minute < 30:
            return 0
        elif 30 <= minute < 50:
            return 1
        else:
            return 2
    else:
        if 0 <= minute < 30:
            return 0
        else:
            return 1

# WAR 상태 반환 함수
def get_war_status(doc_service):
    war_update = doc_service.worksheet("업데이트").get_all_values()
    if war_update[-1][1] == "업데이트 완료": return 1
    elif war_update[-1][1] == "업데이트 전": return 0

# 팀 상태 반환 함수
def get_team_status(driver):
    update = update_games(driver).values.tolist()
    if update[1][0] == '오늘은 경기가 없습니다.': return 0
    updated = int(re.search(r'(\d+)/(\d+)', update[1][0]).group(1))
    total = int(re.search(r'(\d+)/(\d+)', update[1][0]).group(2))
    if updated == total: return 1
    else: return 0

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

# 크롬 옵션 설정
chrome_options = wd.ChromeOptions()

# 구글 스프레드시트 로드
doc_service = load_gspread("/home/imdw/untatiz/api/untatiz-75f1c6db233b.json", "https://docs.google.com/spreadsheets/d/1dBXiLWcMnTToACuOySMume3xzkruL4iL_zLOYwtSaQY/edit?usp=sharing")
doc_fa = load_gspread("/home/imdw/untatiz/api/untatiz-75f1c6db233b.json", "https://docs.google.com/spreadsheets/d/1ff2L7MFQbAWBtscwoQr1Y8UdY34f5Lk1ajR5jZu2rh8/edit?usp=sharing")
doc_transaction = load_gspread("/home/imdw/untatiz/api/untatiz-75f1c6db233b.json", "https://docs.google.com/spreadsheets/d/1mOni5ojcYOU7XCMHCZUuMzb4qZEcHH73zUqrw1J75GU/edit?usp=sharing")

# 시간 및 팀 상태 초기화
time_previous = 0
time_current = -1

team_previous = set()
team_current = set([0])

while(True):
    try:
        while(True):
            driver = wd.Chrome(service=Service(), options=chrome_options)
            
            time_previous = time_current
            time_current = get_time_status()
            
            team_previous = team_current
            team_current = updated_teams(driver)
            
            war_status = get_war_status(doc_service)
            team_status = get_team_status(driver)
            
            driver.quit()
            
            if time_current == 2:
                update_status = 0
                print("update skipped at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
                continue
            if war_status == 0 and team_status == 1: break 
            if time_previous != time_current: break
            if team_previous != team_current: break
            
            print("update skipped at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
            time.sleep(60)
        
        driver = wd.Chrome(service=Service(), options=chrome_options)
        
        print("update started at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        
        update_transaction(doc_transaction)
        #update_id(driver)
        player_name, player_id, player_activation, war_basis, transaction = load_data()
        games = update_games(driver)
        bat = load_statiz_bat(driver)
        pit = load_statiz_pit(driver)
        live_war, current_war = get_war(bat, pit, player_id, player_activation, war_basis)
        update_db(player_name, player_id, current_war, bat, pit)
        update_service(doc_service, player_name, player_id, live_war, games)
        update_fa(bat, pit, player_id, player_activation, doc_fa)
        backup_db()
        
        print("update finished at : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        
        driver.quit()
        
        time.sleep(60)
        
    except:
        time.sleep(10)