import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from tenacity import retry
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from dotenv import load_dotenv

"""
環境変数取得
"""
load_dotenv()
# LINE
LINE_TOKEN = os.environ["LINE_TOKEN"]
LINE_TOKEN_TEST = os.environ["LINE_TOKEN_TEST"]
LINE_API = os.environ["LINE_API"]

# GOOGLE
SPREADSHEET_KEY = os.environ["SPREADSHEET_KEY"]
GDRIVE_FOLDER_PATH = os.environ["GDRIVE_FOLDER_PATH"]
SPREADSHEET_FILE_PATH = os.environ["SPREADSHEET_FILE_PATH"]
ACCOUNT_KEY_PATH = os.environ["ACCOUNT_KEY_PATH"]

# 検索条件適用URL
SEARCH_URL = os.environ["SEARCH_URL"]
SEARCH_WORD1 = os.environ["SEARCH_WORD1"]
SEARCH_WORD2 = os.environ["SEARCH_WORD2"]
FAVORITE_LIST = os.environ["FAVORITE_LIST"].split(',')
DROP_LIST = os.environ["DROP_LIST"].split(',')

# 日付取得
today = datetime.date.today()
yesterday = today + relativedelta(days=-1)
today = str(today)
today = (today.replace('-',''))
yesterday = str(yesterday)
yesterday = (yesterday.replace('-',''))

"""
サイトから取得したデータをCSVファイルに格納
"""


def create_data_list():
    print("* create csv file start *")
    result = scraping()
    all_data = result[0]
    line_msg_favorite_list = result[1]
    notice_flg = result[2]

    print('----------all_data----------')
    print(all_data)
    print('----------------------------')
    return all_data, line_msg_favorite_list, notice_flg


@retry()
def get_html(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    return soup

def scraping():
    # 検索条件適用URL
    base_url =SEARCH_URL

    all_data = []
    max_page = 1

    line_msg_favorite_list = []

    for page in range(1, max_page+1):
        # define url
        url = base_url.format(page)

        # get html
        soup = get_html(url)

        # extract all items
        items = soup.findAll("div", {"class": "cassetteitem"})
        print("data : page", page, "items", len(items))
        print("DROP_LIST:")
        print(DROP_LIST)

        # process each item
        for item in items:
            stations = item.findAll("div", {"class": "cassetteitem_detail-text"})

            address = item.find("li", {"class": "cassetteitem_detail-col1"}).getText().strip()
            # アドレスにSEARCH_WORDが含まれるデータのみ格納
            search_word1 = SEARCH_WORD1
            search_word2 = SEARCH_WORD2
            if search_word1 in address or search_word2 in address:
                # process each station
                for station in stations:
                    # define variable
                    base_data = {}

                    base_data["新着"] = ""
                    # collect base information
                    rent_name = item.find("div", {"class": "cassetteitem_content-title"}).getText().strip()
                    base_data["名称"] = rent_name
                    base_data["アドレス"] = address
                    base_data["築年数"] = item.find("li", {"class": "cassetteitem_detail-col3"}).findAll("div")[0].getText().strip()
                    base_data["構造"] = item.find("li", {"class": "cassetteitem_detail-col3"}).findAll("div")[1].getText().strip()

                    # process for each room
                    tbodys = item.find("table", {"class": "cassetteitem_other"}).findAll("tbody")

                    for tbody in tbodys:
                        data = base_data.copy()

                        newClass = tbody.find("td", {"class": "cassetteitem_other-checkbox--newarrival"})
                        if newClass:
                            data["新着"] = "new"
                        else:
                            data["新着"] = ""

                        data["階数"] = tbody.findAll("td")[2].getText().strip()
                        data["URL"] = "https://suumo.jp" + tbody.findAll("td")[8].find("a").get("href")

                        data["家賃"] = tbody.findAll("td")[3].findAll("li")[0].getText().strip()
                        data["管理費"] = tbody.findAll("td")[3].findAll("li")[1].getText().strip()

                        data["面積"] = tbody.findAll("td")[5].findAll("li")[1].getText().strip()
                        data["間取り"] = tbody.findAll("td")[5].findAll("li")[0].getText().strip()

                        data["敷金"] = tbody.findAll("td")[4].findAll("li")[0].getText().strip()
                        data["礼金"] = tbody.findAll("td")[4].findAll("li")[1].getText().strip()

                        # 除外する物件はデータに入れない
                        if not rent_name in DROP_LIST:
                            all_data.append(data)

                        # お気に入り物件はLINEにメッセージ
                        if rent_name in FAVORITE_LIST:
                            line_msg_favorite_list.append(rent_name + "\n")

    notice_flg = 0
    line_msg_favorite_list = set(line_msg_favorite_list)
    print('notice_flg: ' + str(notice_flg))
    return all_data, line_msg_favorite_list, notice_flg


"""
    # 前回作られたデータ取得
    worksheet = open_spreadsheet()
    df = pd.DataFrame(worksheet.get_all_values())

    # 昨日の物件名称と一致する場合は個人、しない場合はグループ通知
    exists = rent_name in df.values
    if exists:
        notice_flg = 0 #個人
    else:
    notice_flg = 1 #グループ
"""


def convert_to_dataframe(all_data):

    # convert to dataframe
    df = pd.DataFrame(all_data)
    df.drop_duplicates(subset=['名称','家賃'], inplace=True) #delete duplication
    df = df.sort_values(by='面積', ascending=False)
    df.index = np.arange(1, len(df)+1) #fix index from 1

    print("* create csv file done *")
    return df


"""
作成したCSVファイルの内容をスプレッドシートにアップする
"""
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

#認証情報設定
credentials = ServiceAccountCredentials.from_json_keyfile_name(ACCOUNT_KEY_PATH, scope)

def open_spreadsheet():
    #Google APIにログイン
    gc = gspread.authorize(credentials)

    #共有設定したスプレッドシートのシート1を開く
    worksheet = gc.open_by_key(SPREADSHEET_KEY).sheet1
    return worksheet

def update_spreadsheet(df):
    print("* update spreadshhet start *")

    worksheet = open_spreadsheet()

    # スプレッドシートの中身を空にする
    worksheet.clear()

    # CSVを書き込み
    set_with_dataframe(worksheet, df)

    print("* update spreadshhet done *")


"""
スプレッドシートが更新されたことをLINEに通知する
"""

def Notify(notice_flg, rent_info_line_msg, line_msg_favorite_list):
    print("* send LINE start *")
    file_path = SPREADSHEET_FILE_PATH
    s = SEARCH_URL
    search_url = s.replace('&page={}', '')
    favorite_list = '\n'.join(line_msg_favorite_list)

    if notice_flg == 1:
        if line_msg_favorite_list:

            print('送信先：グループ（お気に入りあり）')
            send_line_msg ='\n新着情報があります😙🎶\n \n 🗼物件情報\n' + rent_info_line_msg + '\n \n🕯詳細情報リンク:\n' + file_path + '\n \nお気に入り物件に空室があります😆\n' + favorite_list
        else:
            print('送信先：グループ（お気に入りなし）')
            send_line_msg ='\n 🗼本日の物件情報\n' + rent_info_line_msg + '\n \n🕯詳細情報リンク:\n' + file_path
    elif notice_flg == 2:
            print('送信先：個人（該当物件なし）')
            send_line_msg ='\n 😞本日の該当物件はありません\n' + '\n \n🛋検索条件URL：\n' + search_url + '\n \nお気に入り物件があれば下に物件名が表示されます😆\n' + favorite_list
    else:
        print('送信先：個人')
        send_line_msg ='\n 🗼本日の物件情報\n' + rent_info_line_msg + '\n \n🕯詳細情報リンク:\n' + file_path + '\n \n🛋検索条件URL：\n' + search_url + '\n \nお気に入り物件があれば下に物件名が表示されます😆\n' + favorite_list


    send_line_notify(notice_flg, send_line_msg)
    print('----------notification_message----------')
    print(send_line_msg)
    print('----------------------------------------')

    print("* send LINE done *")



def send_line_notify(notice_flg, notification_message):
    """
    LINEに通知する
    """
    if notice_flg == 1:
        line_notify_token = LINE_TOKEN
    else:
        line_notify_token = LINE_TOKEN_TEST

    line_notify_api = LINE_API
    headers = {'Authorization': f'Bearer {line_notify_token}'}
    data = {'message': notification_message}
    requests.post(line_notify_api, headers = headers, data = data)


def main():
    print("****** START ******")

    # データ取得
    result1 = create_data_list()
    all_data = result1[0]
    line_msg_favorite_list = result1[1]
    notice_flg = result1[2]

    #データ有無確認
    df = pd.DataFrame(all_data)
    isEmpty = df.empty

    if not isEmpty:
        #データ変換とスプレッドシートの内容をアップデート
        df = convert_to_dataframe(all_data)
        update_spreadsheet(df)

        df['名称'] = df['名称'].str[:6] + '...'
        df = df.loc[:, ['名称', '家賃', '管理費', '面積']]
        df = df.sort_values(by='面積', ascending=False).head(5)
        df.index = np.arange(1, len(df)+1) #fix index from 1
        rent_info_line_msg = str(df)

        Notify(notice_flg, rent_info_line_msg, line_msg_favorite_list)
    else:
        Notify(notice_flg=2, rent_info_line_msg="", line_msg_favorite_list="")

    print("****** DONE ******")


# 実行
main()
