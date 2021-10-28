import datetime
from dateutil.relativedelta import relativedelta
from tenacity import retry
import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread

from dotenv import load_dotenv

load_dotenv()

"""
環境変数取得
"""
# LINE
LINE_TOKEN = os.environ["LINE_TOKEN"]
LINE_TOKEN_TEST = os.environ["LINE_TOKEN_TEST"]
LINE_API = os.environ["LINE_API"]

# GOOGLE
SPREADSHEET_KEY = os.environ["SPREADSHEET_KEY"]
GDRIVE_FOLDER_PATH = os.environ["GDRIVE_FOLDER_PATH"]
ACCOUNT_KEY_PATH = os.environ["ACCOUNT_KEY_PATH"]

# file path
CSV_FOLDER_PATH = os.environ["CSV_FOLDER_PATH"]

# 検索条件適用URL
SEARCH_URL = os.environ["SEARCH_URL"]
SEARCH_WORD = os.environ["SEARCH_WORD"]
FAVORITE_LIST = os.environ["FAVORITE_LIST"].split(',')

today = datetime.date.today()
yesterday = today + relativedelta(days=-1)
today = str(today)
today = (today.replace('-',''))
yesterday = str(yesterday)
yesterday = (yesterday.replace('-',''))

"""
サイトから取得したデータをCSVファイルに格納
"""


def create_csv_file():
    print("* create csv file start *")
    result = scraping()
    all_data = result[0]
    line_msg_favorite_list = result[1]
    notice_flg = result[2]

    print('----------all_data----------')
    print(all_data)
    print('----------------------------')
    file_name = convert_to_dataframe_and_csv(all_data)
    return file_name, line_msg_favorite_list, notice_flg


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

        # process each item
        for item in items:
            stations = item.findAll("div", {"class": "cassetteitem_detail-text"})

            address = item.find("li", {"class": "cassetteitem_detail-col1"}).getText().strip()
            # アドレスに「千代田区九段」が含まれるデータのみ格納
            search_word = SEARCH_WORD
            if search_word in address:
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

                        all_data.append(data)

                        # serch favorite
                        if rent_name in FAVORITE_LIST:
                            line_msg_favorite_list.append(rent_name + "\n")

                        # 昨日作られたデータと比較
                        file_name_prev = 'datalist' + yesterday + '.csv'
                        df = pd.read_csv(CSV_FOLDER_PATH + file_name_prev, encoding='shift jis')

                        if rent_name in df.columns:
                            notice_flg = 1
                        else:
                            notice_flg = 0

                        """
                        テスト時は、
                        notice_flg を 0 に指定するコードを
                        以下に追加
                        """

                        print('notice_flg: ' + str(notice_flg))
                        return all_data, line_msg_favorite_list, notice_flg


# create file name
def create_file_name():
    file_name = 'datalist' + today + '.csv'
    return file_name


def convert_to_dataframe_and_csv(all_data):

    # convert to dataframe
    df = pd.DataFrame(all_data)
    df.drop_duplicates(subset=['名称','家賃'], inplace=True) #delete duplication
    df.reset_index(drop=True, inplace=True) #fix index

    # convert to csv
    file_name =create_file_name()
    print ("CSV file name:" + file_name)
    df.to_csv(CSV_FOLDER_PATH + file_name, encoding='shift jis')

    print("* create csv file done *")
    return file_name


"""
作成したCSVファイルな内容をスプレッドシートにアップする
"""
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

#認証情報設定
credentials = ServiceAccountCredentials.from_json_keyfile_name(ACCOUNT_KEY_PATH, scope)
SPREADSHEET_KEY = SPREADSHEET_KEY

def update_spreadsheet(file_name):
  print("* update spreadshhet start *")
  #Google APIにログイン
  gc = gspread.authorize(credentials)

  # CSVファイル読み込み
  csv_file_name = CSV_FOLDER_PATH + file_name

  #共有設定したスプレッドシートのシート1を開く
  worksheet = gc.open_by_key(SPREADSHEET_KEY).sheet1

  # スプレッドシートの中身を空にする
  worksheet.clear()

  # CSVを書き込み
  worksheet.update(list(csv.reader(open(csv_file_name, encoding='shift jis'))))

  print("* update spreadshhet done *")


"""
スプレッドシートが更新されたことをLINEに通知する
"""

def Notify(notice_flg, line_msg_favorite_list):
    print("* send LINE start *")
    folder_path = GDRIVE_FOLDER_PATH

    if notice_flg == 1:
        if line_msg_favorite_list:
            favorite_list = '\n'.join(line_msg_favorite_list)

            print('送信先：グループ')
            send_line_msg ='\n 新着の物件情報があります!\n' + folder_path + '\n\nーー以下のお気に入り物件に空室がありますーー\n' + favorite_list
        else:
            print('送信先：グループ')
            send_line_msg ='\n 新着の物件情報があります!\n' + folder_path
    else:
        print('送信先：個人')
        send_line_msg ='\n 本日の物件情報\n' + folder_path

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
    data = {'message': f'message: {notification_message}'}
    requests.post(line_notify_api, headers = headers, data = data)


def main():
  print("****** START ******")

  result = create_csv_file()

  csv_file_name = result[0]
  line_msg_favorite_list = result[1]
  notice_flg = result[2]


  update_spreadsheet(csv_file_name)

  if notice_flg == 1:
    Notify(notice_flg, line_msg_favorite_list)
  else:
    Notify(notice_flg, line_msg_favorite_list)

  print("****** DONE ******")


# 実行
main()
