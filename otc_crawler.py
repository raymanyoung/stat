import sqlite3
import requests 
import json
import time
import datetime


def init_database():
    conn = sqlite3.connect('otc.db')
    print("db connected")
    c = conn.cursor()

    # check table
    table_name = 'History'
    c.execute("SELECT * FROM sqlite_master WHERE type = 'table' AND name = '{0}'".format(table_name)); 

    if c.fetchone() == None:
        # init tables
        c.execute('''CREATE TABLE  History
           (
           ID INTEGER PRIMARY KEY  AUTOINCREMENT,
           TS                 LONG     NOT NULL,
           BEST_PRICE         FLOAT    NOT NULL,
           BEST_PRICE_AMOUNT  FLOAT    NOT NULL,
           AVE_PRICE          FLOAT    NOT NULL,
           TOTAL_AMOUNT       FLOAT    NOT NULL,
           MODE               STRING   NOT NULL
           );
           ''')

        conn.commit()
        print("{0} created".format(table_name))

    return conn


def load_last_timestamp(conn):
    c = conn.cursor()
    c.execute("Select max(ts) from History");
    result = c.fetchone()
    # print("====== last ======" , result[0])
    if result[0] is None:
        return 0
    return result[0]

def process_txs(conn, txs) :
    total_amount = 0
    total_price = 0
    best_price = 0
    best_price_amount = 0

    for x in txs:
        if best_price == 0:
            best_price = x['price']

        total_amount += x['tradeCount']
        total_price += x['tradeCount'] * x['price']

        if best_price == x['price']:
            best_price_amount += x['tradeCount']

    ave = total_price / total_amount

    insert_tx(conn, { 'ts': time.time(), 'total_amount': total_amount, 'ave_price' : ave, 'best_price': best_price, 'best_price_amount': best_price_amount, 'mode': '\"buy\"' })


def insert_tx(conn, tx_dict):
    # print("inserting data: ");
    print(tx_dict);
    c = conn.cursor()
    c.execute("Insert into History (TS, BEST_PRICE, BEST_PRICE_AMOUNT, AVE_PRICE, TOTAL_AMOUNT, MODE) values ({0}, {1}, {2}, {3}, {4}, {5})" \
        .format(tx_dict['ts'], tx_dict['best_price'], tx_dict['best_price_amount'], tx_dict['ave_price'], tx_dict['total_amount'], tx_dict['mode']));
    conn.commit();


def load_test_json():
    file = open('otc_test.json', 'r', encoding='UTF-8')
    content = file.read()
    result = json.loads(content)
    return result

def collect(conn):
    last_ts = load_last_timestamp(conn);
    r = requests.get(url = 'https://otc-api.eiijo.cn/v1/data/trade-market?coinId=2&currency=1&tradeType=buy&currPage=1&payMethod=0&country=37&blockType=general&online=1&range=0&amount=', timeout = 10)

    data = r.json()
    # data = load_test_json()
    if data['code'] != 200:
        return False

    process_txs(conn, data['data'])
    
    return True


c = init_database();
while(True):
    try:
        print(datetime.datetime.now(), " -- loading from exchange")
        success = collect(c)
        if success: 
            time.sleep(300)
        else:
            print("failed to retrieve data")
            time.sleep(10)
    except:
        print("exception happens")
        time.sleep(10) 