import sqlite3
import requests 
import json
import time
import datetime

def init_database():
    conn = sqlite3.connect('example.db')
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
           TS             LONG     NOT NULL,
           AMOUNT         FLOAT    NOT NULL,
           PRICE          FLOAT    NOT NULL
           );
           ''')

        conn.commit()
        print("{0} created".format(table_name))

    table_name = 'AGGREGATION'

    c.execute("SELECT * FROM sqlite_master WHERE type = 'table' AND name = '{0}'".format(table_name)); 

    if c.fetchone() == None:
        # init tables
        c.execute('''CREATE TABLE  AGGREGATION
           (
           ID INTEGER PRIMARY KEY  AUTOINCREMENT,
           TS             LONG     NOT NULL,
           VWAP           FLOAT    NOT NULL,
           VWSD           FLOAT    NOT NULL,
           VOLUME         FLOAT    NOT NULL
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

def process_tx(tx) :
    amount = 0
    total_price = 0
    for sub in tx['data']:
        amount += sub['amount']
        total_price += sub['amount'] * sub['price']

    price = total_price / amount
    result = {'ts': tx['ts'], 'amount': amount, 'price' : price}
    return result;

def load_test_json():
    file = open('test.json', 'r')
    content = file.read()
    result = json.loads(content)
    return result

def insert_tx(conn, tx_dict):
    print("inserting data: ");
    print(tx_dict);
    c = conn.cursor()
    c.execute("Insert into History (TS, AMOUNT, PRICE) values ({0}, {1}, {2})".format(tx_dict['ts'], tx_dict['amount'], tx_dict['price']));
    conn.commit();

def collect(conn):
    last_ts = load_last_timestamp(conn);
    r = requests.get(url = 'https://api.hbdm.com/market/history/trade?symbol=BTC_CW&size=200')

    data = r.json()
    # data = load_test_json()
    if data['status'] != 'ok':
        return False

    length = len(data['data'])
    for index in range(0, length):
        i = length - 1 - index
        d = data['data'][i]
        # print("last ts: {0}, this ts: {1}".format(last_ts, d['ts']))
        if (d['ts'] > last_ts):
            insert_tx(conn, process_tx(d))    

    return True

c = init_database();
while(True):
    print(datetime.datetime.now(), " -- loading from exchange")
    success = collect(c)
    if success: 
        time.sleep(10)
    else:
        print("failed to retrieve data")
        time.sleep(1)

