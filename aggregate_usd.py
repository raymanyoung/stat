# -*- coding: utf-8 -*

import sqlite3
import requests 
import json
import time
import datetime
import math 

INTERVAL_SECONDS = 3600 * 4
DATA_INDEX_AMOUNT = 2
DATA_INDEX_PRICE = 3

def init_database():
    conn = sqlite3.connect('otc.db')
    print("db connected")
    c = conn.cursor()

    table_name = 'AGGREGATION'

    # c.execute("drop table AGGREGATION")

    c.execute("SELECT * FROM sqlite_master WHERE type = 'table' AND name = '{0}'".format(table_name)); 

    if c.fetchone() == None:
        # init tables
        c.execute('''CREATE TABLE  AGGREGATION
           (
           ID INTEGER PRIMARY KEY  AUTOINCREMENT,
           TS             LONG     NOT NULL,
           OTC_OPEN      FLOAT    NOT NULL,
           OTC_HIGH      FLOAT    NOT NULL,
           OTC_LOW      FLOAT    NOT NULL,
           OTC_CLOSE      FLOAT    NOT NULL,
           USD_START      FLOAT    NOT NULL,
           USD_HIGH      FLOAT    NOT NULL,
           USD_LOW      FLOAT    NOT NULL,
           USD_CLOSE      FLOAT    NOT NULL
           );
           ''')

        conn.commit()
        print("{0} created".format(table_name))
        
    return conn

def load_last_timestamp(conn):
    c = conn.cursor()
    c.execute("Select max(ts) from AGGREGATION");
    result = c.fetchone()
    print("====== last ======" , result[0])
    if result[0] is None:
        return 0
    return result[0];

# 获取需要处理的原始数据
# 返回值：
#   -1: 原始表没有数据
#    0: 原始表没有待处理的数据
#   >0: 需要处理数据的起始时间戳（如果该区间内不完整则返回0）
def find_first_ts_in_history(conn, base_ts):
    c = conn.cursor()
    c.execute("Select min(ts) from HISTORY where ts >= {0}".format(base_ts));
    minTs = c.fetchone()
    print("====== min in history ======" , minTs[0], base_ts)
    if minTs[0] is None:
        return 0

    c.execute("select count(*) from HISTORY where ts >= {0}".format(base_ts + INTERVAL_SECONDS))
    remainingTs = c.fetchone()
    if remainingTs[0] == 0:
        return 0

    return minTs[0]

def calc_price(txs, base_ts):
    start = 0
    high = 0
    low = 99999
    close = 0
    for x in txs:
        print(x)
        price = x[1]
        if start == 0:
            start = price
        if high < price:
            high = price
        if low > price:
            low = price
        close = price

    return { 'ts': base_ts, 'open': start, 'high': high, 'low': low, 'close': close }

def insert_data(conn, otc_price, usd_price):
    print("inserting data: ");
    print(otc_price);
    print(usd_price);
    c = conn.cursor()
    c.execute("Insert into AGGREGATION (TS, OTC_OPEN,OTC_HIGH,OTC_LOW,OTC_CLOSE,USD_START,USD_HIGH,USD_LOW,USD_CLOSE) \
        values ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7},{8})"\
        .format(otc_price['ts'], otc_price['open'], otc_price['high'], otc_price['low'], otc_price['close'], usd_price['open'], usd_price['high'], usd_price['low'], usd_price['close']));
    conn.commit();    


def try_aggregate(conn):
    last_ts = load_last_timestamp(conn) + INTERVAL_SECONDS;
    start_to_process = find_first_ts_in_history(conn, last_ts)
    if start_to_process <= 0:
        return -1

    base_ts = math.floor(start_to_process / INTERVAL_SECONDS) * INTERVAL_SECONDS

    print("loading data from {0}-{1} in OTC".format(base_ts, base_ts + INTERVAL_SECONDS))
    c = conn.cursor()
    c.execute("select TS, Best_Price from HISTORY where ts >= {0} and ts < {1} order by ts".format(base_ts, base_ts + INTERVAL_SECONDS)); 

    result = c.fetchall()
    
    otc_price = calc_price(result, base_ts)

    print("loading data from {0}-{1} in USD".format(base_ts, base_ts + INTERVAL_SECONDS))
    c = conn.cursor()
    c.execute("select ts, price from USD_RMB where ts >= {0} and ts < {1} order by ts".format(base_ts, base_ts + INTERVAL_SECONDS)); 

    usd_result = c.fetchall()
    
    usd_price = calc_price(usd_result, base_ts)

    insert_data(conn, otc_price, usd_price)

    return 0

c = init_database();
while(True):
    print(datetime.datetime.now(), " -- loading from data")
    try:
        result = try_aggregate(c)
        if result < 0:
            print("no more data, waiting for 10 sec");
            time.sleep(10)
    except Exception as e:
        print("error occurred", str(e))
        time.sleep(10)
