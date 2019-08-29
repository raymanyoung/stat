import sqlite3
import requests 
import json
import time
import datetime
import math 

INTERVAL_MILLISECONDS = 600000
DATA_INDEX_AMOUNT = 2
DATA_INDEX_PRICE = 3

def init_database():
    conn = sqlite3.connect('example.db')
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
        # check whether there is existing entries in the table
        # c.execute("Select min(ts) from HISTORY");
        # result1 = c.fetchone()
        # if result1[0] is None:
        #     return -1
        # else:
        #     return 0  # this means there is 

    c.execute("select count(*) from HISTORY where ts >= {0}".format(base_ts + INTERVAL_MILLISECONDS))
    remainingTs = c.fetchone()
    if remainingTs[0] == 0:
        return 0

    return minTs[0]

def insert_sdv(conn, sdv):
    print("inserting data: ");
    print(sdv);
    c = conn.cursor()
    c.execute("Insert into AGGREGATION (TS, VWAP, VWSD, VOLUME) values ({0}, {1}, {2}, {3})".format(sdv['ts'], sdv['ave_price'], sdv['sdv'], sdv['amount']));
    conn.commit();

def calc_sdv(txs, base_ts):
    total = 0
    amount = 0
    for x in txs:
        print(x)
        total += x[DATA_INDEX_AMOUNT] * x[DATA_INDEX_PRICE]
        amount += x[DATA_INDEX_AMOUNT]

    ave = total / amount

    sdv = 0
    for x in txs:
        sdv += math.pow((x[DATA_INDEX_PRICE] - ave), 2) * x[DATA_INDEX_AMOUNT]

    sdv = math.sqrt(sdv / amount)
    return { 'ts': base_ts, 'ave_price': ave, 'amount': amount, 'sdv': sdv }

def try_aggregate(conn):
    last_ts = load_last_timestamp(conn) + INTERVAL_MILLISECONDS;
    start_to_process = find_first_ts_in_history(conn, last_ts)
    if start_to_process <= 0:
        return -1

    base_ts = math.floor(start_to_process / INTERVAL_MILLISECONDS) * INTERVAL_MILLISECONDS

    print("loading data from {0}-{1}".format(base_ts, base_ts + INTERVAL_MILLISECONDS))
    c = conn.cursor()
    c.execute("select * from HISTORY where ts >= {0} and ts < {1}".format(base_ts, base_ts + INTERVAL_MILLISECONDS)); 

    result = c.fetchall()
    print(result)
    
    sdv = calc_sdv(result, base_ts)

    insert_sdv(conn, sdv)
    return 0

c = init_database();
while(True):
    print(datetime.datetime.now(), " -- loading from data")
    result = try_aggregate(c)
    if result < 0:
        print("no more data, waiting for 10 sec");
        time.sleep(10)

