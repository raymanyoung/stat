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
    table_name = 'USD_RMB'
    c.execute("SELECT * FROM sqlite_master WHERE type = 'table' AND name = '{0}'".format(table_name)); 

    if c.fetchone() == None:
        # init tables
        c.execute('''CREATE TABLE  USD_RMB
           (
           ID INTEGER PRIMARY KEY  AUTOINCREMENT,
           TS                 LONG     NOT NULL,
           Price              LONG     NOT NULL
           );
           ''')

        conn.commit()
        print("{0} created".format(table_name))

    return conn

def process_price(conn, price):
    # print("inserting data: ");
    c = conn.cursor()
    c.execute("Insert into USD_RMB (TS, Price) values ({0}, {1})".format(time.time(), price));
    conn.commit();

def collect(conn):
    r = requests.get(url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=USDCNH_FOS&sty=FDPBPFB&st=z&token=7bc05d0d4c3c22ef9fca8c2a912d779c')
    data = r.text.split(',')
    price = data[4]

    process_price(conn, price)
    
    return True


c = init_database();
while(True):
    print(datetime.datetime.now(), " -- loading from exchange")
    try:
      success = collect(c)
      if success: 
          time.sleep(300)
      else:
          print("failed to retrieve data")
          time.sleep(5)
    except:
        print("exception happens")
        time.sleep(5) 