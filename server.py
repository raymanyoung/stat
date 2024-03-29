# -*- coding: utf-8 -*
import sys
import http.server
# from SimpleHTTPServer import SimpleHTTPRequestHandler
import sqlite3
from datetime import datetime
from urllib import parse

class RequestHandler(http.server.BaseHTTPRequestHandler):
    Page = '''\
            <html>
                <head>
                    <style>
                        table td{{
                            border: solid grey 1px
                        }}

                    </style>
                </head>
            <body>
            {0}
            </body>
            </html>
    '''
    def processBtcStat(self):
        conn = sqlite3.connect('example.db')
        print("db connected")
        c = conn.cursor()

        c.execute("Select ts, vwap, vwsd, volume from AGGREGATION order by ts desc limit 50");
        result = c.fetchall()

        line = "<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td></tr>"
        table = ""
        for data in result:
            datestr = datetime.fromtimestamp(int(data[0])/ 1000) 
            table += line.format(datestr, data[1], data[2], data[3])

        return self.Page.format("<table><tr><td>Time (UTC)</td><td>VWAP</td><td>VWSD</td><td>VOLUME</td></tr>{0}</table>".format(table))

    def processUsdStat(self):
        conn = sqlite3.connect('otc.db')
        print("db connected")
        c = conn.cursor()

        c.execute("Select max(ts) from HISTORY order by ts desc limit 50");
        result = c.fetchone()
        otc_latest_time = datetime.fromtimestamp(int(result[0])) 

        c.execute("Select max(ts) from USD_RMB order by ts desc limit 50");
        result = c.fetchone()
        usd_latest_time = datetime.fromtimestamp(int(result[0])) 

        c.execute("Select ts, OTC_CLOSE, USD_CLOSE from AGGREGATION order by ts desc limit 50");
        result = c.fetchall()


        line = "<tr><td>{0}</td><td>{1}</td><td>{2}</td></tr>"
        table = ""
        for data in result:
            datestr = datetime.fromtimestamp(int(data[0])) 
            table += line.format(datestr, data[1], data[2])

        header_text = "<div>最后获取 OTC 价格时间：{0}  最后获取离岸人民币价格时间：{1} </div>".format(otc_latest_time, usd_latest_time)

        return self.Page.format(header_text + "<table><tr><td>Time</td><td>OTC收盘价</td><td>USD收盘价</td></tr>{0}</table>".format(table))


    def do_btcStat(self):
        content = self.processBtcStat()
        
        self.send_response(200)
        self.send_header("Content-Type","text/html")
        self.send_header("Content-Length",str(len(content)))
        self.end_headers()
        self.wfile.write(bytes(content, "utf-8"))    

    def do_usdStat(self):
        content = self.processUsdStat()
        
        self.send_response(200)
        self.send_header("Content-Type","text/html;charset=utf-8")
        self.send_header("Content-Length",str(len(content)))
        self.end_headers()
        self.wfile.write(bytes(content, "utf-8"))         

    def do_GET(self):
        print("request received")
        query = parse.urlparse(self.path).query
        if query.startswith('key=uJfi372jdfk'):
            self.do_btcStat();
        elif query.startswith('key=j2kdf88D2'):
            self.do_usdStat();

if sys.argv[1:]:
    ip = sys.argv[1]
else:
    ip = ''

serverAddress = (ip, 8192)
server = http.server.HTTPServer(serverAddress, RequestHandler)

try:
    server.serve_forever() 
except KeyboardInterrupt:
    pass
server.server_close()
