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
    def processData(self):
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

    def do_GET(self):
        query = parse.urlparse(self.path).query
        if query != 'key=uJfi372jdfk':
            return

        content = self.processData()
        
        self.send_response(200)
        self.send_header("Content-Type","text/html")
        self.send_header("Content-Length",str(len(content)))
        self.end_headers()
        self.wfile.write(bytes(content, "utf-8"))


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
