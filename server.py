import http.server as BaseHTTPServer
import sys
import json

import db_storage as db;

class SimpleHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_POST(self):
        content_len = int(self.headers.get('Content-Length'))
        post_body = self.rfile.read(content_len)
        
        item_keys = json.loads(post_body.decode())
        item_values = db.get(item_keys)

        self.send_response(200)
        self.end_headers()
        item_values_encoded = json.dumps(item_values).encode()
        self.wfile.write(item_values_encoded)

def db_start():
    with open('./server.cfg') as f:
        cfg_info = json.load(f)
    db_path = cfg_info['db_path']
    opts = {}
    db.open(db_path, opts) 

def main(HandlerClass=SimpleHTTPRequestHandler,
         ServerClass=BaseHTTPServer.HTTPServer,
         protocol="HTTP/1.0"):
    
    if sys.argv[2:]:
        port = int(sys.argv[2])
    else:
        port = 8080

    db_start()

    server_address = ('', port)
    HandlerClass.protocol_version = protocol
    httpd = BaseHTTPServer.HTTPServer(server_address, HandlerClass)

    sa = httpd.socket.getsockname()
    print("Serving HTTP on port %d" % port)
    httpd.serve_forever()

if __name__ == '__main__':
    main()

