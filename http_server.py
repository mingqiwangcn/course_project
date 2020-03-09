import http.server as BaseHTTPServer
import sys
import json

class SimpleHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_POST(self):
        content_len = int(self.headers.get('Content-Length'))
        post_body = self.rfile.read(content_len)
        data = json.loads(post_body.decode())
        new_data = {'msg':'hello'}
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(new_data).encode())

def main(HandlerClass=SimpleHTTPRequestHandler,
         ServerClass=BaseHTTPServer.HTTPServer,
         protocol="HTTP/1.0"):
    if sys.argv[2:]:
        port = int(sys.argv[2])
    else:
        port = 8080
    server_address = ('', port)

    HandlerClass.protocol_version = protocol
    httpd = BaseHTTPServer.HTTPServer(server_address, HandlerClass)

    sa = httpd.socket.getsockname()
    print("Serving HTTP on port %d" % port)
    httpd.serve_forever()

if __name__ == '__main__':
    main()

