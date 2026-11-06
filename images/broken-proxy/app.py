"""S3 test proxy that fails every second UploadPartCopy request."""

import http.client
import threading
from itertools import count
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlsplit
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

import bottle
from bottle import HTTPResponse

PART_REQUESTS = count(1)
LOCK = threading.Lock()


@bottle.get("/resolve")
def resolve():
    return "broken-proxy01"


@bottle.route("<:path>", method="GET")
@bottle.route("<:path>", method="POST")
@bottle.route("<:path>", method="PUT")
@bottle.route("<:path>", method="DELETE")
@bottle.route("<:path>", method="HEAD")
def index():
    request = bottle.request
    query = parse_qs(request.query_string, keep_blank_values=True)
    if (
        request.method == "PUT"
        and "uploadId" in query
        and "X-Amz-Copy-Source" in request.headers
    ):
        # Count parts separately so other parallel requests cannot hide a fault.
        with LOCK:
            fail = next(PART_REQUESTS) % 2 == 0
        if fail:
            return HTTPResponse(
                status=500,
                body="<Error><Code>InternalError</Code>"
                "<Message>injected UploadPartCopy failure</Message></Error>",
                headers={"Content-Type": "application/xml"},
            )

    # Forward only to the configured test storage; Host must not choose a destination.
    target = urlsplit("{{ conf.s3.endpoint }}")
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=60)
    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in ("connection", "proxy-connection", "transfer-encoding")
        # WSGI inserts an empty Content-Length for requests without a body.
        and not (key.lower() == "content-length" and not value)
    }
    body = request.body.read()
    try:
        connection.request(
            request.method,
            request.environ["RAW_URI"],
            body=body,
            headers=headers,
        )
        response = connection.getresponse()
        data = response.read()
        response_headers = {
            key: value
            for key, value in response.getheaders()
            if key.lower() not in ("connection", "transfer-encoding")
        }
        return HTTPResponse(status=response.status, body=data, headers=response_headers)
    finally:
        connection.close()


class ThreadedServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class ProxyHandler(WSGIRequestHandler):
    def get_environ(self):
        # Forward proxies receive absolute request targets; WSGI expects a path.
        target = urlsplit(self.path)
        self.path = target.path + ("?" + target.query if target.query else "")
        environ = super().get_environ()
        # PATH_INFO is decoded by WSGI. Forward the original escapes so S3 keys
        # containing literal percent sequences are not decoded a second time.
        environ["RAW_URI"] = self.path
        return environ


bottle.run(
    host="0.0.0.0",
    port=4080,
    server="wsgiref",
    server_class=ThreadedServer,
    handler_class=ProxyHandler,
)
