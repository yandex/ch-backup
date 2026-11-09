"""Fail the second copy part with HTTP 503, then an embedded HTTP 200 error."""

import http.client
import json
import logging
import threading
from collections import Counter
from socketserver import ThreadingMixIn
from urllib.parse import urlsplit
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

import bottle
from bottle import HTTPResponse

PART_REQUESTS = Counter()
LOCK = threading.Lock()


def log_request(event, status):
    """Record multipart identity without credentials, signed queries or bodies."""
    request = bottle.request
    query = request.query
    upload_id = query.get("uploadId")
    operation = request.method
    if "uploads" in query and request.method == "POST":
        operation = "CreateMultipartUpload"
    elif upload_id:
        operation = {
            "PUT": (
                "UploadPartCopy"
                if "X-Amz-Copy-Source" in request.headers
                else "UploadPart"
            ),
            "POST": "CompleteMultipartUpload",
            "DELETE": "AbortMultipartUpload",
        }.get(request.method, request.method)
    logging.info(
        json.dumps(
            {
                "event": event,
                "operation": operation,
                "path": urlsplit(request.environ["RAW_URI"]).path,
                "upload_id": upload_id,
                "part_number": query.get("partNumber"),
                "copy_source_range": request.headers.get("X-Amz-Copy-Source-Range"),
                "status": status,
            }
        )
    )


@bottle.route("<:path>", method="GET")
@bottle.route("<:path>", method="POST")
@bottle.route("<:path>", method="PUT")
@bottle.route("<:path>", method="DELETE")
@bottle.route("<:path>", method="HEAD")
def index():
    request = bottle.request
    upload_id = request.query.get("uploadId")
    if (
        request.method == "PUT"
        and upload_id
        and request.query.get("partNumber") == "2"
        and "X-Amz-Copy-Source" in request.headers
    ):
        # Count this part alone so concurrent requests cannot consume its faults.
        with LOCK:
            PART_REQUESTS[upload_id] += 1
            attempt = PART_REQUESTS[upload_id]
        if attempt <= 2:
            status = 503 if attempt == 1 else 200
            log_request("injected_part_error", status)
            return HTTPResponse(
                status=status,
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
        log_request("forwarded", response.status)
        return HTTPResponse(status=response.status, body=data, headers=response_headers)
    finally:
        connection.close()


class ThreadedServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class ProxyHandler(WSGIRequestHandler):
    def log_request(self, code="-", size="-"):
        # Structured records above replace access logs, which expose signed queries.
        pass

    def get_environ(self):
        # Forward proxies receive absolute request targets; WSGI expects a path.
        target = urlsplit(self.path)
        self.path = target.path + ("?" + target.query if target.query else "")
        environ = super().get_environ()
        # PATH_INFO is decoded by WSGI. Forward the original escapes so S3 keys
        # containing literal percent sequences are not decoded a second time.
        environ["RAW_URI"] = self.path
        return environ


logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.info(json.dumps({"event": "proxy_started"}))
bottle.run(
    host="0.0.0.0",
    port=4080,
    server="wsgiref",
    server_class=ThreadedServer,
    handler_class=ProxyHandler,
)
