"""S3 test proxy with deterministic multipart copy failures and request logs."""

import http.client
import json
import logging
import threading
from collections import Counter
from pathlib import Path
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlsplit
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer
from xml.etree import ElementTree

import bottle
from bottle import HTTPResponse

PART_REQUESTS = Counter()
FAULT_PATH = None
LOCK = threading.Lock()


def log_request(event, status, upload_id=None):
    """Record multipart identity without credentials, signed queries or bodies."""
    request = bottle.request
    query = parse_qs(request.query_string, keep_blank_values=True)
    upload_id = upload_id or query.get("uploadId", [None])[0]
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
                "part_number": query.get("partNumber", [None])[0],
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
    query = parse_qs(request.query_string, keep_blank_values=True)
    # Tests write this local file through Docker, without a public control API.
    fault_file = Path("/tmp/copy-fault.json")
    fault = (
        json.loads(fault_file.read_text())
        if fault_file.exists()
        else {"failures": 2, "status": 503}
    )
    if fault and "uploadId" in query:
        global FAULT_PATH
        path = urlsplit(request.environ["RAW_URI"]).path
        with LOCK:
            if request.method == "PUT" and "X-Amz-Copy-Source" in request.headers:
                if FAULT_PATH is None:
                    FAULT_PATH = path
                part = query["partNumber"][0]
                identity = (query["uploadId"][0], part)
                PART_REQUESTS[identity] += 1
                fail_part = (
                    path == FAULT_PATH
                    and part == "2"
                    and PART_REQUESTS[identity] <= fault["failures"]
                )
            else:
                fail_part = False
        fail_abort = (
            request.method == "DELETE"
            and path == FAULT_PATH
            and fault.get("abort_status", 204) != 204
        )
        if fail_part or fail_abort:
            status = fault["status"] if fail_part else fault["abort_status"]
            operation = "UploadPartCopy" if fail_part else "AbortMultipartUpload"
            log_request("injected_part_error" if fail_part else "injected_abort_error", status)
            return HTTPResponse(
                status=status,
                body="<Error><Code>InternalError</Code>"
                "<Message>injected {} failure</Message></Error>".format(operation),
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
        upload_id = None
        if request.method == "POST" and "uploads" in query and response.status == 200:
            try:
                upload_id = next(
                    (
                        node.text
                        for node in ElementTree.fromstring(data).iter()
                        if node.tag.rsplit("}", 1)[-1] == "UploadId"
                    ),
                    None,
                )
            except ElementTree.ParseError:
                # Logging must not change how an invalid backend reply is forwarded.
                pass
        response_headers = {
            key: value
            for key, value in response.getheaders()
            if key.lower() not in ("connection", "transfer-encoding")
        }
        log_request("forwarded", response.status, upload_id)
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
