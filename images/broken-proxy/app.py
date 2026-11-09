"""S3 test proxy injecting part errors and lost multipart completion replies."""

import http.client
import json
import logging
import threading
from itertools import count
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlsplit
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer
from xml.etree import ElementTree

import bottle
from bottle import HTTPResponse

PART_REQUESTS = count(1)
COPY_UPLOADS = set()
COMPLETED_COPIES = set()
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
    if request.method == "POST" and "uploadId" in query:
        with LOCK:
            completed = query["uploadId"][0] in COMPLETED_COPIES
            COMPLETED_COPIES.discard(query["uploadId"][0])
        if completed:
            # SeaweedFS accepts duplicate Complete requests; emulate S3's
            # NoSuchUpload after a lost success response to test reconciliation.
            log_request("injected_no_such_upload", 404)
            return HTTPResponse(
                status=404,
                body="<Error><Code>NoSuchUpload</Code></Error>",
                headers={"Content-Type": "application/xml"},
            )
    if (
        request.method == "PUT"
        and "uploadId" in query
        and "X-Amz-Copy-Source" in request.headers
    ):
        # Count parts separately so other parallel requests cannot hide a fault.
        with LOCK:
            part_request = next(PART_REQUESTS)
            COPY_UPLOADS.add(query["uploadId"][0])
        if part_request % 2 == 0:
            status = 200 if part_request % 4 == 0 else 500
            log_request("injected_part_error", status)
            return HTTPResponse(
                status=status,
                body="<Error><Code>InternalError</Code>"
                "<Message>injected UploadPartCopy failure HTTP {}</Message></Error>".format(
                    status
                ),
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
        if request.method == "POST" and "uploadId" in query and response.status == 200:
            with LOCK:
                lose_response = query["uploadId"][0] in COPY_UPLOADS
                COPY_UPLOADS.discard(query["uploadId"][0])
                if lose_response:
                    COMPLETED_COPIES.add(query["uploadId"][0])
            if lose_response:
                # The backend has completed the copy, but the caller receives a
                # gateway failure. Retrying Complete must not copy all parts again.
                log_request("injected_lost_complete_response", 502)
                return HTTPResponse(
                    status=502,
                    body="<Error><Code>InternalError</Code>"
                    "<Message>injected lost CompleteMultipartUpload response</Message></Error>",
                    headers={"Content-Type": "application/xml"},
                )
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
