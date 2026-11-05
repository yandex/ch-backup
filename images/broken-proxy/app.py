"""Always-broken proxy, with opt-in deterministic copy faults for integration tests."""

import http.client
import threading
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlsplit
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

import bottle
from bottle import HTTPResponse

STATE = {"config": None, "requests": [], "resolutions": 0, "failures": 0}
LOCK = threading.Lock()


@bottle.post("/__fault__/config")
def configure():
    with LOCK:
        STATE.update(config=bottle.request.json, requests=[], resolutions=0, failures=0)
    return {"configured": True}


@bottle.get("/__fault__/state")
def state():
    with LOCK:
        return {
            "requests": list(STATE["requests"]),
            "resolutions": STATE["resolutions"],
        }


@bottle.get("/__fault__/resolve")
def resolve():
    with LOCK:
        STATE["resolutions"] += 1
        config = STATE["config"]
        if config.get("rotate") and STATE["failures"]:
            return "proxy01"
    return "broken-proxy01"


def error_response(status, code):
    return HTTPResponse(
        status=status,
        body="<Error><Code>{}</Code><Message>injected multipart copy failure</Message></Error>".format(
            code
        ),
        headers={"Content-Type": "application/xml"},
    )


def operation(method, query, headers):
    if method == "POST":
        return (
            "CreateMultipartUpload" if "uploads" in query else "CompleteMultipartUpload"
        )
    if "uploadId" in query:
        return "AbortMultipartUpload" if method == "DELETE" else "UploadPartCopy"
    if "X-Amz-Copy-Source" in headers:
        return "CopyObject"
    return method


@bottle.route("<path:path>", method="GET")
@bottle.route("<path:path>", method="POST")
@bottle.route("<path:path>", method="PUT")
@bottle.route("<path:path>", method="DELETE")
@bottle.route("<path:path>", method="HEAD")
def index(path):
    request = bottle.request
    query = parse_qs(request.query_string, keep_blank_values=True)
    op = operation(request.method, query, request.headers)
    record = {
        "operation": op,
        "path": path,
        "upload_id": query.get("uploadId", [None])[0],
        "part_number": int(query.get("partNumber", [0])[0]),
        "range": request.headers.get("X-Amz-Copy-Source-Range"),
        "source": request.headers.get("X-Amz-Copy-Source"),
    }
    with LOCK:
        config = STATE["config"]
        if config is None:
            return error_response(500, "InternalError")
        if path == config["path"]:
            STATE["requests"].append(record)
            for rule in config["rules"]:
                if (
                    op != rule["operation"]
                    or rule.get("part_number", 0) != record["part_number"]
                ):
                    continue
                remaining = rule["failures"]
                if remaining:
                    if remaining > 0:
                        rule["failures"] -= 1
                    STATE["failures"] += 1
                    record["injected"] = True
                    return error_response(rule["status"], rule["code"])

    # Only used in the isolated HTTP test network. Preserve Host/signature and
    # all S3 headers; do not log credentials or Authorization.
    target = urlsplit("http://" + request.headers["Host"])
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
            path + ("?" + request.query_string if request.query_string else ""),
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
        return super().get_environ()


bottle.run(
    host="0.0.0.0",
    port=4080,
    server="wsgiref",
    server_class=ThreadedServer,
    handler_class=ProxyHandler,
)
