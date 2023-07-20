import bottle
from bottle import HTTPResponse


@bottle.route("<path:path>", method="GET")
@bottle.route("<path:path>", method="POST")
@bottle.route("<path:path>", method="PUT")
@bottle.route("<path:path>", method="DELETE")
@bottle.route("<path:path>", method="HEAD")
def index(path):
    return HTTPResponse(
        status=500,
        body='<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>InternalError</Code'
        "><Message>We encountered an internal error, please try "
        "again.</Message></Error>",
    )


bottle.run(host="0.0.0.0", port=4080)
