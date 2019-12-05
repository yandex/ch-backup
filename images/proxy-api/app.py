import bottle
import os

@bottle.route('/')
def index():
    return os.environ.get('PROXY_HOST', 'minio01')

bottle.run(host='0.0.0.0', port=8080)