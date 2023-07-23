import os
import random

import bottle


@bottle.route("/")
def index():
    network = str(os.environ.get("NETWORK"))
    # 10% chance to get broken proxy.
    if random.randint(0, 9) > 0:
        return "proxy01." + network
    else:
        return "broken-proxy01." + network


bottle.run(host="0.0.0.0", port=8080)
