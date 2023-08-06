import os
import random

import bottle


@bottle.route("/")
def index():
    network = os.environ.get("NETWORK")
    # 10% chance to get broken proxy.
    if not os.environ.get("S3_FAULT_INJECTION_ENABLED") or random.randint(0, 9) > 0:
        return f"proxy01.{network}"
    else:
        return f"broken-proxy01.{network}"


bottle.run(host="0.0.0.0", port=8080)
