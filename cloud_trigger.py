import os
import sys
import json

from python_roh.src.config import *
from set_secrets import set_secrets
from cloud.utils import GCPRequest, log


if __name__ == "__main__":
    with open("payload.json") as f:
        payload = json.load(f)
    args = sys.argv[1:]
    if len(args) > 0:
        args = parse_args(args)
        payload.update(args)
    serve_as = payload.get("serve_as", False)
    if serve_as:
        url_env = {
            "cloud_run": "CLOUD_RUN_URL",
            "cloud_function": "CLOUD_FUNCTION_URL",
        }.get(serve_as, "CLOUD_FUNCTION_URL")
    url = os.getenv(url_env, os.getenv("CLOUD_FUNCTION_URL"))
    log(f"Sending {payload} to {url}")
    response = GCPRequest(url).post(payload)
    log(response.text)
