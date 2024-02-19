import os
import sys
import json

from cloud.utils import GCPRequest
from python_roh.src.config import *
from set_secrets import set_secrets


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
    response = GCPRequest(os.environ["CLOUD_FUNCTION_URL"]).post(payload)
    print(response.text)
