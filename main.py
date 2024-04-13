import os
import sys
import json
from flask import Flask, jsonify, request as flask_request

from python_roh.set_secrets import set_secrets

from cloud.utils import log
from python_roh.src.config import *
from python_roh.entry import main_entry

app = Flask(__name__)


def entry_point(request=None):
    """
    Entry point for the HTTP request
    """
    if request is not None:
        request_json = request.get_json(silent=True, force=True)
        request_args = request.args
        payload = request_json if request_json else request_args
    log("Payload:", payload)
    main_entry(payload)
    return payload


@app.route("/", methods=["POST", "GET"])
def flask_entry_point():
    """
    Entry point adapted for Flask, compatible with Cloud Run.
    """
    if flask_request.method == "POST":
        payload = flask_request.get_json(silent=True, force=True)
    else:
        payload = flask_request.args.to_dict()

    log("SERVE_AS:", os.environ.get("SERVE_AS", None))
    log("Payload:", payload)
    # Pass the payload to your main_entry function
    response_message, status_code = main_entry(payload)
    return jsonify(message=response_message), status_code


if __name__ == "__main__":
    serve_as = os.environ.get("SERVE_AS", "local")
    port = int(os.environ.get("PORT", 8080))
    host = os.environ.get("HOST", "0.0.0.0")
    if serve_as == "cloud_run":
        log("Starting the Flask app for Cloud Run")
        app.run(host=host, port=port)
    elif serve_as == "dash_app":
        log("Starting the Dash app")
        from python_roh.dash.app import app

        app.run_server(host=host, port=port)
    else:
        with open("payload.json", "r") as f:
            payload = json.load(f)
        args = sys.argv[1:]
        if len(args) > 0:
            args = parse_args(args)
            payload.update(args)
        main_entry(payload)
