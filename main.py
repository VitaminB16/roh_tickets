import os
import sys
import json
from flask import Flask, jsonify
from flask import request as flask_request


from set_secrets import set_secrets

from cloud.utils import log
from tools.parquet import Parquet
from python_roh.src.config import *
from python_roh.src.api import get_query_dict
from python_roh.src.src import API, print_performance_info
from python_roh.src.graphics import plot_hall, plot_events
from python_roh.upcoming_events import handle_upcoming_events


if "src_secret.py" in os.listdir("python_roh/src"):
    from python_roh.src.src_secret import secret_function


def upcoming_events_entry(**kwargs):
    """
    Entry point for the upcoming events task and the events timeline plot
    """
    events_df, today_tomorrow_events_df, next_week_events_df = handle_upcoming_events(
        QUERY_DICT
    )
    plot_events(events_df, **kwargs)
    Parquet(EVENTS_PARQUET_LOCATION).write(
        events_df, partition_cols=["location", "date", "time", "title"]
    )
    return events_df, today_tomorrow_events_df, next_week_events_df


def seats_availability_entry(**kwargs):
    """
    Entry point for the seats availability task and the hall seats plot
    """
    print_performance_info()
    if isinstance(json.loads(str(os.getenv("PERFORMANCE_ID"))), list):
        raise ValueError("List performance IDs are not supported for `seats` task.")
    all_data = API(QUERY_DICT).query_all_data(post_process=True)
    seats_price_df, prices_df, zones_df, price_types_df = (
        all_data["seats"],
        all_data["prices"],
        all_data["zone_ids"],
        all_data["price_types"],
    )
    log(f"Seats available: {seats_price_df.seat_available.sum()}")
    plot_hall(seats_price_df, prices_df, **kwargs)
    return seats_price_df, prices_df, zones_df, price_types_df


def task_scheduler(task_name, **kwargs):
    task_fun = {
        "events": upcoming_events_entry,
        "seats": seats_availability_entry,
    }.get(task_name, None)
    if task_fun is None:
        raise ValueError(f"Task {task_name} not found")

    query_dict_args = {
        "performance_id": os.getenv("PERFORMANCE_ID"),
        "mode_of_sale_id": os.getenv("MODE_OF_SALE_ID"),
        "constituent_id": os.getenv("CONSTITUENT_ID"),
        "source_id": os.getenv("SOURCE_ID"),
    }
    query_dict_args = {k: v for k, v in query_dict_args.items() if v is not None}
    # Merge the query dict with the provided keyword arguments
    query_dict_args.update(kwargs)

    global QUERY_DICT  # Querying all data relating to the performance
    QUERY_DICT = get_query_dict(**query_dict_args)
    return task_fun


def main(task_name, **kwargs):
    """
    Main scheduler function
    """
    task_fun = task_scheduler(task_name, **kwargs)
    return task_fun(**kwargs)


def main_entry(payload):
    if "secret_function" in globals() and payload.get("secret_function", False):
        log("Executing the secret function")
        task_scheduler(**payload)  # Skipping the main function
        secret_function(QUERY_DICT)
    else:
        main(**payload)
    log("Execution finished")
    return ("Pipeline Complete", 200)


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


app = Flask(__name__)


@app.route("/", methods=["POST", "GET"])
def flask_entry_point():
    """
    Entry point adapted for Flask, compatible with Cloud Run.
    """
    if flask_request.method == "POST":
        payload = flask_request.get_json(silent=True, force=True)
    else:
        payload = flask_request.args.to_dict()

    log("Payload:", payload)
    # Pass the payload to your main_entry function
    response_message, status_code = main_entry(payload)
    return jsonify(message=response_message), status_code


if __name__ == "__main__":
    serve_as = os.environ.get("SERVE_AS", "local")
    if serve_as == "cloud_run":
        app.run(host="0.0.0.0", port=8080)
    else:
        with open("payload.json", "r") as f:
            payload = json.load(f)
        args = sys.argv[1:]
        if len(args) > 0:
            args = parse_args(args)
            payload.update(args)
        payload.update({"save_both": False})
        main_entry(payload)
