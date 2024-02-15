import os
import sys
import json
import base64
import pyarrow as pa
import pyarrow.parquet as pq


from set_secrets import set_secrets

from python_roh.src.config import *
from python_roh.src.api import get_query_dict
from python_roh.src.src import API, print_performance_info
from python_roh.src.graphics import plot_hall, plot_events
from python_roh.upcoming_events import handle_upcoming_events

from tools.parquet import Parquet


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
    all_data = API(QUERY_DICT).query_all_data(post_process=True)

    seats_price_df, prices_df, zones_df, price_types_df = (
        all_data["seats"],
        all_data["prices"],
        all_data["zone_ids"],
        all_data["price_types"],
    )
    plot_hall(seats_price_df, prices_df, **kwargs)
    return seats_price_df, prices_df, zones_df, price_types_df


def main(task_name, **kwargs):
    """
    Main entry point for the tasks
    """
    task_fun = {
        "events": upcoming_events_entry,
        "seats": seats_availability_entry,
    }.get(task_name, None)
    if task_fun is None:
        raise ValueError(f"Task {task_name} not found")

    query_dict_args = {
        "performance_id": os.environ["PERFORMANCE_ID"],
        "mode_of_sale_id": os.environ["MODE_OF_SALE_ID"],
        "constituent_id": os.environ["CONSTITUENT_ID"],
        "source_id": os.environ["SOURCE_ID"],
    }
    # Merge the query dict with the provided keyword arguments
    query_dict_args.update(kwargs)

    global QUERY_DICT  # Querying all data relating to the performance
    QUERY_DICT = get_query_dict(**query_dict_args)

    return task_fun(**kwargs)


def entry_point(event=None, context=None):
    msg = base64.b64decode(event["data"]).decode("utf-8")
    payload = json.loads(msg)
    main(**payload)
    if "secret_function" in globals() and "secret_function" in payload:
        print("Executing the secret function")
        secret_function(QUERY_DICT)
    print("Execution finished")
    return ("Pipeline Complete", 200)


if __name__ == "__main__":
    payload = {"task_name": "events", "pid": "soonest"}
    args = sys.argv[1:]
    if len(args) > 0:
        args = parse_args(args)
        payload.update(args)
    main(**payload)
