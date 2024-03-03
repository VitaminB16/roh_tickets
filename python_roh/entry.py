import os
import json

from cloud.utils import log
from tools.parquet import Parquet
from python_roh.src.config import *
from tools.firestore import Firestore
from python_roh.src.graphics import Graphics
from python_roh.src.api import get_query_dict
from python_roh.src.src import API, print_performance_info
from python_roh.upcoming_events import handle_upcoming_events

if "src_secret.py" in os.listdir("python_roh/src"):
    from python_roh.src.src_secret import secret_function


global QUERY_DICT


def upcoming_events_entry(dont_save=True, **kwargs):
    """
    Entry point for the upcoming events task and the events timeline plot
    """
    events_df, today_tomorrow_events_df, next_week_events_df = handle_upcoming_events(
        QUERY_DICT, **kwargs
    )
    fig = Graphics("events").plot(events_df, dont_save=dont_save, **kwargs)
    if not dont_save:
        partition_cols = ["location", "date", "time", "title", "url"]
        Parquet(EVENTS_PARQUET_LOCATION).write(events_df, partition_cols=partition_cols)
        today = pd.Timestamp("today").date() - pd.Timedelta(hours=1)
        recent_df = events_df.query(
            "date >= @today & location == 'Main Stage'"
        ).reset_index(drop=True)
        Firestore(EVENTS_PARQUET_LOCATION).write(
            recent_df,
            columns=partition_cols + ["performanceId", "productionId", "timestamp"],
        )
    return events_df, today_tomorrow_events_df, next_week_events_df, fig


def seats_availability_entry(**kwargs):
    """
    Entry point for the seats availability task and the hall seats plot
    """
    print_performance_info(**kwargs)
    if isinstance(json.loads(str(os.getenv("PERFORMANCE_ID"))), list):
        raise ValueError("List performance IDs are not supported for `seats` task.")
    all_data = API(QUERY_DICT).query_all_data(
        data_types=["seats", "prices", "zone_ids", "price_types"], post_process=True
    )
    seats_price_df, prices_df, zones_df, price_types_df = (
        all_data["seats"],
        all_data["prices"],
        all_data["zone_ids"],
        all_data["price_types"],
    )
    log(f"Seats available: {seats_price_df.seat_available.sum()}")
    fig = Graphics("hall").plot(seats_price_df, prices_df, **kwargs)
    return seats_price_df, prices_df, zones_df, price_types_df, fig


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
        **kwargs,
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


def main_entry(payload, return_output=False):
    if "secret_function" in globals() and payload.get("secret_function", False):
        task_scheduler(**payload)  # Sets the QUERY_DICT without executing the task
        secret_function(QUERY_DICT)
    else:
        output = main(**payload)
        if return_output:
            return output
    log("Execution finished")
    return ("Pipeline Complete", 200)
