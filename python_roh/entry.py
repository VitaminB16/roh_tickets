import os
import json

from cloud.utils import log
from tools.parquet import Parquet
from python_roh.src.config import *
from tools.firestore import Firestore
from python_roh.src.graphics import Graphics
from python_roh.src.src import API, print_performance_info
from python_roh.upcoming_events import handle_upcoming_events
from python_roh.src.api import get_query_dict, configure_query_dict

try:
    from python_roh.src.src_secret import secret_function

    HAS_SECRET = True
except ImportError:
    HAS_SECRET = False


global QUERY_DICT


def upcoming_events_entry(dont_save=True, **kwargs):
    """
    Entry point for the upcoming events task and the events timeline plot
    """
    df_bundle = handle_upcoming_events(QUERY_DICT, **kwargs)
    events_df, today_tomorrow_events_df, next_week_events_df, new_events_df = df_bundle
    fig = Graphics("events").plot(events_df, dont_save=dont_save, **kwargs)
    if not dont_save:
        partition_cols = ["location", "date", "time", "title"]
        if not new_events_df.empty:
            log(f"Saving to events parquet: {new_events_df.title.unique()}")
            Parquet(EVENTS_PARQUET_LOCATION).write(
                new_events_df, partition_cols=partition_cols
            )
        Firestore(EVENTS_PARQUET_LOCATION).write(
            events_df,
            columns=partition_cols
            + ["performanceId", "productionId", "timestamp", "url"],
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

    query_dict_args = configure_query_dict(**kwargs)

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
    if HAS_SECRET and payload.get("secret_function", False):
        task_scheduler(**payload)  # Sets the QUERY_DICT without executing the task
        secret_function(QUERY_DICT)
    else:
        output = main(**payload)
        if return_output:
            return output
    log("Execution finished")
    return ("Pipeline Complete", 200)
