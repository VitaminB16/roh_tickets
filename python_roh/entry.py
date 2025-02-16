import os
import json

from cloud.utils import log
from python_roh.src.config import *
from tools import Parquet, Firestore
from python_roh.src.graphics import Graphics
from python_roh.src.src import API, print_performance_info
from python_roh.upcoming_events import handle_upcoming_events
from python_roh.src.api import get_query_dict, configure_query_dict
from python_roh.casts import handle_new_past_casts, handle_seen_performances

try:
    from python_roh.src.src_secret import secret_function

    HAS_SECRET = True
except ImportError:
    HAS_SECRET = False


global QUERY_DICT


def save_new_events(new_events_df, partition_cols):
    if not new_events_df.empty:
        log(f"Saving to events parquet: {new_events_df.title.unique()}")
        Parquet(EVENTS_PARQUET_LOCATION).write(
            new_events_df, partition_cols=partition_cols
        )
    else:
        log("No new events to save")
    return


def save_unseen_events(events_df, new_events_df, partition_cols):
    existing_events_df = Parquet(EVENTS_PARQUET_LOCATION).read(
        allow_empty=True, use_bigquery=True
    )
    existimng_ids = existing_events_df.performanceId.unique()
    new_ids = new_events_df.performanceId.unique()
    unseen_events_df = events_df.query(
        "(performanceId not in @existimng_ids)"
        + "and (performanceId not in @new_ids)"
        + "and (performanceId.notnull())"
    )
    if not unseen_events_df.empty:
        log(f"Saving to events parquet: {unseen_events_df.title.unique()}")
        Parquet(EVENTS_PARQUET_LOCATION).write(
            unseen_events_df, partition_cols=partition_cols
        )
    else:
        log("No unseen events to save")


def upcoming_events_entry(dont_save=True, **kwargs):
    """
    Entry point for the upcoming events task and the events timeline plot
    """
    df_bundle = handle_upcoming_events(QUERY_DICT, **kwargs)
    events_df, today_tomorrow_events_df, next_week_events_df, new_events_df = df_bundle
    fig = Graphics("events").plot(events_df, dont_save=dont_save, **kwargs)
    # fig = None
    if not dont_save:
        partition_cols = ["location", "date", "time", "title"]
        save_new_events(new_events_df, partition_cols)
        save_unseen_events(events_df, new_events_df, partition_cols)

        Firestore(EVENTS_PARQUET_LOCATION).write(
            events_df,
            columns=partition_cols
            + ["performanceId", "productionId", "timestamp", "url"],
        )
        handle_new_past_casts(events_df)
        handle_seen_performances()
    return events_df, today_tomorrow_events_df, next_week_events_df, fig


def seats_availability_entry(**kwargs):
    """
    Entry point for the seats availability task and the hall seats plot
    """
    print_performance_info(**kwargs)
    if isinstance(json.loads(str(os.getenv("PERFORMANCE_ID"))), list):
        raise ValueError("List performance IDs are not supported for `seats` task.")
    all_data = API(QUERY_DICT).query_all_data(
        data_types=["seats", "prices", "zone_ids", "price_types"],
        post_process=True,
        **kwargs,
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
    task_name = payload.pop("task_name", None)
    if HAS_SECRET and payload.get("secret_function", False):
        task_scheduler(task_name, **payload)  # Sets the QUERY_DICT only
        secret_function(QUERY_DICT)
    else:
        output = main(task_name, **payload)
        if return_output:
            return output
    log("Execution finished")
    return ("Pipeline Complete", 200)
