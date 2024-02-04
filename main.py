import os
import sys
import argparse
import plotly.express as px

from set_secrets import set_secrets
from python_roh.src.config import parse_args
from python_roh.src.api import get_query_dict
from python_roh.src.src import query_all_data
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
    plot_events(events_df)


def seats_availability_entry(**kwargs):
    """
    Entry point for the seats availability task and the hall seats plot
    """

    all_data = query_all_data(QUERY_DICT, post_process=True)

    seats_price_df, prices_df, zones_df, price_types_df = (
        all_data["seats_price"],
        all_data["prices"],
        all_data["zones"],
        all_data["price_types"],
    )

    plot_hall(seats_price_df)


def main(task_name, **kwargs):
    """
    Main entry point for the tasks
    """
    task_fun = {
        "upcoming": upcoming_events_entry,
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


if __name__ == "__main__":
    args = sys.argv[1:]
    args = parse_args(args)
    main(**args)
    if "secret_function" in globals() and "secret_function" in args:
        secret_function(QUERY_DICT)
