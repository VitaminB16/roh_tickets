import pandas as pd

from .src.config import *
from .src.src import query_all_data, _query_soonest_performance_id

"""
This module contains the functions to handle the data for the upcoming events.
"""


def handle_upcoming_events(query_dict):
    """
    Entry point for the upcoming events
    """
    data = query_all_data(query_dict, "events")
    events_df, included_df = data["events"]

    locations_df = get_locations_df(included_df)
    events_df = events_df.merge(locations_df, on="locationId", how="left")

    performances_df = get_performances_df(events_df)
    events_df = pd.concat([events_df, performances_df], axis=1)
    events_df.drop(columns=["performances", "date"], inplace=True)
    events_df = enrich_events_df(events_df)

    today = pd.Timestamp.today(tz="Europe/London") - pd.Timedelta(hours=1)
    today_tomorrow_events_df, next_week_events_df = get_next_weeks_events(
        events_df, today
    )

    return events_df, today_tomorrow_events_df, next_week_events_df


def get_locations_df(included_df):
    """
    Extract the locations data from the included_df
    """
    locations_df = included_df.query("type == 'locations'").drop(
        columns=["type", "relationships"]
    )
    locations_attrs = locations_df.attributes.apply(pd.Series)
    locations_df = pd.concat([locations_df, locations_attrs], axis=1)
    locations_df.drop(columns=["attributes"], inplace=True)
    locations_df.reset_index(drop=True, inplace=True)
    locations_df.rename(columns={"id": "locationId", "title": "location"}, inplace=True)
    return locations_df


def get_performances_df(events_df):
    """
    Extract the performances data from the events_df
    """
    performances_df = events_df.performances.apply(pd.Series)
    performances_df = performances_df.assign(
        timestamp=pd.to_datetime(performances_df.date, utc=True)
    )
    # Adjust the timestamp to account for UK timezone
    performances_df.timestamp = performances_df.timestamp.dt.tz_convert("Europe/London")
    return performances_df


def enrich_events_df(events_df):
    """
    Enrich the events_df with additional date and url columns
    """
    events_df.sort_values(by=["timestamp"], inplace=True)
    events_df["date"] = events_df.timestamp.dt.date
    events_df["time"] = events_df.timestamp.dt.time
    events_df["day"] = events_df.timestamp.dt.day_name()
    events_df.drop_duplicates(
        subset=["type", "id", "location", "timestamp"], inplace=True, ignore_index=True
    )
    events_df["url"] = events_df.slug.apply(
        lambda x: f"{TICKETS_AND_EVENTS_URL}/{x}-dates"
    )
    return events_df


def get_next_weeks_events(events_df, today):
    """
    Get the events for today and tomorrow and for the next week
    """
    tomorrow = today + pd.Timedelta(days=1)
    next_week = today + pd.Timedelta(days=7)
    events_df = events_df.query("timestamp > @today").reset_index(drop=True)
    today_tomorrow_events_df = events_df.query("date <= @tomorrow.date()").reset_index(
        drop=True
    )
    next_week_events_df = events_df.query("date <= @next_week.date()").reset_index(
        drop=True
    )
    return today_tomorrow_events_df, next_week_events_df


def query_soonest_performance_id():
    """
    Query the soonest performance id
    """
    query_dict = {
        "events": {
            "url": ALL_EVENTS_URL,
            "params": {},
        },
    }
    events_df, _, _ = handle_upcoming_events(query_dict)
    today = pd.Timestamp.today(tz="Europe/London") - pd.Timedelta(hours=1)
    events_df_sub = events_df.query(
        "location == 'Main Stage' & date >= @today.date()"
    ).reset_index(drop=True)
    soonest_production_url = events_df_sub.url.iloc[0]
    soonest_performance_id = _query_soonest_performance_id(soonest_production_url)
    return soonest_performance_id
