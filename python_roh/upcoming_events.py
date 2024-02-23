import pandas as pd
from urllib.parse import unquote

from cloud.utils import log
from tools.parquet import Parquet
from python_roh.src.config import *
from python_roh.src.utils import force_list, enforce_schema
from python_roh.src.src import (
    API,
    _query_soonest_performance_id,
    query_production_activities,
)

"""
This module contains the functions to handle the data for the upcoming events.
"""


def handle_upcoming_events(query_dict, query_events_api=True, **kwargs):
    """
    Entry point for the upcoming events
    """
    if not query_events_api:
        log("Not querying the API for events. Using stored data.")
        events_df = Parquet(EVENTS_PARQUET_LOCATION).read(allow_empty=True)
        today = pd.Timestamp.today(tz="Europe/London") - pd.Timedelta(hours=1)
        today_tomorrow_events_df, next_week_events_df = get_next_weeks_events(
            events_df, today
        )
        return events_df, today_tomorrow_events_df, next_week_events_df
    data = API(query_dict).query_all_data("events")
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
        subset=["type", "productionId", "location", "timestamp"],
        inplace=True,
        ignore_index=True,
    )
    events_df["url"] = events_df.slug.apply(
        lambda x: f"{TICKETS_AND_EVENTS_URL}/{x}-dates"
    )
    events_df["productionId"] = events_df.productionId.astype(int)
    events_df = enrich_events_with_productions(events_df)
    events_df.reset_index(drop=True, inplace=True)
    return events_df


def enrich_events_with_productions(events_df, dont_read_from_storage=True):
    """
    Enriches the events_df with the production_id
    """
    unique_productions = events_df.drop_duplicates(subset=["productionId"])

    if dont_read_from_storage:
        # Get the existing partitions without reading the parquet
        existing_prods = PLATFORM.glob(f"{PRODUCTIONS_PARQUET_LOCATION}/*/*")
        existing_prods = [x.split("/")[-1] for x in existing_prods]
        existing_prods = [x.replace("productionId=", "") for x in existing_prods]
        existing_prod_ids = [int(x) for x in existing_prods if x.isdigit()]
    else:
        # Get the existing productions from the Parquet
        existing_prods = Parquet(PRODUCTIONS_PARQUET_LOCATION).read(allow_empty=True)
        existing_prod_ids = existing_prods.productionId.unique()
    added_productions = unique_productions.query(
        "productionId not in @existing_prod_ids"
    ).reset_index(drop=True)

    if not added_productions.empty:
        handle_added_productions(added_productions)
    events_df = merge_prouctions_into_events(
        events_df, dont_read_from_storage=dont_read_from_storage
    )

    return events_df


def handle_added_productions(added_productions):
    """
    Get the activities for the added productions and store them in a Parquet
    """
    log(f"New productions: \n{added_productions.title.unique()}")
    for production_i in added_productions.itertuples():
        production_url = production_i.url
        activities_df = query_production_activities(production_url)
        activities_df.rename(
            columns={"id": "performanceId", "date": "timestamp"}, inplace=True
        )
        performances_df = activities_df.loc[:, ["performanceId", "timestamp"]]
        performances_df = performances_df.assign(
            productionId=production_i.productionId,
            title=production_i.title,
            location=production_i.location,
            slug=production_i.slug,
            date=performances_df.timestamp.dt.date,
            time=performances_df.timestamp.dt.time,
        )
        # Store the productions Parquet
        Parquet(PRODUCTIONS_PARQUET_LOCATION).write(
            performances_df,
            partition_cols=["title", "productionId", "date", "time", "performanceId"],
        )
    return None


def merge_prouctions_into_events(events_df, dont_read_from_storage=True):
    """
    Enrich the events_df with the production information
    """
    production_cols = ["productionId", "title", "date", "time", "performanceId"]
    # Get the existing partitions without reading the parquet
    all_productions = Parquet(PRODUCTIONS_PARQUET_LOCATION).read(
        columns=production_cols,
        allow_empty=True,
        use_bigquery=False,
        read_partitions_only=dont_read_from_storage,
    )
    events_df = events_df.merge(
        all_productions, on=["productionId", "title", "date", "time"], how="left"
    )
    return events_df


def get_next_weeks_events(events_df, today):
    """
    Get the events for today and tomorrow and for the next week
    """
    tomorrow = today + pd.Timedelta(days=1)
    next_week = today + pd.Timedelta(days=7)
    events_df = events_df.query("timestamp > @today").reset_index(drop=True)
    today_tomorrow_events_df = events_df.query("timestamp <= @tomorrow").reset_index(
        drop=True
    )
    next_week_events_df = events_df.query("timestamp <= @next_week").reset_index(
        drop=True
    )
    return today_tomorrow_events_df, next_week_events_df


def query_soonest_performance_id(n_soonest=1, use_stored=True, **kwargs):
    """
    Query the soonest performance id
    """
    query_dict = {
        "events": {
            "url": ALL_EVENTS_URL,
            "params": {},
        },
    }
    if use_stored:
        events_df = Parquet(EVENTS_PARQUET_LOCATION).read(
            filters={"location": "Main Stage"}, allow_empty=True
        )
    else:
        events_df, _, _ = handle_upcoming_events(query_dict)
    today = pd.Timestamp.today(tz="Europe/London") - pd.Timedelta(hours=1)
    events_df_sub = events_df.query(
        "location == 'Main Stage' & timestamp >= @today"
    ).reset_index(drop=True)
    events_df_sub.sort_values(by=["timestamp"], inplace=True)

    if use_stored:
        soonest_perf_ids = (
            events_df_sub.performanceId.astype(str).iloc[:n_soonest].values.tolist()
        )
        soonest_perf_ids = force_list(soonest_perf_ids)
        log(f"Soonest performance id: {soonest_perf_ids}")
    else:
        soonest_prod_urls = events_df_sub.url.iloc[:n_soonest].values.tolist()
        soonest_prod_urls = force_list(soonest_prod_urls)
        soonest_perf_ids = [_query_soonest_performance_id(x) for x in soonest_prod_urls]
    if len(soonest_perf_ids) == 1:
        soonest_perf_ids = soonest_perf_ids[0]
    return soonest_perf_ids
