import pandas as pd

from .src.config import *
from tools.parquet import Parquet
from .src.src import API, _query_soonest_performance_id, query_production_activities


"""
This module contains the functions to handle the data for the upcoming events.
"""


def handle_upcoming_events(query_dict):
    """
    Entry point for the upcoming events
    """
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


def enrich_events_with_productions(events_df):
    """
    Enriches the events_df with the production_id
    """
    unique_productions = events_df.drop_duplicates(subset=["productionId"])
    # Get the existing partitions without reading the parquet
    existing_productions = Parquet(PRODUCTIONS_PARQUET_LOCATION).read(allow_empty=True)
    if existing_productions.empty:
        existing_productions = pd.DataFrame(columns=["slug", "productionId"])
    added_productions = unique_productions.query(
        "productionId not in @existing_productions.productionId"
    ).reset_index(drop=True)

    if not added_productions.empty:
        handle_added_productions(added_productions)
    events_df = merge_prouctions_into_events(events_df)

    return events_df


def handle_added_productions(added_productions):
    """
    Get the activities for the added productions and store them in a Parquet
    """
    print(f"New productions: \n{added_productions.title.unique()}")
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


def merge_prouctions_into_events(events_df):
    """
    Enrich the events_df with the production information
    """
    all_productions = Parquet(PRODUCTIONS_PARQUET_LOCATION).read(allow_empty=True)
    all_productions = all_productions.loc[
        :, ["productionId", "title", "date", "time", "performanceId"]
    ]
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


def query_soonest_performance_id(use_stored=True):
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

    soonest_production_url = events_df_sub.url.iloc[0]
    if use_stored:
        soonest_performance_id = events_df_sub.performanceId.iloc[0].astype(int)
        print(f"Soonest performance id: {soonest_performance_id}")
    else:
        soonest_performance_id = _query_soonest_performance_id(soonest_production_url)
    return soonest_performance_id
