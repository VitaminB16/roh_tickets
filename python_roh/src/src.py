import time
import random
import requests
import pandas as pd

from .config import *
from .utils import force_list


def pre_process_zone_df(input_json):
    zones_df = pd.DataFrame(input_json)
    zones_df = zones_df["Zone"]
    zones_df = zones_df.apply(pd.Series)
    zones_df = zones_df.rename(columns={"Id": "ZoneId"})
    _zone_groups = zones_df.ZoneGroup.apply(pd.Series)
    zones_df = pd.concat([zones_df, _zone_groups], axis=1)
    zones_df.drop(columns=["ZoneGroup", "Description"], inplace=True)
    zones_df.rename(
        columns={"Id": "ZoneGroupId", "AliasDescription": "ZoneName"}, inplace=True
    )
    return zones_df


def pre_process_seats_df(input_json):
    seats_df = pd.DataFrame(input_json)
    seats_df.rename(columns={"Id": "SeatId"}, inplace=True)
    return seats_df


def pre_process_prices_df(input_json):
    prices_df = pd.DataFrame(input_json)
    prices_df = prices_df.query("Enabled == True").reset_index(drop=True)
    return prices_df


def pre_process_price_types_df(input_json):
    price_types_df = pd.DataFrame(input_json)
    return price_types_df


def pre_process_events_df(input_json):
    data = input_json
    events_df = pd.DataFrame(data["data"])
    included_df = pd.DataFrame(data["included"])
    events_attrs = events_df.attributes.apply(pd.Series)
    events_attrs.drop(
        columns=[
            "description",
            "dateFieldOverride",
            "imageResult",
            "imageTray",
            "productionPageUrl",
            "helpInformation",
        ],
        inplace=True,
    )
    events_rels = events_df.relationships.apply(pd.Series)
    events_df = pd.concat([events_df, events_attrs, events_rels], axis=1)
    events_df.query("isCancelled != True", inplace=True)
    events_df.drop(
        columns=[
            "attributes",
            "relationships",
            "isCancelled",
            "ctaBehaviour",
            "cinemaBroadcastLink",
        ],
        inplace=True,
    )
    # remove events with no listed performances
    events_df.query("performances not in [[]]", inplace=True)
    events_df.reset_index(drop=True, inplace=True)
    events_df = events_df.explode("performances", ignore_index=True)
    events_df.locations = events_df.locations.apply(lambda x: x["data"])
    events_df = events_df.explode("locations", ignore_index=True)
    events_df.locations = events_df.locations.apply(lambda x: x["id"])
    events_df.rename(columns={"locations": "locationId"}, inplace=True)
    return events_df, included_df


def do_nothing(input_json):
    return input_json


def pre_process_df(input_json, df_type):
    pre_process_fun = {
        "seats": pre_process_seats_df,
        "prices": pre_process_prices_df,
        "zone_ids": pre_process_zone_df,
        "price_types": pre_process_price_types_df,
        "events": pre_process_events_df,
    }
    return pre_process_fun.get(df_type, do_nothing)(input_json)


def post_process_all_data(data, data_types=None):
    """
    Merge the different dataframes together and do some post-processing
    """

    seats_df, prices_df, zones_df, price_types_df = (
        data["seats"],
        data["prices"],
        data["zone_ids"],
        data["price_types"],
    )
    seats_price_df = seats_df.merge(prices_df, on="ZoneId")
    seats_price_df = seats_price_df.merge(zones_df, on="ZoneId")
    seats_price_df.query("ZoneName in @ZONE_HIERARCHY.keys()", inplace=True)
    # Fix YPosition so that it follows the zone hierarchy
    seats_price_df = fix_y_positions(seats_price_df)
    # Keep only the lowest price for each (SeatId, SectionId, PerformanceId)
    seats_price_df.sort_values(
        by=["SeatId", "SectionId", "PerformanceId", "Price"],
        inplace=True,
        ascending=True,
    )
    seats_price_df.drop_duplicates(
        subset=["SeatId", "SectionId", "PerformanceId"], inplace=True
    )
    seats_price_df = seats_price_df.assign(
        seat_available=(~seats_price_df.SeatStatusId.isin(TAKEN_SEAT_STATUS_IDS))
    )
    seats_price_df.Price = seats_price_df.Price.where(
        seats_price_df.seat_available, None
    )
    data = {
        "seats_price": seats_price_df,
        "prices": prices_df,
        "zones": zones_df,
        "price_types": price_types_df,
    }

    return data


def query_one_data(query_dict, data_type):
    """
    Query one type of data from the query_dict
    """

    url = query_dict[data_type]["url"]
    params = query_dict[data_type]["params"]
    json_response = requests.get(url, params=params).json()
    return pre_process_df(json_response, data_type)


def query_all_data(query_dict, data_types=None, post_process=False):
    """
    Query all data from the query_dict
    """
    if data_types is None:
        data_types = query_dict.keys()
    data = {}
    for data_type in force_list(data_types):
        data[data_type] = query_one_data(query_dict, data_type)
        time.sleep(1)
    print(f"Queried the following from the API: {list(data.keys())}")
    if post_process:
        data = post_process_all_data(data)
    return data


def fix_y_positions(df):
    """
    Fix the YPosition so that it follows the zone hierarchy
    """
    df = df.assign(zone_hierarchy=df.ZoneName.map(ZONE_HIERARCHY).astype(int))
    max_y_positions = df.groupby(["zone_hierarchy"]).YPosition.max().reset_index()
    # Calculate cumulative sum of seats in each zone
    max_y_positions.sort_values(by=["zone_hierarchy"], inplace=True)
    cum_sum = max_y_positions.YPosition.cumsum()[:-1]
    cum_sum = pd.concat([pd.Series([0]), cum_sum], axis=0).reset_index(drop=True)
    max_y_positions["y_absolute_shift"] = cum_sum
    max_y_positions.rename(columns={"YPosition": "max_zone_y_position"}, inplace=True)
    # Merge the max YPosition back into the df
    df.drop(
        columns=["max_zone_y_position", "y_absolute_shift"],
        inplace=True,
        errors="ignore",
    )
    df = df.merge(max_y_positions, on="zone_hierarchy")
    df["YPosition"] = df["max_zone_y_position"] - df["YPosition"]
    df["YPosition"] = df["YPosition"] + df["y_absolute_shift"]
    df.drop(columns=["max_zone_y_position", "y_absolute_shift"], inplace=True)
    return df
