import json
import time
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import unquote

from .config import *
from .utils import force_list


def _pre_process_zone_df(input_json):
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


def _pre_process_seats_df(input_json):
    seats_df = pd.DataFrame(input_json)
    seats_df.rename(columns={"Id": "SeatId"}, inplace=True)
    seats_df = seats_df.assign(SeatName=seats_df.SeatRow + seats_df.SeatNumber)
    return seats_df


def _pre_process_prices_df(input_json):
    prices_df = pd.DataFrame(input_json)
    if prices_df.empty:
        raise ValueError("No prices available for this performance!")
    prices_df = prices_df.query("Enabled == True").reset_index(drop=True)
    return prices_df


def _pre_process_price_types_df(input_json):
    price_types_df = pd.DataFrame(input_json)
    return price_types_df


def _pre_process_events_df(input_json):
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
    events_df.rename(
        columns={"locations": "locationId", "id": "productionId"}, inplace=True
    )
    return events_df, included_df


def do_nothing(input_json):
    return input_json


def pre_process_df(input_json, df_type):
    """
    Function selector for pre-processing the different data types
    """
    pre_process_fun = {
        "seats": _pre_process_seats_df,
        "prices": _pre_process_prices_df,
        "zone_ids": _pre_process_zone_df,
        "price_types": _pre_process_price_types_df,
        "events": _pre_process_events_df,
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
    seats_price_df = _fix_xy_positions(seats_price_df)
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
    data = {
        "seats": seats_price_df,
        "prices": prices_df,
        "zone_ids": zones_df,
        "price_types": price_types_df,
    }

    return data


class API:
    def __init__(self, query_dict, all_data={}):
        self.query_dict = query_dict
        self.all_data = all_data

    def query_all_data(self, data_types=None, post_process=False):
        """
        Query all data from the query_dict
        Args:
        query_dict: dict, keys are data types and values are dicts with keys "url" and "params"
        data_types: list, data types to query
        post_process: bool, whether to post-process the data
        all_data: dict, if not None, then this data will be used if it wasn't queried
        """
        if data_types is None:
            data_types = self.query_dict.keys()
        for data_type in force_list(data_types):
            self.all_data[data_type] = self.query_one_data(data_type)
            time.sleep(0.3)
        print(f"Queried the following from the API: {data_types}")
        if post_process:
            self.all_data = post_process_all_data(self.all_data)
        return self.all_data

    def query_one_data(self, data_type=None):
        """
        Query one type of data from the query_dict
        """
        url = self.query_dict[data_type]["url"]
        params = self.query_dict[data_type]["params"]
        json_response = requests.get(url, params=params).json()
        return pre_process_df(json_response, data_type)


def _fix_xy_positions(df):
    """
    Map the seats to their positions according to the web layout
    """
    seat_positions = pd.read_csv("various/seat_map_positions/seat_positions.csv")
    df_zones = df.ZoneName.unique()
    seat_positions.query("ZoneName in @df_zones", inplace=True)
    df.drop(columns=["x", "y"], inplace=True, errors="ignore")
    df = df.merge(seat_positions, on=["SeatName", "ZoneName"], how="left")
    df.query("x.notnull()", inplace=True)  # Remove seats with no position, e.g. aisles
    return df


# def _fix_y_positions(df):
#     """
#     Fix the YPosition so that it follows the zone hierarchy
#     """
#     df = df.assign(zone_hierarchy=df.ZoneName.map(ZONE_HIERARCHY).astype(int))
#     max_y_positions = df.groupby(["zone_hierarchy"]).YPosition.max().reset_index()
#     # Calculate cumulative sum of seats in each zone
#     max_y_positions.sort_values(by=["zone_hierarchy"], inplace=True)
#     cum_sum = max_y_positions.YPosition.cumsum()[:-1]
#     cum_sum = pd.concat([pd.Series([0]), cum_sum], axis=0).reset_index(drop=True)
#     max_y_positions["y_absolute_shift"] = cum_sum
#     max_y_positions.rename(columns={"YPosition": "max_zone_y_position"}, inplace=True)
#     # Merge the max YPosition back into the df
#     df.drop(
#         columns=["max_zone_y_position", "y_absolute_shift"],
#         inplace=True,
#         errors="ignore",
#     )
#     df = df.merge(max_y_positions, on="zone_hierarchy")
#     df["YPosition"] = df["max_zone_y_position"] - df["YPosition"]
#     df["YPosition"] = df["YPosition"] + df["y_absolute_shift"]
#     df.drop(columns=["max_zone_y_position", "y_absolute_shift"], inplace=True)
#     return df


def _query_production_activities(production_url):
    """
    production_url: str, e.g. "https://www.roh.org.uk/tickets-and-events/tosca-by-jonathan-kent-dates"
    """
    production_data = requests.get(production_url).text
    soup = BeautifulSoup(production_data, "html.parser")

    # __INTIIAL_STATE__ is a JSON object containing all the data we need
    scripts = soup.find_all("script")
    for script in scripts:
        if "__INITIAL_STATE__" in script.string:
            initial_state = script.string
            break
    initial_state = initial_state.replace("__INITIAL_STATE__=", "")[1:-1]
    initial_state = unquote(initial_state)
    initial_state = json.loads(initial_state)
    activities_df = pd.DataFrame(initial_state["activities"])
    return activities_df


def query_production_activities(production_url):
    """
    production_url: str, e.g. "https://www.roh.org.uk/tickets-and-events/tosca-by-jonathan-kent-dates"
    """
    activities_df = _query_production_activities(production_url)
    activities_df.date = pd.to_datetime(activities_df.date, utc=True)
    activities_df.date = activities_df.date.dt.tz_convert("Europe/London")
    activities_df.sort_values(by=["date"], inplace=True, ignore_index=True)
    return activities_df


def _query_soonest_performance_id(production_url):
    """
    production_url: str, e.g. "https://www.roh.org.uk/tickets-and-events/tosca-by-jonathan-kent-dates"
    """
    activities_df = query_production_activities(production_url)
    today = pd.Timestamp.today(tz="Europe/London") + pd.Timedelta(hours=1)
    activities_df.query("date >= @today", inplace=True)
    activities_df.reset_index(drop=True, inplace=True)
    soonest_performance_id = int(activities_df.id[0])
    print(f"Soonest performance_id: {soonest_performance_id}")
    return soonest_performance_id
