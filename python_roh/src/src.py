import os
import json
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import unquote

from python_roh.src.config import (
    SEAT_MAP_POSITIONS_CSV,
    AVAILABLE_SEAT_STATUS_IDS,
    PRODUCTIONS_PARQUET_LOCATION,
    ZONE_HIERARCHY,
    ZONE_MAPPING,
    SEAT_STATUSES_PATH,
    VIEW_FROM_SEAT_URL,
)
from cloud.utils import log
from cloud.platform import PLATFORM
from tools import Parquet, Firestore
from python_roh.src.utils import force_list


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
    seat_slug = seats_df.apply(
        lambda x: f"{x.SeatNumber}-{x.SeatRow}-{x.ScreenId}", axis=1
    )
    seats_view_url = seat_slug.apply(
        lambda x: f"{VIEW_FROM_SEAT_URL}/seat-{x.replace(' ', '_')}.jpg"
    )
    seats_df = seats_df.assign(SeatsViewUrl=seats_view_url)
    return seats_df


def _pre_process_prices_df(input_json):
    prices_df = pd.DataFrame(input_json)
    if prices_df.empty:
        raise ValueError("No prices available for this performance!")
    prices_df.loc[~prices_df.Enabled, "Price"] = None
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


def post_process_all_data(data, data_types=None, available_seat_status_ids=None):
    """
    Merge the different dataframes together and do some post-processing
    """
    available_seat_status_ids = available_seat_status_ids or AVAILABLE_SEAT_STATUS_IDS
    seats_df, prices_df, zones_df, price_types_df = (
        data["seats"],
        data["prices"],
        data["zone_ids"],
        data["price_types"],
    )
    seats_price_df = seats_df.merge(prices_df, on="ZoneId")
    seats_price_df = seats_price_df.merge(zones_df, on="ZoneId")
    seats_price_df = seats_price_df.assign(
        ZoneNameGeneral=seats_price_df.ZoneName.apply(lambda x: ZONE_MAPPING.get(x, x))
    )
    seats_price_df.query("ZoneNameGeneral in @ZONE_HIERARCHY.keys()", inplace=True)

    # Fix YPosition so that it follows the zone hierarchy
    seats_price_df = _fix_xy_positions(seats_price_df)
    seats_price_df = enrich_seats_price_df(seats_price_df)
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
        seat_available=(seats_price_df.SeatStatusId.isin(available_seat_status_ids))
    )
    data = {
        "seats": seats_price_df,
        "prices": prices_df,
        "zone_ids": zones_df,
        "price_types": price_types_df,
    }

    return data


def enrich_seats_price_df(seats_price_df):
    """
    Enrich the seats_price_df with additional columns
    """
    seats_price_df = enrich_seats_price_statuses(seats_price_df)
    return seats_price_df


def enrich_seats_price_statuses(seats_price_df):
    """
    Enrich the seats_price_df with the seat statuses
    """
    seat_statuses = load_statuses_df()["seat_statuses"]
    seats_statuses_df = pd.DataFrame(seat_statuses)
    seats_statuses_df = seats_statuses_df.assign(
        SeatStatusStr=seats_statuses_df.apply(
            lambda x: f"{x.Id} ({x.StatusCode})", axis=1
        )
    )

    seats_statuses_df.rename(columns={"Id": "SeatStatusId"}, inplace=True)
    cols_to_drop = set(seats_statuses_df.columns) - {"SeatStatusId", "SeatStatusStr"}
    seats_statuses_df.drop(columns=cols_to_drop, inplace=True, errors="raise")
    seats_price_df = seats_price_df.merge(
        seats_statuses_df, on="SeatStatusId", how="left"
    )
    not_enabled = (seats_price_df.SeatStatusId == 0) & (seats_price_df.Price.isnull())
    seats_price_df.loc[not_enabled, "SeatStatusStr"] = "Not enabled"
    return seats_price_df


def load_statuses_df(errors="ignore"):
    """
    Load the seat statuses from Firestore
    """
    try:
        seat_statuses_df = Firestore(SEAT_STATUSES_PATH).read(allow_empty=False)
    except Exception as e:
        if errors == "raise":
            raise e
        log(f"Error reading Firestore. Loading the seat to Firestore...")
        from various.seat_statuses.load_seat_statuses import load_seat_statuses

        success = load_seat_statuses()
        if success:
            load_statuses_df(errors="raise")
        else:
            raise ValueError("Failed to load seat statuses to Firestore")
    return seat_statuses_df


class API:
    def __init__(self, query_dict, all_data={}):
        self.query_dict = query_dict
        self.all_data = all_data

    def query_all_data(
        self,
        data_types=None,
        post_process=False,
        available_seat_status_ids=None,
        **kwargs,
    ):
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
            time.sleep(0.01)
        log(f"Queried the following from the API: {data_types}")
        if post_process:
            self.all_data = post_process_all_data(
                self.all_data, available_seat_status_ids=available_seat_status_ids
            )
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
    log("Fixing the seat positions")
    log(SEAT_MAP_POSITIONS_CSV)

    # Check if SEAT_MAP_POSITIONS_CSV exists
    if not PLATFORM.exists(SEAT_MAP_POSITIONS_CSV):
        log(
            f"File {SEAT_MAP_POSITIONS_CSV} does not exist! Running load_positions.py..."
        )
        from various.seat_map_positions.load_positions import load_positions

        load_positions()
    seat_positions = Firestore(SEAT_MAP_POSITIONS_CSV).read(apply_schema=True)
    seat_positions.rename(columns={"ZoneName": "ZoneNameGeneral"}, inplace=True)
    log(f"Seat positions: {seat_positions.shape}")
    df_zones = df.ZoneNameGeneral.unique()
    seat_positions.query("ZoneNameGeneral in @df_zones", inplace=True)
    df.drop(columns=["x", "y"], inplace=True, errors="ignore")
    df = df.merge(seat_positions, on=["SeatName", "ZoneNameGeneral"], how="left")
    df.query("x.notnull()", inplace=True)  # Remove seats with no position, e.g. aisles
    return df


def _query_production_activities(production_url):
    """
    production_url: str, e.g. "https://www.rbo.org.uk/tickets-and-events/tosca-by-jonathan-kent-dates"
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
    production_url: str, e.g. "https://www.rbo.org.uk/tickets-and-events/tosca-by-jonathan-kent-dates"
    """
    activities_df = _query_production_activities(production_url)
    activities_df.date = pd.to_datetime(activities_df.date, utc=True)
    activities_df.date = activities_df.date.dt.tz_convert("Europe/London")
    activities_df.sort_values(by=["date"], inplace=True, ignore_index=True)
    return activities_df


def _query_soonest_performance_id(production_url):
    """
    production_url: str, e.g. "https://www.rbo.org.uk/tickets-and-events/tosca-by-jonathan-kent-dates"
    """
    activities_df = query_production_activities(production_url)
    today = pd.Timestamp.today(tz="Europe/London") + pd.Timedelta(hours=1)
    activities_df.query("date >= @today", inplace=True)
    activities_df.reset_index(drop=True, inplace=True)
    soonest_performance_id = int(activities_df.id[0])
    log(f"Soonest performance_id: {soonest_performance_id}")
    return soonest_performance_id


def print_performance_info(
    performance_id=None, print_info=True, print_performance_info=True, **kwargs
):
    if not print_performance_info:
        return None
    performance_id = (
        os.environ["PERFORMANCE_ID"]
        if "soonest" in str(performance_id) or performance_id is None
        else performance_id
    )
    performance_id = json.loads(str(performance_id))
    performance_id = [int(x) for x in force_list(performance_id)]
    performance_df = (
        Parquet(PRODUCTIONS_PARQUET_LOCATION)
        .read(filters={"performanceId": performance_id})
        .sort_values(by=["date", "time"], ascending=True)
    )
    if not print_info:
        return performance_df

    for i in range(performance_df.shape[0]):
        log(
            f"""
            {performance_df.title.iloc[i]}
            {performance_df.date.iloc[i].strftime('%b %-d, %Y')}
            {performance_df.time.iloc[i]}
            ID: {performance_df.performanceId.iloc[i]}
            """
        )
    return performance_df
