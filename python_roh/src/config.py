import os
import argparse
import pandas as pd

from cloud.platform import PLATFORM


ZONE_HIERARCHY = {
    "Orchestra Stalls": 0,
    "Stalls Circle": 1,
    "Donald Gordon Grand Tier": 2,
    "Donald Gordon Grand Tier Boxes": 2,
    "Balcony": 3,
    "Balcony Boxes": 3,
    "Amphitheatre": 4,
    "Slips": 5,
    "WC Spaces": 6,
    "Companion Seat": 6,
}
ZONE_MAPPING = {
    "Slips": "Amphitheatre",
    "Donald Gordon Grand Tier Boxes": "Donald Gordon Grand Tier",
    "Balcony Boxes": "Balcony",
}


SEATS_BASE_URL = f"https://www.roh.org.uk/api/proxy/TXN/Performances/{os.getenv('PERFORMANCE_ID')}/Seats"
PRICE_BASE_URL = f"https://www.roh.org.uk/api/proxy/TXN/Performances/Prices"
PRICE_TYPES_BASE_URL = "https://www.roh.org.uk/api/proxy/TXN/PriceTypes/Details"
ZONE_ID_BASE_URL = (
    f"https://www.roh.org.uk/api/proxy/TXN/Performances/ZoneAvailabilities"
)
ALL_EVENTS_URL = "https://www.roh.org.uk/api/events"
TICKETS_AND_EVENTS_URL = "https://www.roh.org.uk/tickets-and-events"
SEAT_STATUSES_URL = "https://www.roh.org.uk/api/proxy/ReferenceData/SeatStatuses"
VIEW_FROM_SEAT_URL = "https://static.roh.org.uk/view-from-seat/910"

# TAKEN_SEAT_STATUS_IDS = [3, 4, 6, 7, 8, 13]
# TAKEN_SEAT_STATUS_IDS = [4, 5, 6, 7, 8, 13, 592]  # 3,
AVAILABLE_SEAT_STATUS_IDS = [0]
PRICE_COLOR_LIST = [
    "rgb(250,53,38)",  # Most expensive
    "rgb(251,32,204)",
    "rgb(47,249,254)",
    "rgb(69,104,255)",
    "rgb(253,149,46)",
    "rgb(74,154,88)",
    "rgb(237,168,249)",
    "rgb(212,56,252)",
    "rgb(31,182,244)",
    "rgb(252,24,146)",
    "rgb(0,68,136)",
    "rgb(136,204,39)",
    # Below inserted when > 12 price types
    "rgb(107,122,248)",
    "rgb(162,36,23)",
    "rgb(190,102,15)",
    "rgb(0,119,101)",
    "rgb(246,247,67)",
    "rgb(85,35,229)",  # Cheapest
]
NA_COLOR = "rgb(191,191,191)"


def parse_args(args):
    """
    Parse the command line arguments.
    Options:
    - events: query the upcoming events and plot the events timeline
    - seats: query the seats availability and plot the hall seats
        options:
        --soonest: plot the hall seats for the soonest performance
        -pid: performance_id (int) or "soonest" (str)
        -mosid: mode_of_sale_id
        --secret_function: option to use the secret function
    --no_plot: do not plot the results
    -platform: platform name (local, GCP)
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("task_name", help="Task name", choices=["events", "seats"])
    parser.add_argument(
        "--soonest",
        help="Plot the hall seats for the soonest performance",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "-pid",
        help="Performance id",
        type=lambda x: int(x) if x.isdigit() else x,
        dest="performance_id",
    )
    parser.add_argument(
        "-mosid", help="Mode of sale id", type=int, dest="mode_of_sale_id"
    )
    parser.add_argument(
        "--secret_function", help="Secret function", action="store_true", default=None
    )
    parser.add_argument(
        "--no_plot", help="Do not plot the results", action="store_true", default=None
    )
    args = parser.parse_args(args)

    output = vars(args)

    # Remove unset arguments
    output = {k: v for k, v in output.items() if v is not None}
    return output


CLOUD_BUCKET = "vitaminb16-clean/"
PUBLIC_BUCKET = "vitaminb16-public/"
PREFIX = PLATFORM.fs_prefix
PREFIX_PUBLIC = PLATFORM.fs_prefix
if PLATFORM.name != "Local":
    PREFIX = PREFIX + CLOUD_BUCKET
    PREFIX_PUBLIC = PREFIX_PUBLIC + PUBLIC_BUCKET
# Private --------------------------------
SEAT_MAP_POSITIONS_CSV = PREFIX + "metadata/seat_positions.csv"
TEXT_MAP_POSITIONS_CSV = PREFIX + "metadata/text_positions.csv"
TITLE_COLOURS_LOCATION = PREFIX + "metadata/titles_colour.json"
SEAT_STATUSES_PATH = PREFIX + "metadata/seat_statuses.json"
SEAT_POSITIONS_JSON_LOCATION = PREFIX + "metadata/seat_positions.json"
EVENTS_PARQUET_LOCATION = PREFIX + "output/roh_events.parquet"
PRODUCTIONS_PARQUET_LOCATION = PREFIX + "output/roh_productions.parquet"
SOONEST_PERFORMANCES_LOCATION = PREFIX + "metadata/soonest_performances.json"
# Public  --------------------------------
HALL_IMAGE_LOCATION = PREFIX_PUBLIC + "output/images/ROH_hall.png"
EVENTS_IMAGE_LOCATION = PREFIX_PUBLIC + "output/images/ROH_events.png"

EVENTS_PARQUET_SCHEMA = {
    "date": [
        lambda x: pd.to_datetime(x, format="%Y-%m-%d").dt.date,
        lambda x: pd.to_datetime(x, unit="ms").dt.date,
        lambda x: pd.to_datetime(x, format="%Y-%m-%d").date,
        lambda x: pd.to_datetime(x, unit="ms").date,
    ],
    "time": [
        lambda x: pd.to_datetime(x, format="%H:%M:%S.000000").dt.time,
        lambda x: pd.to_datetime(x, format="%H:%M:%S").dt.time,
        lambda x: pd.to_datetime(x, unit="ms").dt.time,
        lambda x: pd.to_datetime(x, format="%H:%M:%S.000000").time,
        lambda x: pd.to_datetime(x, format="%H:%M:%S").time,
        lambda x: pd.to_datetime(x, unit="ms").time,
    ],
    "timestamp": [
        lambda x: pd.to_datetime(x, unit="ms", utc=True).dt.tz_convert("Europe/London"),
        lambda x: pd.to_datetime(x, unit="ms", utc=True).tz_convert("Europe/London"),
    ],
    "performanceId": lambda x: x.astype(str),
    "productionId": lambda x: x.astype(int),
}
PRODUCTIONS_PARQUET_SCHEMA = {
    "date": [
        lambda x: pd.to_datetime(x, format="%Y-%m-%d").dt.date,
        lambda x: pd.to_datetime(x, format="%Y-%m-%d").date,
    ],
    "time": [
        lambda x: pd.to_datetime(x, format="%H:%M:%S.000000").dt.time,
        lambda x: pd.to_datetime(x, format="%H:%M:%S").dt.time,
        lambda x: pd.to_datetime(x, format="%H:%M:%S.000000").time,
        lambda x: pd.to_datetime(x, format="%H:%M:%S").time,
    ],
    "performanceId": lambda x: x.astype(str),
    "productionId": lambda x: x.astype(int),
}

PARQUET_SCHEMAS = {
    EVENTS_PARQUET_LOCATION: EVENTS_PARQUET_SCHEMA,
    PRODUCTIONS_PARQUET_LOCATION: PRODUCTIONS_PARQUET_SCHEMA,
}
FIRESTORE_SCHEMAS = {k.replace(PREFIX, ""): v for k, v in PARQUET_SCHEMAS.items()}

PARQUET_TABLE_RELATIONS = {
    EVENTS_PARQUET_LOCATION: "clean.v_roh_events",
    PRODUCTIONS_PARQUET_LOCATION: "clean.v_roh_productions",
}
