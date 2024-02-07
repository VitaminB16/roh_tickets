import os
import argparse

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


SEATS_BASE_URL = f"https://www.roh.org.uk/api/proxy/TXN/Performances/{os.environ['PERFORMANCE_ID']}/Seats"
PRICE_BASE_URL = f"https://www.roh.org.uk/api/proxy/TXN/Performances/Prices"
PRICE_TYPES_BASE_URL = "https://www.roh.org.uk/api/proxy/TXN/PriceTypes/Details"
ZONE_ID_BASE_URL = (
    f"https://www.roh.org.uk/api/proxy/TXN/Performances/ZoneAvailabilities"
)
ALL_EVENTS_URL = "https://www.roh.org.uk/api/events"
TICKETS_AND_EVENTS_URL = "https://www.roh.org.uk/tickets-and-events"

# TAKEN_SEAT_STATUS_IDS = [3, 4, 6, 7, 8, 13]
TAKEN_SEAT_STATUS_IDS = [4, 5, 6, 7, 8, 13, 592]  # 3,

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
    "rgb(136,204,39)",  # Cheapest
]
NA_COLOR = "rgb(191,191,191)"


def parse_args(args):
    """
    Parse the command line arguments.
    Options:
    - upcoming: query the upcoming events and plot the events timeline
    - seats: query the seats availability and plot the hall seats
        options:
        --soonest: plot the hall seats for the soonest performance
        -pid: performance_id (int) or "soonest" (str)
        -mosid: mode_of_sale_id
        --secret_function: option to use the secret function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("task_name", help="Task name", choices=["upcoming", "seats"])
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
    args = parser.parse_args(args)

    output = vars(args)

    # Remove unset arguments
    output = {k: v for k, v in output.items() if v is not None}
    return output
