import os
import argparse

ZONE_HIERARCHY = {
    "Orchestra Stalls": 0,
    "Stalls Circle": 1,
    "Donald Gordon Grand Tier": 2,
    "Balcony": 3,
    "Amphitheatre": 4,
}

SEATS_BASE_URL = f"https://www.roh.org.uk/api/proxy/TXN/Performances/{os.environ['PERFORMANCE_ID']}/Seats"
PRICE_BASE_URL = f"https://www.roh.org.uk/api/proxy/TXN/Performances/Prices"
PRICE_TYPES_BASE_URL = "https://www.roh.org.uk/api/proxy/TXN/PriceTypes/Details"
ZONE_ID_BASE_URL = (
    f"https://www.roh.org.uk/api/proxy/TXN/Performances/ZoneAvailabilities"
)
ALL_EVENTS_URL = "https://www.roh.org.uk/api/events"
TICKETS_AND_EVENTS_URL = "https://www.roh.org.uk/tickets-and-events"

TAKEN_SEAT_STATUS_IDS = [3, 4, 6, 7, 8, 13]


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
    args = parser.parse_args(args)

    output = vars(args)

    # Remove unset arguments
    output = {k: v for k, v in output.items() if v is not None}
    return output
