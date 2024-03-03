import requests

from python_roh.src.src import API
from tools.firestore import Firestore
from python_roh.src.config import SEAT_STATUSES_URL, SEAT_STATUSES_PATH


def load_seat_statuses():
    """
    Load the seat statuses for a given performance
    """
    query_dict = {
        "seat_statuses": {
            "url": SEAT_STATUSES_URL,
            "params": {},
        }
    }
    seat_statuses = API(query_dict).query_all_data()
    Firestore(SEAT_STATUSES_PATH).write(seat_statuses)
    return True


if __name__ == "__main__":
    load_seat_statuses()
