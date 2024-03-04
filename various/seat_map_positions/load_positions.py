import json
import pandas as pd

from python_roh.set_secrets import set_secrets

from cloud.utils import log
from python_roh.src.src import API
from python_roh.src.utils import JSON
from tools.firestore import Firestore
from python_roh.src.api import get_query_dict
from python_roh.src.config import (
    SEAT_MAP_POSITIONS_CSV,
    SEAT_POSITIONS_JSON_LOCATION,
    PREFIX,
)


def load_positions():
    # JSON file with the seat map positions from the web page using extract_positions.js
    try:
        seat_map_json = JSON(SEAT_POSITIONS_JSON_LOCATION).load(allow_empty=False)
    except OSError:
        local_seat_path = SEAT_POSITIONS_JSON_LOCATION.replace(PREFIX, "")
        with open(local_seat_path, "r") as f:
            json_file = json.load(f)
        Firestore(SEAT_POSITIONS_JSON_LOCATION).write(json_file)
        seat_map_json = Firestore(SEAT_POSITIONS_JSON_LOCATION).read()

    if seat_map_json == {}:
        log(
            f"{SEAT_POSITIONS_JSON_LOCATION} not found!\n"
            + "Make sure to run extract_positions.js first."
        )
    seat_map = pd.DataFrame(seat_map_json)
    seat_map.rename(columns={"cx": "x", "cy": "y"}, inplace=True)

    # Get the seats data from the API
    query_dict = get_query_dict()
    seats_df = API(query_dict).query_one_data("seats")
    seats_df = seats_df.loc[:, ["SeatId", "SeatName"]]

    # Handle two type of seats in the seat map
    seat_map_syos = seat_map.query("id.str.contains('syos-')")
    seat_map_other = seat_map.query("~id.str.contains('syos-')")

    seat_map_syos.loc[:, "id"] = seat_map_syos.id.str.replace("syos-custom-icon-", "")
    seat_map_syos.loc[:, "id"] = seat_map_syos.id.astype(int)

    seat_map_syos = seat_map_syos.merge(
        seats_df, left_on="id", right_on="SeatId", how="left"
    )
    seat_map_syos.drop(columns=["id"], inplace=True)

    # "seat-13-A-1" -> "A13"
    seat_map_other = seat_map_other.assign(
        SeatName=seat_map_other.id.str.split("-").str[2:0:-1].str.join("")
    )

    # Merge the two types of seats back together
    seat_map_syos.drop(columns=["SeatId"], inplace=True, errors="ignore")
    seat_map_syos.x = seat_map_syos.x.astype(float) + 4.9
    seat_map_syos.y = seat_map_syos.y.astype(float) + 5.35

    seat_map_other.drop(columns=["id"], inplace=True)
    seat_map_other.x = seat_map_other.x.astype(float)
    seat_map_other.y = seat_map_other.y.astype(float)

    seat_map = pd.concat([seat_map_syos, seat_map_other], axis=0)
    seat_map.x = seat_map.x.astype(float)
    seat_map.y = seat_map.y.astype(float)

    csv_name = SEAT_MAP_POSITIONS_CSV
    seat_map.to_csv(csv_name, index=False)
    Firestore(SEAT_MAP_POSITIONS_CSV).write(seat_map)
    log(f"Written seat map positions to {csv_name}")


if __name__ == "__main__":
    load_positions()
