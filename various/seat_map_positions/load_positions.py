import pandas as pd

from set_secrets import set_secrets

from python_roh.src.src import API
from python_roh.src.utils import JSON
from python_roh.src.api import get_query_dict


def load_positions():
    # JSON file with the seat map positions from the web page using extract_positions.js
    seat_map_json = JSON("various/seat_map_positions/seat_positions.json").load()
    if seat_map_json == {}:
        print(
            "various/seat_map_positions/seat_positions.json not found!\n"
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

    csv_name = "various/seat_map_positions/seat_positions.csv"
    seat_map.to_csv(csv_name, index=False)
    print(f"Written seat map positions to {csv_name}")


if __name__ == "__main__":
    load_positions()
