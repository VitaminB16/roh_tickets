import os
import sys
import plotly.express as px

from set_secrets import set_secrets
from python_roh.src.api import get_query_dict
from python_roh.src.src import query_all_data
from python_roh.src.graphics import hall_plot

if "src_secret.py" in os.listdir("python_roh/src"):
    from python_roh.src.src_secret import secret_function


def main():
    performance_id, mode_of_sale_id, constituent_id, source_id = (
        os.environ["PERFORMANCE_ID"],
        os.environ["MODE_OF_SALE_ID"],
        os.environ["CONSTITUENT_ID"],
        os.environ["SOURCE_ID"],
    )

    global QUERY_DICT  # Querying all data relating to the performance
    QUERY_DICT = get_query_dict(
        performance_id=performance_id,
        constituent_id=constituent_id,
        mode_of_sale_id=mode_of_sale_id,
        source_id=source_id,
    )
    all_data = query_all_data(QUERY_DICT)

    seats_price_df, prices_df, zones_df, price_types_df = (
        all_data["seats_price"],
        all_data["prices"],
        all_data["zones"],
        all_data["price_types"],
    )

    hall_plot(seats_price_df)


if __name__ == "__main__":
    args = sys.argv[1:]
    main()
    if "secret_function" in globals() and "secret_function" in args:
        secret_function(QUERY_DICT)
