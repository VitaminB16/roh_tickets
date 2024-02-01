import os
import sys
import plotly.express as px

from set_secrets import set_secrets
from src.src import get_query_dict, query_all_data

if "src_secret.py" in os.listdir("src"):
    from src.src_secret import secret_function


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

    seats_price_df["Size"] = 1  # Dummy constant size for scatter plot

    fig = px.scatter(
        seats_price_df,
        x="XPosition",
        y="YPosition",
        color="Price",
        size="Size",
        size_max=9,
        color_continuous_scale="jet",
        hover_data=["Price", "ZoneName", "SeatRow", "SeatNumber"],
        template="simple_white",
    )
    fig.update_xaxes(
        showticklabels=False, showline=False, zeroline=False, ticks="", title=""
    )
    fig.update_yaxes(
        showticklabels=False, showline=False, zeroline=False, ticks="", title=""
    )
    fig.update_layout(
        autosize=False,
        width=800,
        height=700,
        margin=dict(l=0, r=0, b=0, t=0, pad=0),
        coloraxis_showscale=False,
    )
    fig.show()


if __name__ == "__main__":
    args = sys.argv[1:]
    main()
    if "secret_function" in globals() and "secret_function" in args:
        secret_function(QUERY_DICT)
