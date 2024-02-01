import os
import sys
import plotly.express as px

from set_secrets import set_secrets
from python_roh.src.src import get_query_dict, query_all_data

if "src_secret.py" in os.listdir("python_roh/src"):
    from python_roh.src.src_secret import secret_function

def hall_plot(seats_price_df):
    seats_price_df["Size"] = 1  # Dummy constant size for scatter plot

    fig = px.scatter(
        seats_price_df,
        x="XPosition",
        y="YPosition",
        color="Price",
        size="Size",
        size_max=8.5,
        color_continuous_scale=[
            [0.0, "rgb(136,204,39)"],
            [0.5, "rgb(253,149,45)"],
            [0.77, "rgb(69,104,255)"],
            [0.89, "rgb(47,248,254)"],
            [0.96, "rgb(251,32,203)"],
            [1.0, "rgb(250,53,38)"],
        ],
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
        height=900,
        coloraxis=dict(
            colorbar=dict(
                tickmode="linear", dtick=25, ticks="inside", len=0.5, thickness=15
            )
        ),
        margin=dict(l=0, r=0, b=0, t=0, pad=0),
        # coloraxis_showscale=False,
    )
    fig.update_coloraxes(
        colorbar=dict(
            tickmode="linear", dtick=25, ticks="inside", orientation="h", x=0.46, y=0.94
        )
    )
    fig.add_shape(
        type="rect",
        x0=58.5,
        y0=-8,
        x1=22.5,
        y1=-3,
        line=dict(color="black", width=2),
        fillcolor="white",
        opacity=1,
    )
    # Add text for stage
    fig.add_annotation(
        x=40.5,
        y=-5.5,
        text="Stage",
        showarrow=False,
        font=dict(size=20, color="black"),
    )
    fig.update_traces(marker=dict(line=dict(width=0.5, color="DarkSlateGrey")))
    # Set the legend horizontally at the bottom
    fig.update_layout()
    fig.show()

    fig.write_image("ROH_hall.png", scale=3)


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
