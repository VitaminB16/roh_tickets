import random
import pandas as pd
import plotly.express as px

from python_roh.src.config import *
from python_roh.src.utils import JSON


def plot_hall(seats_price_df, prices_df):
    """
    Plot the hall with the seats and their prices and availability
    """
    # Get the edge seats to determine the stage position
    edge_query = "ZoneName == 'Orchestra Stalls' and SeatName.isin(['A1', 'A29'])"
    edge_seats = seats_price_df.query(edge_query)
    left_edge, right_edge = edge_seats.x.min(), edge_seats.x.max()
    middle_x, middle_y = edge_seats.x.mean(), edge_seats.y.mean()
    stage_y_min = middle_y + 120
    stage_y_max = middle_y + 60

    # Set the price bands to their colors
    price_bands = prices_df.Price.unique().astype(str)
    price_bands = ["£" + price for price in price_bands]
    price_colors = PRICE_COLOR_LIST[: len(price_bands)]
    price_color_dict = dict(zip(price_bands, price_colors))
    price_color_dict["Not available"] = NA_COLOR

    plot_df = seats_price_df.copy()
    plot_df["Size"] = 1  # Dummy constant size for scatter plot
    plot_df["Price_print"] = plot_df["Price"]
    plot_df["Price_print"] = plot_df["Price_print"].apply(lambda x: f"£{x:.0f}")
    plot_df.loc[plot_df["Price"].isnull(), "Price_print"] = "Not available"
    plot_df = plot_df.assign(symbol="circle-open")
    plot_df.loc[plot_df["Price"].isnull(), "symbol"] = "circle"
    plot_df.Price = plot_df.Price.fillna("Not available").astype(str)
    plot_df.Price = plot_df.Price.apply(lambda x: x.split(".")[0])
    plot_df["Color"] = plot_df.Price_print.map(price_color_dict)

    fig = px.scatter(
        plot_df,
        x="x",
        y="y",
        custom_data=["Price_print", "ZoneName", "SeatRow", "SeatNumber"],
        template="simple_white",
        color="Price_print",
        color_discrete_map=price_color_dict,
        category_orders={"Price_print": price_color_dict.keys()},
    )
    fig.update_xaxes(
        showticklabels=False, showline=False, zeroline=False, ticks="", title=""
    )
    fig.update_yaxes(
        showticklabels=False,
        showline=False,
        zeroline=False,
        ticks="",
        title="",
        autorange="reversed",
    )
    fig.add_shape(  # Stage rectangle
        type="rect",
        x0=left_edge,
        x1=right_edge,
        y0=stage_y_min,
        y1=stage_y_max,
        line=dict(color="rgb(135,135,135)", width=2),
        fillcolor="white",
        opacity=1,
    )
    fig.add_annotation(  # Stage label
        x=middle_x,
        y=stage_y_min / 2 + stage_y_max / 2,
        text="Stage",
        showarrow=False,
        font=dict(size=20, color="rgb(135,135,135)"),
    )
    fig.update_layout(
        autosize=False,
        width=1200,
        height=900,
        margin=dict(l=0, r=0, b=0, t=0, pad=0),
        hoverlabel=dict(
            font_size=16,
            font_family="Gotham",
            font_color="White",
            bgcolor="#C7102E",
        ),
        legend=dict(
            title="Price",
            title_font=dict(size=16),
            font=dict(size=16),
            orientation="h",
            yanchor="bottom",
            y=0.97,
            xanchor="center",
            x=0.5,
        ),
        title=None,
    )
    fig.layout.font.family = "Gotham"
    fig.update_traces(
        hovertemplate="<br>".join(
            [
                "%{customdata[0]}",
                "%{customdata[1]}",
                "Seat %{customdata[2]}%{customdata[3]}",
            ]
        )
        + "<extra></extra>",
    )

    # circle: occupied, circle-open: available
    for trace in fig.data:
        if trace.marker.color != NA_COLOR:
            trace.marker.symbol = "circle-open"
            trace.marker.line.width = 2.8
            trace.marker.size = 6.6
        else:
            trace.marker.line.width = 0
            trace.marker.size = 8

    fig.show()
    fig.write_image(HALL_IMAGE_LOCATION, scale=3)


def persist_colours(plot_df, all_colours):
    """
    Whenever new titles are added, persist the colours of the existing titles
    """
    all_colours = set(all_colours)
    existing_titles_colour = JSON(TITLE_COLOURS_LOCATION).load()
    existing_titles = set(existing_titles_colour.keys())
    upcoming_titles = set(plot_df.title.unique())
    new_titles = upcoming_titles - existing_titles
    overlapping_titles = existing_titles & upcoming_titles
    upcoming_titles_colour = {
        title: existing_titles_colour[title] for title in overlapping_titles
    }
    used_colours = set(upcoming_titles_colour.values())
    unused_colours = all_colours - used_colours
    unused_colours = list(unused_colours)[: len(new_titles)]
    random.shuffle(unused_colours)  # Randomize the leftover colours
    new_titles_colours = dict(zip(new_titles, unused_colours))
    upcoming_titles_colour.update(new_titles_colours)
    JSON(TITLE_COLOURS_LOCATION).write(upcoming_titles_colour)
    return upcoming_titles_colour


def plot_events(events_df, colours=["Plotly", "Dark2", "G10"], filter_recent=True):
    """
    Plot the timeline of the upcoming events on the Main Stage
    """
    today = pd.Timestamp.today(tz="Europe/London") - pd.Timedelta(hours=1)
    if filter_recent:
        sub_query = "location == 'Main Stage' & timestamp >= @today"
    else:
        sub_query = "location == 'Main Stage'"
    plot_df = events_df.query(sub_query).reset_index(drop=True)
    # Start time: 1:00, End time: 23:00 of the event date
    plot_df["timestamp_start"] = plot_df.timestamp.dt.floor("D") + pd.Timedelta(hours=1)
    plot_df["timestamp_end"] = plot_df.timestamp.dt.ceil("D") - pd.Timedelta(hours=1)

    plot_df["date_str"] = plot_df.timestamp.dt.strftime("%b %-d, %Y")

    colour_list = []
    for colour in colours:
        colour_list.extend(getattr(px.colors.qualitative, colour))

    # Ensure persistence of the colours
    upcoming_titles_colour = persist_colours(plot_df, set(colour_list))

    fig = px.timeline(
        plot_df,
        x_start="timestamp_start",
        x_end="timestamp_end",
        y="time",
        custom_data=["title", "url", "date_str", "performanceId"],
        color="title",
        title="Royal Opera House Events",
        template="simple_white",
        hover_name="url",
        color_discrete_map=upcoming_titles_colour,
        # Sort the legend in the order of the x axis
        category_orders={"title": plot_df.title.unique()},
    )
    # Keep first 5 characters of the y axis marks
    fig.update_yaxes(
        categoryorder="category ascending",
        showgrid=True,
        tickvals=plot_df.time.unique(),
        ticktext=[str(x)[:5] for x in plot_df.time.unique()],
        title="",
    )
    fig.update_xaxes(
        title="",
        showgrid=True,
        gridwidth=1,
        gridcolor="LightGray",
    )
    fig.update_layout(
        hovermode="closest",
        hoverdistance=1000,
        width=1700,
        height=500,
        margin=dict(l=0, r=0, b=0, t=40, pad=0),
    )
    fig.update_layout(
        hoverlabel=dict(
            font_size=16,
            font_family="Gotham",
            font_color="White",
            bgcolor="#C7102E",
        ),
        legend=dict(
            title="",
            font=dict(size=15, family="Gotham"),
            orientation="h",
        ),
        title=None,
        xaxis=dict(
            range=[
                today - pd.Timedelta(hours=5),
                plot_df.timestamp_end.max() + pd.Timedelta(days=1),
            ]
        ),
    )
    fig.layout.font.family = "Gotham"
    fig.layout.font.size = 15
    fig.update_traces(
        hovertemplate="<br>".join(
            [
                "%{customdata[0]}",
                "%{customdata[2]}",
                "%{y}",
                "ID: %{customdata[3]}",
            ],
        )
        + "<extra></extra>",
    )
    for trace in fig.data:
        trace.marker.line.color = trace.marker.color
        trace.marker.line.width = 0.2

    fig.show()
    fig.write_image(EVENTS_IMAGE_LOCATION, scale=3)
