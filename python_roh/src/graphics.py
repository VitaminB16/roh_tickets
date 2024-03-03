import random
import pandas as pd
import plotly.express as px

from cloud.utils import log
from python_roh.src.config import *
from tools.firestore import Firestore
from python_roh.src.utils import JSON, purge_image_cache


class Graphics:
    def __init__(self, plot_type, **kwargs):
        self.plot_type = plot_type

    def plot(self, *args, **kwargs):
        plot_function = globals().get(f"plot_{self.plot_type}", None)
        if plot_function is None:
            raise ValueError(f"Invalid plot type: {self.plot_type}")
        fig = plot_function(*args, **kwargs)
        if kwargs.get("dont_save", False):
            purge_image_cache()
        return fig


def process_hall_plot_df(seats_price_df, prices_df):
    """
    Process the hall plot dataframe to include the price bands and availability
    """
    # Set the price bands to their colors
    price_bands = prices_df.Price.unique()
    price_bands.sort()
    price_bands = price_bands[::-1].astype(str)
    price_bands = ["£" + price for price in price_bands]
    price_bands = [price.split(".")[0] for price in price_bands]
    price_colors = PRICE_COLOR_LIST[: len(price_bands)]
    price_color_dict = dict(zip(price_bands, price_colors))
    price_color_dict["Not available"] = NA_COLOR

    plot_df = seats_price_df.copy()
    plot_df.Price = plot_df.Price.where(plot_df.seat_available, None)
    plot_df["Size"] = 1  # Dummy constant size for scatter plot
    plot_df["Price_print"] = plot_df["Price"]
    plot_df["Price_print"] = plot_df["Price_print"].apply(
        lambda x: f"£{x}".split(".")[0]
    )
    plot_df.loc[plot_df["Price"].isnull(), "Price_print"] = "Not available"
    plot_df = plot_df.assign(symbol="circle-open")
    plot_df.loc[plot_df["Price"].isnull(), "symbol"] = "circle"
    plot_df.Price = plot_df.Price.fillna("Not available").astype(str)
    plot_df.Price = plot_df.Price.apply(lambda x: x.split(".")[0])
    plot_df["Color"] = plot_df.Price_print.map(price_color_dict)
    return plot_df, price_color_dict


def plot_hall(
    seats_price_df,
    prices_df,
    no_plot=False,
    dark_mode=False,
    save_both=True,
    font_family="Gotham SSm-Book",
    dont_save=False,
    dont_show=False,
    autosize=False,
    **kwargs,
):
    """
    Plot the hall with the seats and their prices and availability
    """
    plot_width, plot_height = (1200, 900) if not autosize else (None, None)
    if no_plot:
        log("Skipping the plot")
        return
    # Get the edge seats to determine the stage position
    edge_query = "ZoneName == 'Orchestra Stalls' and SeatName.isin(['A1', 'A29'])"
    edge_seats = seats_price_df.query(edge_query)
    left_edge, right_edge = edge_seats.x.min(), edge_seats.x.max()
    middle_x, middle_y = edge_seats.x.mean(), edge_seats.y.mean()
    stage_y_min = middle_y + 120
    stage_y_max = middle_y + 60
    plot_df, price_color_dict = process_hall_plot_df(seats_price_df, prices_df)

    fig = px.scatter(
        plot_df,
        x="x",
        y="y",
        custom_data=["Price_print", "ZoneName", "SeatRow", "SeatNumber", "SeatStatusId"],
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
        fillcolor="rgba(0,0,0,0)",
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
        width=plot_width,
        height=plot_height,
        autosize=autosize,
        margin=dict(l=0, r=0, b=0, t=0, pad=0),
        hoverlabel=dict(
            font_size=16,
            font_family=font_family,
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
    fig.layout.font.family = font_family
    fig.update_traces(
        hovertemplate="<br>".join(
            [
                "%{customdata[0]}",
                "%{customdata[1]}",
                "Seat %{customdata[2]}%{customdata[3]}",
                "(Status: %{customdata[4]})",
            ]
        )
        + "<extra></extra>",
    )

    # circle: occupied, circle-open: available
    for trace in fig.data:
        if trace.marker.color != NA_COLOR and not dark_mode:
            trace.marker.symbol = "circle-open"
            trace.marker.line.width = 2.8
            trace.marker.size = 6.6
        else:
            trace.marker.line.width = 0
            trace.marker.size = 8

    if save_both:
        plot_hall(
            seats_price_df,
            prices_df,
            no_plot=no_plot,
            dark_mode=not dark_mode,
            save_both=False,  # Prevent infinite recursion
            dont_show=dont_show,
            dont_save=dont_save,
            plot_width=plot_width,
            autosize=autosize,
            **kwargs,
        )

    image_location = HALL_IMAGE_LOCATION
    if dark_mode:
        fig.update_layout(
            plot_bgcolor="#0E1117",
            paper_bgcolor="#0E1117",
        )
        fig.layout.font.color = "white"
        image_location = image_location.replace(".png", "_dark.png")

    if not dont_show:
        fig.show()

    if dont_save:
        return fig

    with PLATFORM.open(image_location, "wb", content_type="image/png") as f:
        f.write(fig.to_image(format="png", scale=3))
    log(f"Saved {image_location}")

    return fig


def persist_colours(plot_df, all_colours):
    """
    Whenever new titles are added, persist the colours of the existing titles
    """
    all_colours = set(all_colours)
    existing_titles_colour = Firestore(TITLE_COLOURS_LOCATION).read(allow_empty=True)
    load_from_json = existing_titles_colour == {}
    if load_from_json:
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
    if load_from_json:
        JSON(TITLE_COLOURS_LOCATION).write(upcoming_titles_colour)
    Firestore(TITLE_COLOURS_LOCATION).write(upcoming_titles_colour)
    return upcoming_titles_colour


def plot_events(
    events_df,
    colours=["Plotly", "Dark2", "G10"],
    filter_recent=True,
    no_plot=False,
    dark_mode=False,
    save_both=True,
    font_family="GothamSSm-Book",
    dont_save=False,
    dont_show=False,
    autosize=False,
    **kwargs,
):
    """
    Plot the timeline of the upcoming events on the Main Stage
    """
    # All used columns: title, timestamp, location, url, performanceId, time
    plot_width, plot_height = (1200, 500) if not autosize else (None, None)
    if no_plot:
        log("Skipping the plot")
        return

    today = pd.Timestamp.today(tz="Europe/London")
    if filter_recent:
        sub_query = "location == 'Main Stage' & timestamp >= @today"
    else:
        sub_query = "location == 'Main Stage'"
    plot_df = events_df.query(sub_query).reset_index(drop=True)
    # Start time: 1:00, End time: 23:00 of the event date
    plot_df["timestamp_start"] = plot_df.timestamp.dt.floor("D") + pd.Timedelta(hours=1)
    plot_df["timestamp_end"] = plot_df.timestamp.dt.ceil("D") - pd.Timedelta(hours=1)

    plot_df["date_str"] = plot_df.timestamp.dt.strftime("%A, %b %-d, %Y")

    colour_list = []
    for colour in colours:
        colour_list.extend(getattr(px.colors.qualitative, colour))

    # Ensure persistence of the colours
    upcoming_titles_colour = persist_colours(plot_df, set(colour_list))
    plot_df.sort_values("timestamp_start", inplace=True)

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
        width=plot_width,
        height=plot_height,
        autosize=autosize,
        margin=dict(l=0, r=0, b=0, t=40, pad=0),
    )
    fig.update_layout(
        hoverlabel=dict(
            font_size=16,
            font_family=font_family,
            font_color="White",
            bgcolor="#C7102E",
        ),
        legend=dict(
            title="",
            font=dict(size=15, family=font_family),
            orientation="h",
        ),
        title=None,
        xaxis=dict(
            range=[
                today - pd.Timedelta(hours=6),
                plot_df.timestamp_end.max() + pd.Timedelta(days=1),
            ]
        ),
    )
    # Add "Updated at: date time" to the top right corner
    fig.add_annotation(
        text=f"Updated at: {today.strftime('%b %-d, %Y (%-I:%M %p)')}",
        xref="paper",
        yref="paper",
        x=1,
        y=1.07,
        showarrow=False,
        font=dict(size=16, family=font_family),
    )
    fig.layout.font.family = font_family
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

    image_location = EVENTS_IMAGE_LOCATION

    if save_both:
        plot_events(
            events_df,
            colours=colours,
            filter_recent=filter_recent,
            no_plot=no_plot,
            dark_mode=not dark_mode,
            font_family=font_family,
            save_both=False,  # Prevent infinite recursion
            dont_show=dont_show,
            dont_save=dont_save,
            plot_width=plot_width,
            **kwargs,
        )

    if dark_mode:
        fig.update_layout(
            plot_bgcolor="#0E1117",
            paper_bgcolor="#0E1117",
        )
        fig.layout.xaxis.gridcolor = "#3D3630"
        fig.layout.yaxis.gridcolor = "#3D3630"
        fig.layout.font.color = "white"
        image_location = image_location.replace(".png", "_dark.png")

    if not dont_show:
        fig.show()

    if dont_save:
        return fig

    with PLATFORM.open(image_location, "wb", content_type="image/png") as f:
        f.write(fig.to_image(format="png", scale=3))
    log(f"Saved {image_location}")

    return fig
