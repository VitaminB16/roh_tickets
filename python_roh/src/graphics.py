import pandas as pd
import plotly.express as px


def plot_hall(seats_price_df):
    """
    Plot the hall with the seats and their prices and availability
    """
    plot_df = seats_price_df.copy()

    plot_df["Size"] = 1  # Dummy constant size for scatter plot
    plot_df["Price_print"] = plot_df["Price"]
    plot_df["Price_print"] = plot_df["Price_print"].apply(lambda x: f"£{x:.0f}")
    plot_df.loc[plot_df["Price"].isnull(), "Price_print"] = "Not available"

    fig = px.scatter(
        plot_df,
        x="XPosition",
        y="YPosition",
        color="Price",
        size="Size",
        custom_data=["Price_print", "ZoneName", "SeatRow", "SeatNumber"],
        size_max=8.5,
        color_continuous_scale=[
            [0.0, "rgb(136,204,39)"],
            [0.5, "rgb(253,149,45)"],
            [0.77, "rgb(69,104,255)"],
            [0.89, "rgb(47,248,254)"],
            [0.96, "rgb(251,32,203)"],
            [1.0, "rgb(250,53,38)"],
        ],
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
    fig.update_layout(
        hoverlabel=dict(
            font_size=16,
            font_family="Gotham",
            font_color="White",
            bgcolor="#C7102E",
        ),
        legend=dict(
            title="Title",
            title_font=dict(size=15, family="Gotham"),
            font=dict(size=13, family="Gotham"),
        ),
        title=None,
    )
    fig.layout.font.family = "Gotham"
    fig.update_traces(marker=dict(line=dict(width=0.5, color="DarkSlateGrey")))
    fig.update_traces(
        hovertemplate="<br>".join(
            [
                "%{customdata[0]}",
                "%{customdata[1]}",
                "Seat %{customdata[2]}%{customdata[3]}",
            ],
        ),
    )
    # Set the legend horizontally at the bottom
    fig.update_layout()
    fig.show()
    # fig.write_image("output/ROH_hall.png", scale=3)


def plot_events(events_df):
    """
    Plot the timeline of the upcoming events on the Main Stage
    """
    today = pd.Timestamp.today(tz="Europe/London") - pd.Timedelta(hours=1)
    events_df_sub = events_df.query(
        "location == 'Main Stage' & date >= @today.date()"
    ).reset_index(drop=True)
    events_df_sub["Size"] = 1  # set the size of the dots
    fig = px.scatter(
        events_df_sub,
        custom_data=["title", "url"],
        x="date",
        y="time",
        color="title",
        size="Size",
        size_max=7,
        title="Royal Opera House Events",
        template="simple_white",
        hover_name="url",
    )
    # Keep first 5 characters of the y axis marks
    fig.update_yaxes(
        categoryorder="category ascending",
        showgrid=True,
        tickvals=events_df_sub.time.unique(),
        ticktext=[str(x)[:5] for x in events_df_sub.time.unique()],
        title="",
    )
    fig.update_xaxes(
        title="",
        showgrid=True,
        gridwidth=1,
        gridcolor="LightGray",
    )
    fig.update_layout(
        width=1700,
        height=500,
        margin=dict(l=0, r=0, b=0, t=40, pad=0),
    )
    fig.update_traces(
        marker=dict(line=dict(width=1, color="DarkSlateGrey")),
        selector=dict(mode="markers"),
    )
    fig.update_layout(
        hoverlabel=dict(
            font_size=16,
            font_family="Gotham",
            font_color="White",
            bgcolor="#C7102E",
        ),
        legend=dict(
            title="Title",
            title_font=dict(size=15, family="Gotham"),
            font=dict(size=13, family="Gotham"),
        ),
        title=None,
    )
    fig.layout.font.family = "Gotham"
    # Remove Size from the hover data
    fig.update_traces(
        hovertemplate="<br>".join(
            [
                "%{customdata[0]}",
                "%{x}",
                "%{y}",
            ],
        )
        + "<extra></extra>",
    )
    fig.show()
