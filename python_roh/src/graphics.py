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
        custom_data=["Price_print", "ZoneName", "SeatRow", "SeatNumber"],
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
        # change shape to square
        marker=dict(
            symbol="square",
            size=8.9,
            opacity=0.8,
            line=dict(width=0.5, color="DarkSlateGrey"),
        ),
    )
    fig.update_layout()
    fig.show()
    # fig.write_image("output/ROH_hall.png", scale=3)


def plot_events(events_df, colour1="T10", colour2="Dark24"):
    """
    Plot the timeline of the upcoming events on the Main Stage
    """
    today = pd.Timestamp.today(tz="Europe/London") - pd.Timedelta(hours=1)
    events_df_sub = events_df.query(
        "location == 'Main Stage' & date >= @today.date()"
    ).reset_index(drop=True)
    # Start time: 1:00, End time: 23:00 of the event date
    events_df_sub["timestamp_start"] = events_df_sub.timestamp.dt.floor(
        "D"
    ) + pd.Timedelta(hours=1)
    events_df_sub["timestamp_end"] = events_df_sub.timestamp.dt.ceil(
        "D"
    ) - pd.Timedelta(hours=1)

    events_df_sub["date_str"] = events_df_sub.timestamp.dt.strftime("%b %-d, %Y")

    colour1_list = getattr(px.colors.qualitative, colour1)
    colour2_list = getattr(px.colors.qualitative, colour2)
    combined = colour1_list + colour2_list
    fig = px.timeline(
        events_df_sub,
        x_start="timestamp_start",
        x_end="timestamp_end",
        y="time",
        custom_data=["title", "url", "date_str"],
        color="title",
        title="Royal Opera House Events",
        template="simple_white",
        hover_name="url",
        color_discrete_sequence=combined,
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
            title_font=dict(size=15, family="Gotham"),
            font=dict(size=13, family="Gotham"),
            orientation="h",
        ),
        title=None,
        xaxis=dict(
            range=[
                today,
                events_df_sub.timestamp_end.max() + pd.Timedelta(days=1),
            ]
        ),
    )
    fig.layout.font.family = "Gotham"
    fig.update_traces(
        hovertemplate="<br>".join(
            [
                "%{customdata[0]}",
                "%{customdata[2]}",
                "%{y}",
            ],
        )
        + "<extra></extra>",
        marker=dict(opacity=1, line=dict(width=0.5, color="Black")),
    )
    fig.show()
    # fig.write_image(f"output/ROH_events.png", scale=3)
