import dash
import pandas as pd
from dash import dcc, html
from dash_svg import Svg, G, Path
from python_roh.src.config import *
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ClientsideFunction

from main import main_entry
from python_roh.dash.assets.vars import GITHUB_LOGO_SVG_PATH
from python_roh.casts import (
    try_get_cast_for_current_performance,
    get_previously_seen_casts,
)
from tools import Firestore

app = dash.Dash(__name__)

DARK_MODE_STYLE = {"backgroundColor": "#0E1117", "color": "#FFFFFF"}
LIGHT_MODE_STYLE = {"backgroundColor": "#FFFFFF", "color": "#000000"}
TOGGLE_BUTTON_STYLE = {
    "position": "fixed",
    "bottom": "10px",
    "right": "10px",
    "zIndex": "1000",
    "font-family": "Gotham SSm, Futura, Roboto, Arial, Lucida Sans",
    "borderRadius": "5px",
}
AUTO_MARGIN = {"marginLeft": "auto", "marginRight": "auto"}
FONT_FAMILY = {"font-family": "Gotham SSm, Futura, Roboto, Arial, Lucida Sans"}

DASH_PAYLOAD_DEFAULTS = {
    "dont_save": True,  # don't save the plot?
    "save_both": False,  # save both light and dark mode?
    "dont_show": True,  # don't show the plot?
    "show_both": False,  # show both light and dark mode?
    "dark_mode": False,  # dark mode?
    "query_events_api": False,  # query the events API?
    "print_performance_info": False,  # print performance info?
    "autosize": True,  # autosize parameter
    "font_family": "Gotham SSm",  # font family
    "available_seat_status_ids": [0, 4],  # available seat status IDs
}
EVENTS_DF = pd.DataFrame()
SEEN_CASTS_DF = pd.DataFrame()
SEEN_EVENTS_DF = pd.DataFrame()


# Helper Functions
def create_dynamic_style(is_dark_mode):
    return DARK_MODE_STYLE if is_dark_mode else LIGHT_MODE_STYLE


def update_toggle_button_style(is_dark_mode):
    base_style = TOGGLE_BUTTON_STYLE.copy()
    mode_style = (
        {"border": "1px solid #ddd", **DARK_MODE_STYLE}
        if is_dark_mode
        else {"border": "1px solid #000"}
    )
    return {**base_style, **mode_style}


app.layout = html.Div(
    [
        dcc.Store(id="theme-store"),
        dcc.Store(id="screen-width-store"),
        dcc.Store(id="refresh-toggle-store", data={"refresh_enabled": False}),
        dcc.Store(id="seat-status-store", data=[0]),  # Store for seat status IDs
        html.Div(id="dynamic-content"),
        dcc.Interval(
            id="interval-component-init", interval=100, n_intervals=0, max_intervals=1
        ),
        dcc.Interval(
            id="interval-component-refresh",
            interval=10 * 60 * 1000,
            n_intervals=0,  # every 10 minutes
        ),
    ]
)

# JavaScript clientside callbacks (.js files within assets folder)
app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="detectTheme"),
    Output("theme-store", "data"),
    [Input("interval-component-init", "n_intervals")],
)
app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="detectScreenWidth"),
    Output("screen-width-store", "data"),
    [Input("interval-component-init", "n_intervals")],
)


@app.callback(Output("dynamic-content", "children"), [Input("theme-store", "data")])
def update_dynamic_content(theme_data):
    dark_mode = theme_data["dark_mode"] if theme_data else True
    DASH_PAYLOAD_DEFAULTS["dark_mode"] = dark_mode
    return serve_layout(dark_mode)


@app.callback(
    Output("theme-store", "data", allow_duplicate=True),
    [Input("dark-mode-toggle", "n_clicks")],
    [State("theme-store", "data")],
    prevent_initial_call=True,
)
def toggle_dark_mode(n_clicks, current_state):
    if n_clicks is None:
        raise PreventUpdate
    is_dark_mode = not current_state["dark_mode"]
    DASH_PAYLOAD_DEFAULTS["dark_mode"] = is_dark_mode
    return {"dark_mode": is_dark_mode}


@app.callback(
    [
        Output("refresh-toggle-store", "data"),
        Output("interval-component-refresh", "interval"),
    ],
    [Input("refresh-toggle-button", "n_clicks")],
    [State("refresh-toggle-store", "data")],
    prevent_initial_call=True,
)
def toggle_refresh(n_clicks, current_state):
    if n_clicks is None:
        raise PreventUpdate
    refresh_enabled = not current_state["refresh_enabled"]
    # Set interval to 10 seconds when enabled, 10 minutes when disabled
    new_interval = 10 * 1000 if refresh_enabled else 10 * 60 * 1000
    return {"refresh_enabled": refresh_enabled}, new_interval


def serve_layout(dark_mode):
    style = create_dynamic_style(dark_mode)

    full_page_style = {
        "minHeight": "100vh",
        "margin": "-11px",
        "margin-top": "-25px",
        "margin-bottom": "-25px",
        "padding": "10px",
        **FONT_FAMILY,
        **style,
    }

    return html.Div(
        [
            html.Button(
                "Toggle Dark/Light Mode",
                id="dark-mode-toggle",
                style=update_toggle_button_style(dark_mode),
            ),
            html.Button(
                "Toggle Auto-Refresh",
                id="refresh-toggle-button",
                style={**update_toggle_button_style(dark_mode), "bottom": "30px"},
            ),
            html.Div(
                [
                    html.Div(  # GitHub logo
                        html.A(
                            Svg(
                                G(Path(d=GITHUB_LOGO_SVG_PATH)),
                                width=32,
                                height=32,
                                viewBox="0 0 16 16",
                                fill="#000000" if not dark_mode else "#FFFFFF",
                            ),
                            href=PYTHON_ROH_REPO_URL,
                            target="_blank",
                            style={
                                "display": "inline-block",
                                "vertical-align": "middle",
                            },
                        ),
                        style={"display": "inline-block"},
                    ),
                    html.Div(  # GitHub repo link
                        html.A(
                            "Source code",
                            href=PYTHON_ROH_REPO_URL,
                            target="_blank",
                            style={
                                "display": "inline-block",
                                "vertical-align": "middle",
                                "marginLeft": "5px",
                            },
                        ),
                        style={"display": "inline-block"},
                    ),
                ],
                style={
                    "position": "absolute",
                    "top": "20px",
                    "right": "10px",
                    "zIndex": "1000",
                    **FONT_FAMILY,
                },
            ),
            html.H1(
                "Events and Seats Dashboard",
                style={"textAlign": "center", "margin-top": "30px", **style},
            ),
            dcc.Store(id="current-performance-id", storage_type="memory"),
            dcc.Interval(
                id="interval-component",
                interval=0.1 * 1000,  # 0.1 seconds
                n_intervals=0,
                max_intervals=1,  # It will fire only once
            ),
            html.Div(
                [
                    dcc.Loading(
                        id="loading-events",
                        type="default",
                        children=[
                            dcc.Graph(
                                id="events-graph",
                                style={"height": 400, "visibility": "hidden"},
                            )
                        ],
                        style={**style, "height": 100, "maxWidth": "100%"},
                    ),
                ],
                style={
                    "textAlign": "center",
                    "margin-top": "-25px",
                    "marginBottom": "25px",
                    **style,
                    **AUTO_MARGIN,
                },
            ),
            html.Hr(
                style={
                    "borderTop": "1px solid #dddddd",
                    "width": "100%",
                    "marginBottom": "25px",
                    "marginTop": "25px",
                }
            ),
            html.Div(
                id="event-info-container",
                style={
                    "margin-top": "0px",
                    "marginBottom": "0px",
                    "textAlign": "center",
                    **FONT_FAMILY,
                    **style,
                    **AUTO_MARGIN,
                },
            ),
            # -----------------------------
            # Seats graph, then cast sheet, then seat-view image
            # -----------------------------
            html.Div(
                [
                    dcc.Loading(
                        id="loading-seats",
                        type="default",
                        children=[
                            dcc.Graph(
                                id="seats-graph",
                                style={"height": "1000px", "visibility": "hidden"},
                            )
                        ],
                    ),
                    # Cast sheet in the middle
                    html.Div(
                        id="casts-container",
                        style={
                            "margin-top": "20px",
                            "marginBottom": "20px",
                            "textAlign": "center",
                            **FONT_FAMILY,
                            **style,
                            **AUTO_MARGIN,
                        },
                    ),
                    # Seat image last
                    html.Div(
                        id="seat-view-image-container",
                        style={"marginTop": "20px"},
                    ),
                ],
                style={
                    "textAlign": "center",
                    **style,
                    **AUTO_MARGIN,
                    "maxWidth": "1400px",
                },
            ),
        ],
        style=full_page_style,
    )


@app.callback(Output("page-content", "children"), [Input("theme-store", "data")])
def update_layout(theme_data):
    return serve_layout(theme_data["dark_mode"])


@app.callback(
    Output("seat-status-store", "data"),
    [Input("seat-status-input", "value")],
)
def update_seat_status_ids(input_value):
    status_ids = []
    negative = False
    for s in input_value.split(","):
        s = s.strip()
        if s:
            try:
                s = int(s)
            except ValueError:
                continue  # Skip invalid entries
            if s < 0:
                negative = True
                s = -s
            status_ids.append(s)
    if not status_ids:
        # Handle case where no valid entries were found
        status_ids = [0, 4]  # Default value
    if negative:
        full_array = list(range(0, 20))
        status_ids = [x for x in full_array if x not in status_ids]
    return status_ids


@app.callback(
    [Output("events-graph", "figure"), Output("events-graph", "style")],
    [Input("interval-component", "n_intervals")],
    [State("theme-store", "data"), State("screen-width-store", "data")],
)
def load_events_calendar(n_intervals, theme_data, screen_width):
    if n_intervals == 0:
        raise PreventUpdate
    print(f"Screen width: {screen_width}")
    DASH_PAYLOAD_DEFAULTS["dark_mode"] = theme_data["dark_mode"]
    payload = {"task_name": "events", **DASH_PAYLOAD_DEFAULTS}
    events_df, _, _, fig = main_entry(payload, return_output=True)
    visible_style = {"visibility": "visible", "display": "block"}
    global SEEN_EVENTS_DF
    SEEN_EVENTS_DF = Firestore(SEEN_EVENTS_PARQUET_LOCATION).read(apply_schema=True)
    global SEEN_CASTS_DF
    SEEN_CASTS_DF = Firestore(SEEN_CASTS_PARQUET_LOCATION).read(apply_schema=True)
    global EVENTS_DF
    EVENTS_DF = events_df
    return fig, visible_style


def compute_seen_suffix(name, seen_casts_df):
    """
    Return a string containing all of the performances where 'name' has been seen before,
    in a single comma-separated list. This string will be used as a hover tooltip.
    """
    seen_cast = seen_casts_df.query(f"name == @name")
    title_timestamps = seen_cast.apply(
        lambda x: f"{x.title} ({x.timestamp.strftime('%b %Y')})", axis=1
    )
    title_timestamps = title_timestamps.unique()
    output = "\n".join(title_timestamps)
    return output


@app.callback(
    [
        Output("seats-graph", "figure"),
        Output("seats-graph", "style"),
        Output("event-info-container", "children"),
        Output("current-performance-id", "data"),
        Output("casts-container", "children"),
    ],
    [Input("events-graph", "clickData")],
    [State("theme-store", "data"), State("seat-status-store", "data")],
    prevent_initial_call=False,
)
def display_seats_map(
    clickData=None,
    theme_data=None,
    seat_status_ids=None,
    point=None,
    performance_id=None,
):
    """
    Returns:
    seats-graph.figure,
    seats-graph.style,
    event-info-container.children,
    current-performance-id.data,
    casts-container.children
    """
    if clickData is None and point is None and performance_id is None:
        raise PreventUpdate
    if point is None and clickData is not None:
        point = clickData["points"][0]
    if performance_id is None:
        performance_id = point["customdata"][3]

    event = EVENTS_DF.query(f"performanceId == '{performance_id}'").iloc[0]
    event_title = event["title"]
    event_time = event["timestamp"].strftime("%H:%M")
    event_date = event["timestamp"].strftime("%A, %B %-d, %Y")

    line_style = {"marginBottom": "0px", "marginTop": "0px"}
    box_style = {
        "display": "flex",
        "justifyContent": "space-between",
        "alignItems": "center",
        "border": "1px solid #ddd",
        "padding": "10px",
        "marginBottom": "10px",
        "borderRadius": "5px",
        **(
            DARK_MODE_STYLE
            if theme_data["dark_mode"]
            else {"backgroundColor": "#f9f9f9"}
        ),
    }
    url_style = {
        "display": "flex",
        "flexDirection": "column",
        "alignItems": "right",
    }

    all_events, next_event, previous_event = get_all_title_events(
        performance_id=performance_id, event_title=event_title
    )
    seat_status_input = html.Div(
        [
            html.Label("Seat Status Filter:"),
            html.Br(),
            dcc.Input(
                id="seat-status-input",
                type="text",
                value=(
                    ", ".join(map(str, seat_status_ids)) if seat_status_ids else "0"
                ),
                style={"width": "80px", "marginLeft": "auto", "marginRight": "auto"},
            ),
        ],
        style={
            "textAlign": "left",
            "marginLeft": "20px",
            "position": "absolute",
        },
    )

    event_info_box = html.Div(
        [
            html.H3(event_title, style=line_style),
            html.P(f"{event_date}", style=line_style),
            html.P(f"{event_time}", style=line_style),
        ],
        style={"flex": "1"},
    )
    event_urls = html.Div(
        [
            dcc.Link(
                "Book tickets",
                href=f"{INTERSTITIAL_URL}/{performance_id}",
                target="_blank",
                style=url_style,
            ),
            dcc.Link(
                "View seat map",
                href=f"{SEATMAP_URL}?performanceId={performance_id}",
                target="_blank",
                style=url_style,
            ),
        ],
        style={"position": "absolute", "marginLeft": "75%", "textAlign": "right"},
    )
    next_previous_buttons = html.Div(
        [
            html.Button(
                id="prev-performance-btn",
                children="← Previous",
                n_clicks=0,
                className="link-button",
                style={"marginRight": "10px", **url_style, "font-size": "16px"},
                **({"data-performance-id": previous_event} if previous_event else {}),
            ),
            html.Button(
                id="refresh-performance-btn",
                children="Refresh",
                n_clicks=0,
                className="link-button",
                style={"marginRight": "10px", **url_style, "font-size": "16px"},
                **({"data-performance-id": performance_id} if performance_id else {}),
            ),
            html.Button(
                id="next-performance-btn",
                children="Next →",
                n_clicks=0,
                className="link-button",
                style={**url_style, "font-size": "16px"},
                **({"data-performance-id": next_event} if next_event else {}),
            ),
        ],
        style={
            "display": "flex",
            "justifyContent": "center",
            "marginBottom": "auto",
            "marginTop": "auto",
            "margin-left": "18%",
            "position": "absolute",
            "textAlign": "right",
            **FONT_FAMILY,
        },
    )
    event_info_container = html.Div(
        [event_info_box, event_urls, next_previous_buttons, seat_status_input],
        style=box_style,
    )

    # Get seat map
    fig = get_seats_map(performance_id, available_seat_status_ids=seat_status_ids)

    # Get the casts data
    try:
        casts = try_get_cast_for_current_performance(performance_id)
        seen_casts_df = SEEN_CASTS_DF
        seen_casts_df = get_previously_seen_casts(casts, seen_casts_df)
        casts = casts.loc[:, ["role", "name"]]
        casts = casts.to_dict("records")

        line_style = {"marginBottom": "0px", "marginTop": "0px"}
        casts_list_html = []
        line_style = {"marginBottom": "0px", "marginTop": "0px"}

        for cast_row in casts:
            person_name = cast_row["name"]
            role = cast_row["role"]

            if person_name in seen_casts_df.name.values:
                tooltip_text = compute_seen_suffix(person_name, seen_casts_df)

                # Create a parent span for "(seen)" plus a hidden child with tooltip text
                person_name_element = html.Span(
                    [
                        person_name,  # e.g. "James Cleverton"
                        " ",          # Add a space before "(seen)"
                        html.Span(
                            [
                                # The text the user sees
                                "(seen)",
                                # A hidden span that will show on hover
                                html.Span(
                                    tooltip_text,
                                    className="tooltip-text",
                                )
                            ],
                            className="seen-tooltip",
                        ),
                    ]
                )
            else:
                person_name_element = html.Span(person_name)
            casts_one_html = html.Tr(
                [
                    html.Td(role, style={"textAlign": "right", "width": "40%"}),
                    html.Td("|", style={"textAlign": "center", "width": "2%"}),
                    html.Td(person_name_element, style={"textAlign": "left", "width": "40%"}),
                ],
                style=line_style,
            )
            casts_list_html.append(casts_one_html)

        casts_html = html.Div(
            [
                html.H3("Cast sheet", style=line_style),
                html.Table(
                    casts_list_html,
                    style={"margin": "0 auto"},  # Center horizontally
                ),
            ],
            style=line_style,
        )

    except KeyboardInterrupt:
        raise
    except Exception as e:
        casts_html = html.Div(
            [
                html.H3("Cast sheet", style=line_style),
                html.P("Cast sheet not available", style=line_style),
            ],
            style=line_style,
        )

    visible_style = {"visibility": "visible", "display": "block"}

    return fig, visible_style, event_info_container, performance_id, casts_html


@app.callback(
    Output("seat-view-image-container", "children"),
    [Input("seats-graph", "clickData")],
)
def display_seat_view_image(clickData):
    if clickData is None:
        raise PreventUpdate
    point = clickData["points"][0]
    image_url = point["customdata"][5]
    image = html.Img(src=image_url, style={"maxWidth": "70%", "maxHeight": "70%"})
    price, zone, row, seat = point["customdata"][0:4]
    seat_info_str = f"View from seat: {row}{seat} ({zone})"
    seat_price_str = (
        f"Price | {price}" if price != "Not available" else "Seat not available"
    )
    image_div = html.Div(image, style={"display": "block"})
    seat_info = html.Div(seat_info_str, style={"textAlign": "center"})
    seat_price = html.Div(seat_price_str, style={"textAlign": "center"})
    output = html.Div([image_div, seat_info, seat_price], style={"textAlign": "center"})
    return output


@app.callback(
    [
        Output("current-performance-id", "data", allow_duplicate=True),
        Output("seats-graph", "figure", allow_duplicate=True),
        Output("seats-graph", "style", allow_duplicate=True),
        Output("event-info-container", "children", allow_duplicate=True),
        Output("casts-container", "children", allow_duplicate=True),
    ],
    [
        Input("next-performance-btn", "n_clicks"),
        Input("prev-performance-btn", "n_clicks"),
    ],
    [
        State("current-performance-id", "data"),
        State("theme-store", "data"),
        State("seat-status-store", "data"),
    ],
    prevent_initial_call=True,
)
def update_performance_id(
    next_clicks,
    prev_clicks,
    current_id,
    theme_data,
    seat_status_ids,
):
    ctx = dash.callback_context
    if not ctx.triggered:
        raise PreventUpdate
    if next_clicks == 0 and prev_clicks == 0:
        raise PreventUpdate
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    _, next_perf, previous_perf = get_all_title_events(performance_id=current_id)
    if button_id == "next-performance-btn":
        updated_id = next_perf
    elif button_id == "prev-performance-btn":
        updated_id = previous_perf
    else:
        raise PreventUpdate
    if updated_id is None:
        raise PreventUpdate
    print(f"Updated performance ID: {updated_id}")
    fig, visible_style, event_info_container, _, casts = display_seats_map(
        performance_id=updated_id,
        theme_data=theme_data,
        seat_status_ids=seat_status_ids,
    )
    return updated_id, fig, visible_style, event_info_container, casts


@app.callback(
    Output("seats-graph", "figure", allow_duplicate=True),
    [Input("interval-component-refresh", "n_intervals")],
    [
        State("refresh-toggle-store", "data"),
        State("current-performance-id", "data"),
        State("theme-store", "data"),
        State("seat-status-store", "data"),
    ],
    prevent_initial_call=True,
)
def refresh_seats_map_auto(
    refresh_intervals, refresh_toggle, performance_id, theme_data, seat_status_ids
):
    refresh_enabled = refresh_toggle["refresh_enabled"]
    if refresh_intervals == 0 or not refresh_enabled:
        return dash.no_update
    fig = get_seats_map(performance_id, available_seat_status_ids=seat_status_ids)
    return fig


@app.callback(
    Output("seats-graph", "figure", allow_duplicate=True),
    [Input("refresh-performance-btn", "n_clicks")],
    [
        State("current-performance-id", "data"),
        State("theme-store", "data"),
        State("seat-status-store", "data"),
    ],
    prevent_initial_call=True,
)
def refresh_seats_map_manual(
    refresh_clicks, performance_id, theme_data, seat_status_ids
):
    if refresh_clicks == 0:
        return dash.no_update
    fig = get_seats_map(performance_id, available_seat_status_ids=seat_status_ids)
    return fig


def get_event_title(performance_id):
    event_title = EVENTS_DF.query(f"performanceId == @performance_id").title
    event_title = event_title.iloc[0]
    return event_title


# Call to get the seats map
def get_seats_map(performance_id, available_seat_status_ids=None):
    event_title = get_event_title(performance_id)
    mos_override = {}
    if event_title == "Friends Rehearsals":
        mos_override["mode_of_sale_id"] = 8
    payload = {
        "task_name": "seats",
        "performance_id": performance_id,
        **DASH_PAYLOAD_DEFAULTS,
        **mos_override,
    }
    if available_seat_status_ids is not None:
        payload["available_seat_status_ids"] = available_seat_status_ids
    _, _, _, _, fig = main_entry(payload, return_output=True)
    return fig


def get_all_title_events(performance_id, event_title=None):
    if event_title is None:
        event_title = get_event_title(performance_id)
    today = pd.Timestamp.today(tz="Europe/London") - pd.Timedelta(hours=3.5)
    title_events = EVENTS_DF.query(f"title == @event_title and timestamp >= @today")
    title_events = title_events.sort_values(by=["date", "time"])
    all_performances = list(title_events.performanceId)
    print("All performances:", all_performances)
    print("Current performance:", performance_id)
    index = all_performances.index(performance_id)
    next_perf = (
        all_performances[index + 1] if index + 1 < len(all_performances) else None
    )
    previous_perf = all_performances[index - 1] if index - 1 >= 0 else None
    title_events = title_events.loc[:, ["timestamp", "time", "performanceId"]]
    title_events["date_str"] = title_events["timestamp"].dt.strftime(
        "%a %B %-d, %-I:%M%p"
    )
    return title_events, next_perf, previous_perf


if __name__ == "__main__":
    app.run_server(debug=True)
