import dash
import pandas as pd
from dash import dcc, html, no_update
from dash.exceptions import PreventUpdate
from dash_svg import Svg, G, Path, Circle
from dash.dependencies import Input, Output, State, ClientsideFunction


from main import main_entry

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
GITHUB_LOGO_SVG_PATH = "M8 0c4.42 0 8 3.58 8 8a8.013 8.013 0 0 1-5.45 7.59c-.4.08-.55-.17-.55-.38 0-.27.01-1.13.01-2.2 0-.75-.25-1.23-.54-1.48 1.78-.2 3.65-.88 3.65-3.95 0-.88-.31-1.59-.82-2.15.08-.2.36-1.02-.08-2.12 0 0-.67-.22-2.2.82-.64-.18-1.32-.27-2-.27-.68 0-1.36.09-2 .27-1.53-1.03-2.2-.82-2.2-.82-.44 1.1-.16 1.92-.08 2.12-.51.56-.82 1.28-.82 2.15 0 3.06 1.86 3.75 3.64 3.95-.23.2-.44.55-.51 1.07-.46.21-1.61.55-2.33-.66-.15-.24-.6-.83-1.23-.82-.67.01-.27.38.01.53.34.19.73.9.82 1.13.16.45.68 1.31 2.69.94 0 .67.01 1.3.01 1.49 0 .21-.15.45-.55.38A7.995 7.995 0 0 1 0 8c0-4.42 3.58-8 8-8Z"

global DASH_PAYLOAD_DEFAULTS
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
}
global EVENTS_DF
EVENTS_DF = pd.DataFrame()


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
        html.Div(id="dynamic-content"),
        dcc.Interval(
            id="interval-component-init", interval=100, n_intervals=0, max_intervals=1
        ),
    ]
)


# Callbacks
app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="detectTheme"),
    Output("theme-store", "data"),
    [Input("interval-component-init", "n_intervals")],
)


@app.callback(Output("dynamic-content", "children"), [Input("theme-store", "data")])
def update_dynamic_content(theme_data):
    if theme_data is None:
        dark_mode = True
    else:
        dark_mode = theme_data["dark_mode"]
    global DASH_PAYLOAD_DEFAULTS
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
    global DASH_PAYLOAD_DEFAULTS
    DASH_PAYLOAD_DEFAULTS["dark_mode"] = is_dark_mode

    return {"dark_mode": is_dark_mode}


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
                            href="https://github.com/VitaminB16/roh_tickets",
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
                            href="https://github.com/VitaminB16/roh_tickets",
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
                [  # Upcoming events
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
                ],
                style={
                    "textAlign": "center",
                    **style,
                    **AUTO_MARGIN,
                    "maxWidth": "1400px",
                    "maxHeight": "1000px",
                },
            ),
        ],
        style=full_page_style,
    )


@app.callback(Output("page-content", "children"), [Input("theme-store", "data")])
def update_layout(theme_data):
    return serve_layout(theme_data["dark_mode"])


@app.callback(
    [Output("events-graph", "figure"), Output("events-graph", "style")],
    [Input("interval-component", "n_intervals")],
    [State("theme-store", "data")],
)
def load_events_calendar(n_intervals, theme_data):
    if n_intervals == 0:
        raise PreventUpdate
    global DASH_PAYLOAD_DEFAULTS
    global EVENTS_DF
    DASH_PAYLOAD_DEFAULTS["dark_mode"] = theme_data["dark_mode"]
    payload = {"task_name": "events", **DASH_PAYLOAD_DEFAULTS}
    events_df, _, _, fig = main_entry(payload, return_output=True)
    visible_style = {"visibility": "visible", "display": "block"}
    EVENTS_DF = events_df
    return fig, visible_style


@app.callback(
    [
        Output("seats-graph", "figure"),
        Output("seats-graph", "style"),
        Output("event-info-container", "children"),
        Output("current-performance-id", "data"),
    ],
    [Input("events-graph", "clickData")],
    [State("theme-store", "data")],
)
def display_seats_map(clickData=None, theme_data=None, point=None, performance_id=None):
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

    fig = get_seats_map(performance_id)
    visible_style = {"visibility": "visible", "display": "block"}

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
                href=f"https://www.roh.org.uk/checkout/interstitial/{performance_id}",
                target="_blank",
                style=url_style,
            ),
            dcc.Link(
                "View seat map",
                href=f"https://www.roh.org.uk/seatmap?performanceId={performance_id}",
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
            "margin-left": "25%",
            "position": "absolute",
            **FONT_FAMILY
        },
    )
    event_info_container = html.Div(
        [event_info_box, event_urls, next_previous_buttons], style=box_style
    )

    return fig, visible_style, event_info_container, performance_id


@app.callback(
    [
        Output("current-performance-id", "data", allow_duplicate=True),
        Output("seats-graph", "figure", allow_duplicate=True),
        Output("seats-graph", "style", allow_duplicate=True),
        Output("event-info-container", "children", allow_duplicate=True),
    ],
    [
        Input("next-performance-btn", "n_clicks"),
        Input("prev-performance-btn", "n_clicks"),
    ],
    [State("current-performance-id", "data"), State("theme-store", "data")],
    prevent_initial_call=True,
)
def update_performance_id(next_clicks, prev_clicks, current_id, theme_data):
    ctx = dash.callback_context
    if not ctx.triggered:
        raise PreventUpdate
    if next_clicks == 0 and prev_clicks == 0:
        raise PreventUpdate
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    _, next_perf, prev_perf = get_all_title_events(performance_id=current_id)
    if button_id == "next-performance-btn":
        updated_id = next_perf
    elif button_id == "prev-performance-btn":
        updated_id = prev_perf
    else:
        raise PreventUpdate
    if updated_id is None:
        raise PreventUpdate
    print(f"Updated performance ID: {updated_id}")
    fig, visible_style, event_info_container, _ = display_seats_map(
        performance_id=updated_id, theme_data=theme_data
    )
    return updated_id, fig, visible_style, event_info_container


# Call to get the seats map
def get_seats_map(performance_id):
    payload = {
        "task_name": "seats",
        "performance_id": performance_id,
        **DASH_PAYLOAD_DEFAULTS,
    }
    _, _, _, _, fig = main_entry(payload, return_output=True)
    return fig


def get_all_title_events(performance_id, event_title=None):
    if event_title is None:
        event_title = EVENTS_DF.query(f"performanceId == @performance_id").title
        event_title = event_title.iloc[0]
    today = pd.Timestamp.today(tz="Europe/London")
    title_events = EVENTS_DF.query(f"title == @event_title and timestamp >= @today")
    title_events = title_events.sort_values(by=["date", "time"])
    all_performances = list(title_events.performanceId)
    print("All performances:", all_performances)
    print("Current performance:", performance_id)
    next_perf = all_performances.index(performance_id) + 1
    previous_perf = all_performances.index(performance_id) - 1
    next_perf = (
        all_performances[next_perf] if next_perf < len(all_performances) else None
    )
    previous_perf = all_performances[previous_perf] if previous_perf >= 0 else None
    title_events = title_events.loc[:, ["timestamp", "time", "performanceId"]]
    title_events["date_str"] = title_events["timestamp"].dt.strftime(
        "%a %B %-d, %-I:%M%p"
    )
    return title_events, next_perf, previous_perf


if __name__ == "__main__":
    app.run_server(debug=True)
