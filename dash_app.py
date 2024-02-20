import dash
import darkdetect
import pandas as pd
from dash import dcc, html
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State


from main import main_entry

app = dash.Dash(__name__)

# Initial theme detection
is_dark_mode = darkdetect.isDark()

DASH_PAYLOAD_DEFAULTS = {
    "dont_save": True,  # don't save the plot?
    "save_both": False,  # save both light and dark mode?
    "dont_show": True,  # don't show the plot?
    "show_both": False,  # show both light and dark mode?
    "dark_mode": is_dark_mode,  # dark mode?
    "query_events_api": False,  # query the events API?
    "print_performance_info": False,  # print performance info?
    "autosize": True,  # autosize parameter
}


def serve_layout():
    dark_mode_style = (
        {"backgroundColor": "#0E1117", "color": "#FFFFFF"} if is_dark_mode else {}
    )

    full_page_style = {
        "minHeight": "100vh",
        "margin": "-11px",
        "margin-top": "-25px",
        "margin-bottom": "-25px",
        "padding": "10px",
        **dark_mode_style,
    }

    return html.Div(
        [
            html.H1(
                "Events and Seats Dashboard",
                style={"textAlign": "center", "margin-top": "30px", **dark_mode_style},
            ),
            dcc.Store(id="selected-event"),
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
                        style={
                            **dark_mode_style,
                            "height": 100,
                            "maxWidth": "100%",
                        },
                    ),
                ],
                style={
                    "textAlign": "center",
                    "margin-top": "-25px",
                    "marginBottom": "25px",
                    **dark_mode_style,
                    "marginLeft": "auto",
                    "marginRight": "auto",
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
                id="event-info",
                style={
                    "margin-top": "0px",
                    "marginBottom": "0px",
                    "textAlign": "center",
                    "font-family": "Gotham SSm, Futura, Roboto, Arial, Lucida Sans",
                    **dark_mode_style,
                    "marginLeft": "auto",
                    "marginRight": "auto",
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
                    **dark_mode_style,
                    "maxWidth": "1400px",
                    "marginLeft": "auto",
                    "marginRight": "auto",
                    "maxHeight": "1000px",
                },
            ),
        ],
        style=full_page_style,
    )


app.layout = serve_layout


@app.callback(
    [Output("events-graph", "figure"), Output("events-graph", "style")],
    [Input("interval-component", "n_intervals")],
)
def load_events_calendar(n_intervals):
    if n_intervals == 0:
        raise PreventUpdate
    payload = {"task_name": "events", **DASH_PAYLOAD_DEFAULTS}
    _, _, _, fig = main_entry(payload, return_output=True)
    visible_style = {"visibility": "visible", "display": "block"}
    return fig, visible_style


@app.callback(
    [
        Output("seats-graph", "figure"),
        Output("seats-graph", "style"),
        Output("event-info", "children"),
    ],
    [Input("events-graph", "clickData")],
    [State("selected-event", "data")],
)
def display_seats_map(clickData, _):
    if clickData is None:
        raise PreventUpdate
    point = clickData["points"][0]
    performance_id = point["customdata"][3]
    event_title = point["customdata"][0]
    event_time = pd.to_datetime(point["y"]).strftime("%H:%M")
    event_date = pd.to_datetime(point["x"]).strftime("%A, %B %-d, %Y")
    line_style = {"marginBottom": "0px", "marginTop": "0px"}
    box_style = {
        "border": "1px solid #ddd",
        "padding": "10px",
        "marginBottom": "10px",
        "borderRadius": "5px",
        "backgroundColor": "#f9f9f9",
    }
    event_info_box = html.Div(
        [
            html.H3(event_title, style=line_style),
            html.P(f"{event_date}", style=line_style),
            html.P(f"{event_time}", style=line_style),
        ],
        style=box_style,
    )

    fig = get_seats_map(performance_id)

    visible_style = {"visibility": "visible", "display": "block"}

    return fig, visible_style, event_info_box


# Call to get the seats map
def get_seats_map(performance_id):
    payload = {
        "task_name": "seats",
        "performance_id": performance_id,
        **DASH_PAYLOAD_DEFAULTS,
    }
    _, _, _, _, fig = main_entry(payload, return_output=True)
    return fig


if __name__ == "__main__":
    app.run_server(debug=True)
