import dash
import darkdetect
import pandas as pd
from dash import dcc, html, no_update
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State


from main import main_entry

app = dash.Dash(__name__)

# Initial theme detection
is_dark_mode = not darkdetect.isDark()

global DASH_PAYLOAD_DEFAULTS
DASH_PAYLOAD_DEFAULTS = {
    "dont_save": True,  # don't save the plot?
    "save_both": False,  # save both light and dark mode?
    "dont_show": True,  # don't show the plot?
    "show_both": False,  # show both light and dark mode?
    "dark_mode": not is_dark_mode,  # dark mode?
    "query_events_api": False,  # query the events API?
    "print_performance_info": False,  # print performance info?
    "autosize": True,  # autosize parameter
}
toggle_button_style = {
    "position": "fixed",
    "bottom": "10px",
    "right": "10px",
    "zIndex": "1000",
    "font-family": "Gotham SSm, Futura, Roboto, Arial, Lucida Sans",
    "borderRadius": "5px",
}
# Store initial theme in a dcc.Store to allow dynamic changes
app.layout = html.Div(
    [
        dcc.Store(id="theme-store", data={"dark_mode": darkdetect.isDark()}),
        html.Button(
            "Toggle Dark/Light Mode",
            id="dark-mode-toggle",
            style={
                **toggle_button_style,
                "border": "1px solid #000",
                **(
                    {
                        "border": "1px solid #ddd",
                        "backgroundColor": "#0E1117",
                        "color": "#FFFFFF",
                    }
                    if is_dark_mode
                    else {}
                ),
            },
        ),
        dcc.Interval(
            id="interval-component-init",
            interval=0.1 * 1000,  # 0.1 seconds
            n_intervals=0,
            max_intervals=1,  # It will fire only once
        ),
        html.Div(
            id="dynamic-content"
        ),  # This div will contain dynamically generated content based on the theme
    ]
)


@app.callback(Output("dynamic-content", "children"), [Input("theme-store", "data")])
def update_dynamic_content(theme_data):
    dark_mode = theme_data["dark_mode"]
    # Generate content based on dark mode status
    # This is where you dynamically adjust styles and properties
    content = serve_layout(dark_mode)
    return content


def serve_layout(dark_mode):
    dark_mode_style = (
        {"backgroundColor": "#0E1117", "color": "#FFFFFF"} if dark_mode else {}
    )

    full_page_style = {
        "minHeight": "100vh",
        "margin": "-11px",
        "margin-top": "-25px",
        "margin-bottom": "-25px",
        "padding": "10px",
        "font-family": "Gotham SSm, Futura, Roboto, Arial, Lucida Sans",
        **dark_mode_style,
    }

    return html.Div(
        [
            html.Button(
                children="Toggle Dark/Light Mode",
                id="dark-mode-toggle",
                style={
                    **toggle_button_style,
                    "border": "1px solid #ddd" if dark_mode else "1px solid #000",
                    **dark_mode_style,
                },
            ),
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


@app.callback(Output("page-content", "children"), [Input("theme-store", "data")])
def update_layout(theme_data):
    return serve_layout(theme_data["dark_mode"])


@app.callback(
    Output("theme-store", "data"),
    [
        Input("dark-mode-toggle", "n_clicks"),
        Input("interval-component-init", "n_intervals"),
    ],
    [State("theme-store", "data")],
)
def initialize_or_toggle_dark_mode(n_clicks, n_intervals, current_state):
    ctx = dash.callback_context
    if not ctx.triggered:
        # On page load, there's no triggered input, so we don't change the state.
        # This prevents unintended toggling on page reload.
        raise PreventUpdate
    else:

        triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
        if triggered_id == "dark-mode-toggle":
            # Toggle button was clicked.
            is_dark_mode = not current_state["dark_mode"]
            global DASH_PAYLOAD_DEFAULTS
            DASH_PAYLOAD_DEFAULTS["dark_mode"] = is_dark_mode
            return {"dark_mode": is_dark_mode}
        elif triggered_id == "interval-component-init":
            # Page is loading; no need to change the state but need to ensure content is loaded.
            # So we return the current state to trigger the content update without changing the theme.
            return current_state
        else:
            return no_update


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
        **({"backgroundColor": "#0E1117", "color": "#FFFFFF"} if is_dark_mode else {}),
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
