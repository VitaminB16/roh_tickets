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
FONT_FAMILY = "Gotham SSm, Futura, Roboto, Arial, Lucida Sans"


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

app.layout = html.Div(
    [
        dcc.Store(id="theme-store", data={"dark_mode": darkdetect.isDark()}),
        html.Button(
            "Toggle Dark/Light Mode",
            id="dark-mode-toggle",
            style=update_toggle_button_style(darkdetect.isDark()),
        ),
        dcc.Interval(
            id="interval-component-init", interval=100, n_intervals=0, max_intervals=1
        ),
        html.Div(id="dynamic-content"),
    ]
)


# Callbacks
@app.callback(Output("dynamic-content", "children"), [Input("theme-store", "data")])
def update_dynamic_content(theme_data):
    return serve_layout(theme_data["dark_mode"])


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
            html.H1(
                "Events and Seats Dashboard",
                style={"textAlign": "center", "margin-top": "30px", **style},
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
                            **style,
                            "height": 100,
                            "maxWidth": "100%",
                        },
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
        Output("event-info-container", "children"),
    ],
    [Input("events-graph", "clickData")],
    [State("selected-event", "data"), State("theme-store", "data")],
)
def display_seats_map(clickData, _, theme_data):
    if clickData is None:
        raise PreventUpdate
    point = clickData["points"][0]
    performance_id = point["customdata"][3]
    event_title = point["customdata"][0]
    event_time = pd.to_datetime(point["y"]).strftime("%H:%M")
    event_date = pd.to_datetime(point["x"]).strftime("%A, %B %-d, %Y")
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
            {"backgroundColor": "#0E1117", "color": "#FFFFFF"}
            if theme_data["dark_mode"]
            else {"backgroundColor": "#f9f9f9"}
        ),
    }
    url_style = {
        "display": "flex",
        "flexDirection": "column",
        "alignItems": "right",
        "color": "#FA8072" if theme_data["dark_mode"] else "#0000EE",
        "textDecoration": "underline",
    }

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

    event_info_container = html.Div([event_info_box, event_urls], style=box_style)

    fig = get_seats_map(performance_id)
    visible_style = {"visibility": "visible", "display": "block"}

    return fig, visible_style, event_info_container


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
