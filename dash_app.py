import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State

app = dash.Dash(__name__)

from cloud.utils import log
from tools.parquet import Parquet
from python_roh.src.config import *
from python_roh.src.graphics import Graphics
from python_roh.src.api import get_query_dict
from python_roh.src.src import API, print_performance_info
from python_roh.upcoming_events import handle_upcoming_events

from main import main_entry

app.layout = html.Div(
    [
        dcc.Store(id="selected-event"),
        html.H1("Events and Seats Dashboard"),
        dcc.Graph(id="events-graph"),
        html.Button("Load Events", id="load-events-btn", n_clicks=0),
        dcc.Graph(id="seats-graph"),
    ]
)

DASH_PAYLOAD_DEFAULTS = {
    "dont_save": True,  # don't save the plot?
    "save_both": False,  # save both light and dark mode?
    "dont_show": True,  # don't show the plot?
    "show_both": False,  # show both light and dark mode?
    "dark_mode": False,  # dark mode?
    "query_events_api": False,  # query the events API?
    "print_performance_info": False,  # print performance info?
}


@app.callback(Output("events-graph", "figure"), Input("load-events-btn", "n_clicks"))
def load_events_calendar(n_clicks):
    payload = {
        "task_name": "events",
        **DASH_PAYLOAD_DEFAULTS,
    }
    if not n_clicks > 0:
        return {}
    events_df, today_tomorrow_events_df, next_week_events_df, fig = main_entry(
        payload, return_output=True
    )
    return fig


def get_seats_map(performance_id):
    payload = {
        "task_name": "seats",
        "performance_id": performance_id,
        **DASH_PAYLOAD_DEFAULTS,
    }
    seats_price_df, prices_df, zones_df, price_types_df, fig = main_entry(
        payload, return_output=True
    )
    return fig


@app.callback(
    Output("seats-graph", "figure"),
    Input("events-graph", "clickData"),
    State("selected-event", "data"),
)
def display_seats_map(clickData, selected_event):
    if clickData is None:
        return {}
    performance_id = clickData["points"][0]["customdata"][3]
    fig = get_seats_map(performance_id)
    return fig


if __name__ == "__main__":
    app.run_server(debug=True)
