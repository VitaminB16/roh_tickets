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


@app.callback(Output("events-graph", "figure"), Input("load-events-btn", "n_clicks"))
def load_events_calendar(n_clicks):
    payload = {
        "task_name": "events",
        "dont_save": True,
        "dont_show": True,
        "show_both": False,
        "dark_mode": False,
        "query_events_api": False,
    }
    if not n_clicks > 0:
        return {}
    events_df, today_tomorrow_events_df, next_week_events_df, fig = main_entry(
        payload, return_output=True
    )
    return fig


if __name__ == "__main__":
    app.run_server(debug=True)
