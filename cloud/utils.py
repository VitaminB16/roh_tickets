import os
import json
import logging
import requests
import google.auth
import google.auth.exceptions
from google.oauth2 import id_token
from google.auth.transport.requests import Request
from google.cloud import pubsub_v1, logging as gcp_logging

if os.getenv("PLATFORM", "GCP") in ["GCP", "local"]:
    client = gcp_logging.Client()
    client.get_default_handler()
    client.setup_logging()


class GCPRequest:
    """
    Class to make authorized requests to Google Cloud Platform services
    """

    def __init__(self, url):
        self.url = url

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        self.credentials, self.project = google.auth.default(scopes=scopes)

        self.identity_token = self.get_identity_token()
        self.headers = {
            "Authorization": f"Bearer {self.identity_token}",
            "Content-type": "application/json",
        }

    def get_identity_token(self):
        # Attempt to fetch an identity token for the given URL
        try:
            # Ensure the credentials are valid and refreshed
            if not self.credentials.valid:
                self.credentials.refresh(Request())

            # The audience URL should be the URL of the cloud function or service you are accessing.
            # Make sure this matches exactly what's expected by the service.
            token = id_token.fetch_id_token(Request(), self.url)
            return token
        except google.auth.exceptions.RefreshError as e:
            log(f"Error refreshing credentials: {e}")
            return None
        except Exception as e:
            log(f"Error obtaining identity token: {e}")
            return None

    def post(self, payload, **kwargs):
        response = requests.post(self.url, json=payload, headers=self.headers, **kwargs)
        return response

    def get(self, **kwargs):
        response = requests.get(self.url, headers=self.headers, **kwargs)
        return response

    def put(self, payload, **kwargs):
        response = requests.put(self.url, json=payload, headers=self.headers, **kwargs)
        return response


class PubSub:
    """
    Class for pushing a payload to a topic
    """

    def __init__(self, topic: str):
        self.topic_id = topic
        self.publisher = pubsub_v1.PublisherClient()
        self.project = google.auth.default()[1]
        self.topic_path = self.publisher.topic_path(self.project, topic)

    def publish(self, data) -> None:
        if isinstance(data, dict):
            data = json.dumps(data)
        try:
            publish_future = self.publisher.publish(
                self.topic_path, data.encode("utf-8")
            )
            result = publish_future.result()
        except Exception as e:
            log(f"An error occurred: {e}")


class SQLBuilder:
    """
    Class for building SQL queries.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.query_parts = ["FROM", f"`{table_name}`"]

    def build(self) -> str:
        """
        Return the final query string.
        """
        return " ".join(self.query_parts)

    def select(self, columns=None) -> "SQLBuilder":
        """
        Build a SELECT query.
        """
        if columns is None:
            columns = "*"
        if isinstance(columns, str):
            columns = [columns]
        columns = [f"`{x}`" for x in columns if x != "*"] or ["*"]
        select_clause = "SELECT " + ", ".join(columns)
        self.query_parts.insert(0, select_clause)  # Insert at the beginning
        return self

    def where(self, filters: dict) -> "SQLBuilder":
        """
        Build a WHERE clause.
        """
        where_clause = self._where(filters)
        if where_clause:
            self.query_parts.append("WHERE " + where_clause)
        return self

    def _where(self, filters: dict) -> str:
        """
        Helper to build a WHERE clause.
        """
        if filters is None:
            return ""
        conditions = []
        if isinstance(filters, dict):
            items = filters.items()
        else:
            items = filters
        for col, op, value in items:
            if isinstance(value, list):
                value = ", ".join([f"'{x}'" for x in value])
                conditions.append(f"{col} {op} ({value})")
            else:
                conditions.append(f"{col} {op} '{value}'")
        return " AND ".join(conditions)


def log(*args, **kwargs):
    """
    Function for logging to Google Cloud Logs. Logs a message as usual, and logs a dictionary of data as jsonPayload.

    Arguments:
        *args (list): list of elements to "print" to google cloud logs.
    """
    # Use these environment variables as payload to log to Google Cloud Logs
    env_keys = ["SERVE_AS"]
    env_data = {key: os.getenv(key, None) for key in env_keys}
    log_data = {k: v for k, v in env_data.items() if v is not None}

    # If any arguments are a dictionary, add it to the log_data so it can be queried in Google Cloud Logs
    for arg in args:
        if isinstance(arg, dict):
            log_data.update(arg)
        log_data["message"] = " ".join([str(a) for a in args])

    if os.getenv("SERVE_AS", "cloud_function") in ["cloud_run"]:
        logging.info(log_data)
    else:
        # If running locally, use a normal print
        print(log_data["message"], **kwargs)
