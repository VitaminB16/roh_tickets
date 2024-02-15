import json
import requests
import google.auth
import google.auth.exceptions
from google.cloud import pubsub_v1
from google.oauth2 import id_token
from google.auth.transport.requests import Request


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
            print(f"Error refreshing credentials: {e}")
            return None
        except Exception as e:
            print(f"Error obtaining identity token: {e}")
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
            print(f"An error occurred: {e}")
