import os
from dotenv import load_dotenv

from cloud.utils import log


def set_secrets():
    """
    Set environment variables from secrets.txt

    secrets.txt should be in the format:
    KEY1 = VALUE1
    KEY2 = VALUE2
    """
    try:
        with open("secrets.txt") as f:
            for line in f:
                key, value = line.strip().split("=")
                os.environ[key] = value
    except FileNotFoundError:
        log("No secrets.txt file found. Continuing without these secrets.")


def set_gcp_secrets(service="python-roh"):
    from google.cloud import secretmanager
    import google.auth

    credentials, project = google.auth.default()
    secret_client = secretmanager.SecretManagerServiceClient()
    secrets = secret_client.list_secrets(
        request={"parent": f"projects/{project}", "filter": f"labels.{service}:*"}
    )
    for secret in secrets:
        name = secret.name
        try:
            value = secret_client.access_secret_version(
                request={"name": f"{name}/versions/latest"}
            )
            os.environ[name.split("/")[-1]] = value.payload.data.decode("utf-8")
            print(f"Set secret {name.split('/')[-1]}")
        except Exception as e:
            print(f"Failed to grab secret {name}. Exception: {e}")
            log(f"Failed to grab secret {name}. Exception: {e}")


set_gcp_secrets()
load_dotenv()
set_secrets()
