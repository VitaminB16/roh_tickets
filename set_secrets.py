import os
from dotenv import load_dotenv


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
                key, value = line.strip().split(" = ")
                os.environ[key] = value
    except FileNotFoundError:
        print("No secrets.txt file found. Continuing without these secrets.")


def set_gcp_secrets(service="python-roh"):
    from google.cloud import secretmanager

    project = os.environ["PROJECT"]
    secret_client = secretmanager.SecretManagerServiceClient()
    secrets = secret_client.list_secrets(
        request={"parent": f"projects/{project}", "filter": f"labels.{service}:*"}
    )
    for secret in secrets:
        try:
            value = secret_client.access_secret_version(
                request={"name": f"{secret.name}/versions/latest"}
            )
            os.environ[secret.name.split("/")[-1]] = value.payload.data.decode("utf-8")
        except FailedPrecondition as err:
            print(f"Failed to grab secret {secret.name}. Exception: {err}")


load_dotenv()
set_gcp_secrets()
set_secrets()
