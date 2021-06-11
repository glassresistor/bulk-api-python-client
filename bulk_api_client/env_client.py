import os
import sys

from bulk_api_client.client import Client
from bulk_api_client.exceptions import BulkAPIError


def _env_client():
    token = os.getenv("BULK_API_TOKEN")
    if not token:
        raise BulkAPIError("Environment variable BULK_API_TOKEN was not found.")

    api_url = os.getenv("BULK_API_URL")

    expiration_time = os.getenv("BULK_API_EXPIRATION_TIME")

    if expiration_time:
        expiration_time = int(expiration_time)

    env_client = Client(token, api_url=api_url, expiration_time=expiration_time)
    return env_client


def __getattr__(name):
    if name == "env_client":
        return _env_client()

    # Implicit else
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
