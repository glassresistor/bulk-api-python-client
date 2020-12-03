import os

from bulk_api_client.client import Client
from bulk_api_client.exceptions import BulkAPIError

token = os.getenv("BULK_API_TOKEN")
if not token:
    raise BulkAPIError("Environment variable BULK_API_TOKEN was not found.")

api_url = os.getenv("BULK_API_URL")

timeout = int(os.getenv("BULK_API_TIMEOUT"))

env_client = Client(token, api_url=api_url, timeout=timeout)
