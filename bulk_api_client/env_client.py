import os

from bulk_api_client.client import Client
from bulk_api_client.exceptions import BulkAPIError

token = os.getenv("BULK_API_TOKEN")
if not token:
    raise BulkAPIError("Environment variable BULK_API_TOKEN was not found.")

api_url = os.getenv("BULK_API_URL")

expiration_time = os.getenv("BULK_API_EXPIRATION_TIME")

if expiration_time:
    expiration_time = int(expiration_time)

env_client = Client(token, api_url=api_url, expiration_time=expiration_time)
