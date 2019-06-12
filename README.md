# Python client for Bulk Importer API

## Speculative Design

```
from bulk_api_client import Client

client = Client(token, api_url='https://data-warehoust.pivot')
client.app('app_label').model('model_name').query(filter=...,order=...,page=,page_size=,fields=[...])
```
