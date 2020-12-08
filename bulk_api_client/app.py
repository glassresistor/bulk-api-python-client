import json

from bulk_api_client.exceptions import BulkAPIError
from .model import ModelAPI


class AppAPI(object):
    """
    Cache of ModelAPI objects, keyed by model_name.
    """

    def __init__(
        self, client, app_label,
    ):
        """App object. Given a app label, this object makes a request — using
        the Client class — to the Bulk Importer API. If given a app in Bulk
        Importer, the response cached in app_api_urls dictionary.

        Args:
            client (obj): client obj for requests
            app_label (str): app label of the desired app

        """
        self.client = client
        self.app_label = app_label
        self.model_api_cache = {}

        url = self.client.api_url
        params = {}
        response = self.client.request("GET", url, params)
        if not self.client.app_api_urls:
            self.client.app_api_urls = json.loads(response.content)
        if self.app_label not in self.client.app_api_urls:
            raise BulkAPIError(
                {"app_api": "Application does not exist in bulk api"}
            )

    def __str__(self):
        return self.app_label

    def model(self, model_name):
        """Creates a ModelAPI object from a given model name

        Args:
            model_name (str): model name of the desired model

        Returns:
            ModelAPI obj

        """
        if model_name not in self.model_api_cache:
            self.model_api_cache[model_name] = ModelAPI(self, model_name)
        return self.model_api_cache[model_name]

    def __str__(self):
        return "AppAPI: {}".format(self.app_label)
