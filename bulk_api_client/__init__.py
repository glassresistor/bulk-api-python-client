import os
import requests
import requests_cache
import json
import yaml

from urllib.parse import urljoin
from tempfile import gettempdir


from bulk_api_client.app import AppAPI
from bulk_api_client.exceptions import BulkAPIError

CERT_PATH = os.path.join(
    os.path.dirname(
        os.path.realpath(__file__)),
    'data-warehouse.pivot.pem')


class Client(object):
    app_api_urls = None
    """Dict of Bulk Importer app urls. Updated with the initialization of a
    AppAPI object"""
    model_api_urls = {}
    """Dict of Bulk Importer model urls. Updated with the initialization of a
    ModelAPI object"""
    app_api_cache = {}
    """
    Dict of AppAPI objects, created via app(), key of app_label
    """

    def __init__(self,
                 token,
                 api_url='https://data-warehouse.pivot/bulk/api/',
                 expiration_time=7200):
        """API Client object for bulk_importer to handle app and model requests.
        Requies a user token with access to data-warehouse

        Args:
            token (str): user token with permissions to access the API
            api_url (str): base url for api request; defaults to data_warehouse
            expiration_time (int): denote time requests expire from cache

        """
        self.token = token
        self.api_url = api_url
        requests_cache.install_cache(
            'bulk-api-cache',
            backend=requests_cache.backends.sqlite.DbCache(
                location=os.path.join(gettempdir(), 'bulk-api-cache')
            ),
            expire_after=expiration_time
        )
        yaml_res = self.request(
            method='GET',
            url=urljoin(self.api_url, 'swagger.yaml'),
            params={},
        )
        self.yaml_data = yaml.safe_load(yaml_res.raw)
        self.definitions = self.yaml_data['definitions']
        self.paths = self.yaml_data['paths']

    def request(self, method, url, params, *args, **kwargs):
        """Request function to construct and send a request. Uses the Requests
        python library

        Args:
            method (str): method for the request
            path (str): path to the resource the client should access
            params (dict): (optional) Dictionary, list of tuples or bytes to
            send in the query string for the Request.

        Returns:
            response obj

        """
        headers = {
            'Authorization': 'Token {}'.format(self.token),
        }
        if kwargs.get('headers'):
            kwargs['headers'] = {**headers, **kwargs['headers']}
        else:
            kwargs['headers'] = headers
        response = requests.request(
            method=method,
            url=url,
            params=params,
            verify=CERT_PATH,
            stream=True,
            **kwargs
        )

        if response.status_code not in [200, 201, 204]:
            raise BulkAPIError(json.loads(response.content))
        return response

    def clear_cache(self):
        """Empty requests cache"""

        requests_cache.clear()

    def app(self, app_label):
        """Creates AppAPI object from a given app label

        Args:
            app_label (str): app label of the desired model of an app

        Returns:
            AppAPI obj

        """
        if app_label not in self.app_api_cache:
            self.app_api_cache[app_label] = AppAPI(self, app_label)
        return self.app_api_cache[app_label]
