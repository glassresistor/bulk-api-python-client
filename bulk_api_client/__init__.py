import os
import pandas
import requests
import json
import re
import shutil

from urllib.parse import urljoin, urlparse
from tempfile import gettempdir


CERT_PATH = os.path.join(
    os.path.dirname(
        os.path.realpath(__file__)),
    'data-warehouse.pivot.pem')

CSV_CHUNKSIZE = 10 ** 6


class BulkAPIError(Exception):
    pass


filter_error = TypeError({
    'detail': "filter must be a string of form field_name=value"})


def is_kv(kv_str):
    return bool(re.fullmatch(r'(^\w+=\w+$)', kv_str))


class Client(object):
    """API Client object for bulk_importer for app and model requests
    """
    app_api_urls = None
    model_api_urls = {}

    def __init__(self,
                 token,
                 api_url='https://data-warehouse.pivot/bulk/pandas_views/'):
        """Base Client class to handle bulk_importer API requests

        Args:
            token (str): user token with permissions to access the API
            api_url (str): base url for api request; defaults to data_warehouse

        """
        self.token = token
        self.api_url = api_url
        self.temp_dir = os.path.join(gettempdir(), 'bulk-api-cache')
        os.makedirs(self.temp_dir, exist_ok=True)

    def request(self, method, url, params, ):
        """Request function to construct and send a request

        Args:
            method (str): method for the request
            path (str): path to the resource the client should access
            params (dict): (optional) Dictionary, list of tuples or bytes to
            send in the query string for the Request.

        Returns:
            response obj

        """
        headers = {'Authorization': 'Token {}'.format(self.token)}
        response = requests.request(
            method,
            url,
            params=params,
            headers=headers,
            verify=CERT_PATH,
            stream=True
        )

        if response.status_code != 200:
            raise BulkAPIError(json.loads(response.content))
        return response

    def clear_cache(self):
        """Empty the temp directory"""

        if os.path.exists(self.temp_dir):
            for path in os.listdir(self.temp_dir):
                path = os.path.join(self.temp_dir, path)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                if os.path.isfile(path):
                    os.unlink(path)

    def app(self, app_label):
        """Creates AppAPI object from a given app label

        Args:
            app_label (str): app label of the desired model of an app

        Returns:
            AppAPI obj

        """
        return AppAPI(self, app_label)


class AppAPI(object):
    """Object to handle calls to APIRootView
    """

    def __init__(self, client, app_label,):
        """App obj to create requests to the bulk_importer API apps

        Args:
            client (obj): client obj for requests
            app_label (str): app label of the desired app

        """
        self.client = client
        self.app_label = app_label

        url = self.client.api_url
        params = {}
        response = self.client.request('GET', url, params)
        if not self.client.app_api_urls:
            self.client.app_api_urls = json.loads(response.content)
        if self.app_label not in self.client.app_api_urls:
            raise BulkAPIError({'app_api':
                                "Application does not exist in bulk api"})

    def model(self, model_name):
        """Creates a ModelAPI object from a given model name

        Args:
            model_name (str): model name of the desired model of an app

        Returns:
            ModelAPI obj

        """
        return ModelAPI(self, model_name)


class ModelAPI(object):
    """Object to handle calls to APIAppView
    """

    def __init__(self, app_api, model_name):
        """Model obj to create requests to the bulk_importer API app models

        Args:
            app_api (obj): AppAPI object
            model_name (str): model name of the desired model of an app

        """

        self.app = app_api
        self.model_name = model_name

        url = self.app.client.app_api_urls[self.app.app_label]
        params = {}
        response = self.app.client.request('GET', url, params)

        if self.app.app_label not in self.app.client.model_api_urls:
            self.app.client.model_api_urls[self.app.app_label] = json.loads(
                response.content)
        if self.model_name not in self.app.client.model_api_urls[
                self.app.app_label]:
            raise BulkAPIError({'model_api':
                                "Model does not exist in bulk api"})

    def query(self, fields=None, filter=None, order=None, page=None,
              page_size=None):
        """Query for bulk_importer PandasView to create a pandas DataFrame

        Args:
            fields (list): list of specified fields for the fields query
            filter (str): filter for the filter query
            order (str): order for the ordering query
            page (str): page number for the page query
            page_size (str): page size for the page_size query

        Returns:
            pandas dataframe

        """

        if fields is not None:
            if not isinstance(fields, list):
                raise TypeError({'detail': "fields arguement must be list"})
            fields = ','.join(fields)
        if filter is not None:
            if not isinstance(filter, str):
                raise filter_error
            q_list = re.split(r"(\||\&)", filter)
            for q_val in q_list:
                if not is_kv(q_val) and not re.fullmatch(r'(^[\||\&]$)', q_val):
                    raise filter_error
        if order is not None:
            if not isinstance(order, str):
                raise TypeError({'detail': "order must be a string"})
        if page is not None and (
                not isinstance(page, int) or page <= 0):
            raise TypeError({'detail': "page must be a positive integer"})
        if page_size is not None and (
                not isinstance(page_size, int) or page_size <= 0):
            raise TypeError({'detail': "page size must be a positive integer"})

        url = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        params = {'fields': fields, 'filter': filter,
                  'ordering': order, 'page': page, 'page_size': page_size}
        path = urlparse(url).path[1:]
        path = os.path.join(self.app.client.temp_dir, path)
        os.makedirs(path, exist_ok=True)
        query_hash = "{}.csv".format(hash(json.dumps(params, sort_keys=True)))
        csv_path = os.path.join(path, query_hash)
        if not os.path.exists(csv_path):
            with self.app.client.request('GET', url, params=params) as response:
                with open(csv_path, 'wb') as f:
                    shutil.copyfileobj(response.raw, f)

        df = pandas.concat(
            pandas.read_csv(
                csv_path,
                chunksize=CSV_CHUNKSIZE
            ),
            ignore_index=True)

        return df
