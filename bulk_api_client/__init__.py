import os
import pandas
import requests
import json

from io import BytesIO
from urllib.parse import urljoin


CERT_PATH = os.path.join(
    os.path.dirname(
        os.path.realpath(__file__)),
    'data-warehouse.pivot.pem')


class BulkAPIError(Exception):
    pass


class Client(object):
    """
    """
    app_api_urls = None
    model_api_urls = {}

    def __init__(self, token, api_url='https://data-warehouse.pivot'):
        """Base Client class to handle bulk_importer API requests

        Args:
            token (str): user token with permissions to access the API
            api_url (str): base url for api request; defaults to data_warehouse

        """
        self.token = token
        self.api_url = api_url

    def request(self, method, path, params, ):
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
        return requests.request(
            method,
            urljoin(
                self.api_url,
                path),
            params=params,
            headers=headers,
            verify=CERT_PATH)

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

        path = 'bulk/pandas_views/'
        params = {}
        response = self.client.request('GET', path, params)
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

        path = 'bulk/pandas_views/{}'.format(
            self.app.app_label)
        params = {}
        response = self.app.client.request('GET', path, params)
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
            csv file obj

        """
        path = 'bulk/pandas_views/{}/{}'.format(
            self.app.app_label, self.model_name)
        if fields:
            fields = ','.join(fields)
        params = {'fields': fields, 'filter': filter,
                  'ordering': order, 'page': page, 'page_size': page_size}
        response = self.app.client.request('GET', path, params=params,)
        csv_file = pandas.read_csv(BytesIO(response.content))
        return csv_file
