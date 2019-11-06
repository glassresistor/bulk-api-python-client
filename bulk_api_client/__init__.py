import os
import pandas
import requests
import json
import re
import shutil
import sys
from datetime import datetime, timedelta
from urllib.parse import urljoin
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
    """Determines if an input string is of key=value type

    Args:
        kv_str (str): string to use

    Returns:
        Bool
    """
    return '=' in kv_str


class Client(object):
    app_api_urls = None
    """Dict of Bulk Importer app urls. Updated with the initialization of a
    AppAPI object"""
    model_api_urls = {}
    """Dict of Bulk Importer model urls. Updated with the initialization of a
    ModelAPI object"""

    def __init__(self,
                 token,
                 api_url='https://data-warehouse.pivot/bulk/api/'):
        """API Client object for bulk_importer to handle app and model requests.
        Requies a user token with access to data-warehouse

        Args:
            token (str): user token with permissions to access the API
            api_url (str): base url for api request; defaults to data_warehouse

        """
        self.token = token
        self.api_url = api_url
        self.temp_dir = os.path.join(gettempdir(), 'bulk-api-cache')
        """Temp directory to host files created by the client"""
        os.makedirs(self.temp_dir, exist_ok=True)

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
        """Empty the temp directory for a clean working directory"""

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

    def __init__(self, client, app_label,):
        """App object. Given a app label, this object makes a request — using
        the Client class — to the Bulk Importer API. If given a app in Bulk
        Importer, the response cached in app_api_urls dictionary.

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
            model_name (str): model name of the desired model

        Returns:
            ModelAPI obj

        """
        return ModelAPI(self, model_name)


class ModelAPI(object):

    def __init__(self, app_api, model_name):
        """Model object. Given a model name, this object makes a request — using
        the Client class — to the Bulk Importer API. If given a model in the
        previously specified app, the response is cached in model_api_urls
        dictionary.

        Args:
            app_api (obj): AppAPI object
            model_name (str): model name of the desired model of an app

        """

        self.app = app_api
        self.model_name = model_name.lower()

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

    def query(self, fields=None, filter=None, order=None, page_size=None):
        """Queries to create a Pandas DataFrame for given queryset. The default
        query may be obtained by calling the function, without passing
        any parameters.

        Args:
            fields (list): list of specified fields for the fields query
            filter (str): filter for the filter query
            order (str): order for the ordering query
            page_size (str): page size for the page_size query; Default: 10,000

        Returns:
            pandas dataframe

        """
        dataframes = []
        current_page = 1
        pages_left = 1
        while pages_left > 0:
            df, pages_left = self.query_request(
                fields=fields,
                filter=filter,
                order=order,
                page=current_page,
                page_size=page_size
            )
            current_page += 1
            dataframes.append(df)
        return pandas.concat(dataframes)

    def query_request(self, fields=None, filter=None, order=None, page=None,
                      page_size=None):
        """Queries to create a Pandas DataFrame for given queryset. The default
        query may be obtained by calling the function, without passing
        any parameters.

        Args:
            fields (list): list of specified fields for the fields query
            filter (str): filter for the filter query
            order (str): order for the ordering query
            page (str): page number for the page query; Default: 1
            page_size (str): page size for the page_size query; Default: 10,000

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
        if page is None:
            page = 1
        if page_size is not None and (
                not isinstance(page_size, int) or page_size <= 0):
            raise TypeError({'detail': "page size must be a positive integer"})

        url_path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        url = urljoin(self.app.client.api_url, os.path.join(url_path, 'query'))
        params = {'fields': fields, 'filter': filter,
                  'ordering': order, 'page': page, 'page_size': page_size}
        url_hash = str(hash(url_path) + sys.maxsize + 1)
        path = os.path.join(self.app.client.temp_dir, url_hash)
        os.makedirs(path, exist_ok=True)
        query_hash = "{}.csv".format(
            hash(json.dumps(params, sort_keys=True))
            + sys.maxsize
            + 1
        )
        pageless_params = {'fields': fields, 'filter': filter,
                           'ordering': order, 'page_size': page_size}
        page_hash = "{}.count".format(
            hash(json.dumps(pageless_params, sort_keys=True))
            + sys.maxsize
            + 1
        )
        csv_path = os.path.join(path, query_hash)
        page_path = os.path.join(path, page_hash)
        expiration_time = (datetime.now() - timedelta(hours=2)).timestamp()
        if not os.path.exists(csv_path) or (
                os.path.getmtime(csv_path) < expiration_time):
            with self.app.client.request('GET', url, params=params) as response:
                pages_left = int(response.headers['page_count']) - page
                with open(csv_path, 'wb') as f:
                    shutil.copyfileobj(response.raw, f)
                with open(page_path, 'w') as f:
                    f.write(str(response.headers['page_count']))
        else:
            with open(page_path, 'rb') as f:
                pages_left = int(f.readline()) - page

        df = pandas.concat(
            pandas.read_csv(
                csv_path,
                chunksize=CSV_CHUNKSIZE
            ),
            ignore_index=True)

        return df, pages_left

    def list(self):
        """Lists all model object of a given model; Makes a 'GET' method request
        to the Bulk API

        Args:

        Returns:
            list of dictionary objects of the model data

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        url = urljoin(self.app.client.api_url, path)
        response = self.app.client.request(
            'GET',
            url,
            params={}
        )
        return json.loads(response.content)

    def create(self, obj_data):
        """Creates a model object given it's primary key and new object data;
        Makes a 'POST' method request to the Bulk API

        Args:
            pk (str): primary key of object
            obj_data (dict): new data to create the object with

        Returns:
            dictionary object of the model data

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        url = urljoin(self.app.client.api_url, path)
        data = json.dumps(obj_data)
        kwargs = {
            'data': data,
            'headers': {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        }
        response = self.app.client.request(
            'POST',
            url,
            params={},
            **kwargs
        )
        return json.loads(response.content)

    def get(self, pk):
        """Gets a model object given it's primary key; Makes a 'GET' method
        request to the Bulk API

        Args:
            pk (str): primary key of object

        Returns:
            dictionary object of the model data

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        url = urljoin(self.app.client.api_url, os.path.join(path, pk))
        response = self.app.client.request(
            'GET',
            url,
            params={}
        )
        return json.loads(response.content)

    def update(self, pk, obj_data, patch=True):
        """Updates a model object given it's primary key and new object data;
        Makes a 'PATCH' method request to the Bulk API

        Args:
            pk (str): primary key of object
            obj_data (dict): new data to update the object with
            patch(bool): partial update (default: True)

        Returns:
            success status code (200)

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        url = urljoin(self.app.client.api_url, os.path.join(path, pk))
        data = json.dumps(obj_data)
        kwargs = {
            'data': data,
            'headers': {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        }
        method = 'PATCH'
        if not patch:
            method = 'PUT'
        response = self.app.client.request(
            method,
            url,
            params={},
            **kwargs
        )
        return response.status_code

    def delete(self, pk):
        """Deletes a model object given it's primary key; Makes a 'DELETE'
        method request to the Bulk API

        Args:
            pk (str): primary key of object

        Returns:
            success status code (204)

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        url = urljoin(self.app.client.api_url, os.path.join(path, pk))
        response = self.app.client.request(
            'DELETE',
            url,
            params={}
        )
        return response.status_code
