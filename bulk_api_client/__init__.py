import os
import pandas
import requests
import requests_cache
import json
import re
import yaml

from io import BytesIO
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

    def __str__(self):
        return "AppAPI: {}".format(self.app_label)


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
            df, pages_left = self._query(
                fields=fields,
                filter=filter,
                order=order,
                page=current_page,
                page_size=page_size
            )
            current_page += 1
            dataframes.append(df)
        return pandas.concat(dataframes)

    def _query(self, fields=None, filter=None, order=None, page=None,
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

        with self.app.client.request('GET', url, params=params) as response:
            pages_left = int(response.headers['page_count']) - page

            df = pandas.concat(
                pandas.read_csv(
                    BytesIO(response.content),
                    chunksize=CSV_CHUNKSIZE
                ),
                ignore_index=True
            )

        return df, pages_left

    def _list(self, page):
        """Lists all model object of a given model; Makes a 'GET' method request
        to the Bulk API

        Args:
            page (int): page number of list of model instances
        Returns:
            list of dictionary objects of the model data

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        url = urljoin(self.app.client.api_url, path)
        with requests_cache.disabled():
            response = self.app.client.request(
                'GET',
                url,
                params={'page': page}
            )
        return json.loads(response.content)['results']

    def list(self, page):
        """Makes call to private list method and creates list of ModelObj
        instances from returned model data

        Args:
            page (int): page number of list of model instances
        Returns:
            list of ModelObjs

        """
        data = self._list(page=page)
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        objs = []
        for obj_data in data:
            uri = os.path.join(path, str(obj_data['id']))
            objs.append(ModelObj.with_properties(self, uri, data=obj_data))
        return objs

    def _create(self, obj_data):
        """Creates a model object given it's primary key and new object data;
        Makes a 'POST' method request to the Bulk API

        Args:
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

    def create(self, obj_data):
        """Makes call to private create method and creates ModelObj instance
        from returned model data

        Args:
            obj_data (dict): new data to create the object with
        Returns:
            ModelObj

        """
        data = self._create(obj_data)
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        uri = os.path.join(path, str(data['id']))
        return ModelObj.with_properties(self, uri=uri, data=data)

    def _get(self, uri):
        """Gets a model object given it's primary key; Makes a 'GET' method
        request to the Bulk API

        Args:
            uri (str): identifier of object

        Returns:
            dictionary object of the model data

        """

        url = urljoin(self.app.client.api_url, uri)
        with requests_cache.disabled():
            response = self.app.client.request(
                'GET',
                url,
                params={}
            )
        return json.loads(response.content)

    def get(self, pk):
        """Makes call to private get method and creates a ModelObj instance
        from returned model data

        Args:
            pk (int): primary key of object
        Returns:
            ModelObj

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name]
        uri = os.path.join(path, str(pk))
        data = self._get(uri)
        return ModelObj.with_properties(self, uri, data=data)

    def _update(self, uri, obj_data, patch=True):
        """Updates a model object given it's primary key and new object data;
        Makes a 'PATCH' method request to the Bulk API

        Args:
            uri (str): identifier of object
            obj_data (dict): new data to update the object with
            patch(bool): partial update (default: True)

        Returns:

        """

        url = urljoin(self.app.client.api_url, uri)
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
        if response.status_code == 200:
            return json.loads(response.content)
        raise BulkAPIError(
            {'model_api':
             "update not successful. Status code {}; {}".format(
                 response.status_code, response.content)})

    def _delete(self, uri):
        """Deletes a model object given its primary key; Makes a delete
        method request to the Bulk API

        Args:
            uri (str): identifier of object

        Returns:

        """
        url = urljoin(self.app.client.api_url, uri)
        response = self.app.client.request(
            'DELETE',
            url,
            params={}
        )
        if response.status_code != 204:
            raise BulkAPIError(
                {'model_api':
                 "delete not successful. Status code {}; {}".format(
                     response.status_code, response.content)})

    def __str__(self):
        return "ModelAPI: {}.{}".format(self.app.app_label, self.model_name)


def _get_f(field, properties):
    """Dynamically builds a getter method using a string represnting the name of
     the property and properties of the attribute. Internal getter method used
     by ModelObj with_properties class method.

    Args:
        field (str): ModelAPI its related to
        properties (dict): uri of the resource

    Returns:


    """
    def get_f(cls):
        field_val = cls.data.get(field)
        if properties[field].get('format') == 'uri':
            if hasattr(cls, "_%s" % field):
                return getattr(cls, "_%s" % field)
            app_label, model_name, id = field_val.split('/')[3:]
            model = cls.model_api.app.client.app(app_label).model(model_name)
            related_obj = ModelObj.with_properties(
                model,
                field_val
            )
            return related_obj
        return field_val
    return get_f


def _set_f(field, properties):
    """Dynamically builds a setter method using a string represnting the name of
     the property and properties of the attribute. Internal getter method used
     by ModelObj with_properties class method.

    Args:
        field (str): ModelAPI its related to
        properties (dict): uri of the resource

    Returns:

    """
    def set_f(cls, val):
        if properties[field].get('readOnly', False):
            raise BulkAPIError({'ModelObj':
                                "Cannot set a read only property"})
        if properties[field].get('format') == 'uri':
            if not isinstance(val, ModelObj):
                raise BulkAPIError({'ModelObj':
                                    "New related model must be a _ModelObj"})
            setattr(cls, "_%s" % field, val)
            val = val.uri
        cls.data[field] = val
    return set_f


class ModelObj:
    """
    **DO NOT CALL DIRECTLY**
    Base object which handles mapping local data to api actions. Must call the
    with_properties class method funnction to get properties

    Args:
        model_api (obj): ModelAPI its related to
        uri (str): uri of the resource
        data (dict): property which memoizes _data

    Returns:


    """

    def __init__(self, model_api, uri, data=None):
        self.model_api = model_api
        self.uri = uri
        self.data = data

    def set_data(self, data):
        self._data = data

    def get_data(self):
        if self._data:
            return self._data
        self.data = self.model_api._get(self.uri)
        return self._data

    data = property(get_data, set_data)

    @classmethod
    def with_properties(cls, model_api, uri, data=None):
        """
        Returns an object with proerties of the given model to be modified
        directly and reflected in the database. Mimics objects used by ORMs

        Args:
            model_api (obj): ModelAPI its related to
            uri (str): uri of the resource
            data (dict): property which memoizes _data

        Returns:
            ModelObjWithProperties obj

        """
        if not isinstance(model_api, ModelAPI):
            raise BulkAPIError({'ModelObj':
                                "Given model is not a ModelAPI object"})

        class ModelObjWithProperties(cls):
            pass
        model = '.'.join(
            [model_api.app.app_label, model_api.model_name])
        model_properties = model_api.app.client.definitions[model][
            'properties']
        for field, property_dict in model_properties.items():
            get_f = _get_f(field, model_properties)
            setattr(ModelObjWithProperties, "get_%s" % field, get_f)

            set_f = _set_f(field, model_properties)
            setattr(ModelObjWithProperties, "set_%s" % field, set_f)

            setattr(
                ModelObjWithProperties,
                field,
                property(
                    getattr(ModelObjWithProperties, 'get_%s' % field),
                    getattr(ModelObjWithProperties, 'set_%s' % field)
                )
            )
        return ModelObjWithProperties(model_api, uri, data)

    def save(self):
        """Makes a call to the put update method of the model_api object

        Args:

        Returns:

        """

        self.model_api._update(self.uri, self.data, patch=False)

    def update(self, data):
        """Makes a call to the patch update method of the model_api object

        Args:
            data (dict): data to update the object with

        Returns:

        """
        self.data = self.model_api._update(self.uri, data)

    def delete(self):
        """Makes a call to the delete method of the model_api object

        Args:

        Returns:

        """
        self.model_api._delete(self.uri)

    def __str__(self):
        return "ModelObj: {}".format(self.uri)
