import os
import pytest
import random
import string
import json
import yaml
from io import BytesIO
from unittest import mock
from pandas import DataFrame, read_csv
from urllib.parse import urljoin
from requests.models import Response

from bulk_api_client import Client, AppAPI, ModelAPI, ModelObj
from bulk_api_client import (requests, CERT_PATH, BulkAPIError, is_kv,
                             requests_cache)


def random_string(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))


@pytest.mark.parametrize("test_input,expected",
                         [("key=value", True), ("test=test", True),
                          ("invalid", False), ("!@#$", False)])
def test_is_kv(test_input, expected):
    """Test is_kv func returns correct bool for if pair is a key:val pair"""
    assert is_kv(test_input) == expected


def test_cert_path():
    assert os.path.isfile(CERT_PATH)


def test_client():
    """Test Client class works as intented"""
    token = random_string()
    url = random_string()
    yaml_data = {'definitions': ['some_definitions'],
                 'paths': ['some_paths']}
    data = BytesIO(yaml.dump(yaml_data).encode())
    response = Response()
    response._content = b''
    response.status_code = 200
    response.raw = data
    with mock.patch.object(requests, 'request', return_value=response):
        test_client = Client(token, api_url=url)
    assert test_client.token == token
    assert test_client.api_url == url


def test_client_request(client):
    """Test Client method uses request library"""
    method = 'GET'
    path = random_string()
    full_path = urljoin(client.api_url, path)
    params = {'teset_param': 1}
    kwargs = {'headers': {'Authorization': 'Token {}'.format(client.token)}}
    response = Response()
    response._content = b''
    response.status_code = 200
    with mock.patch.object(requests, 'request', return_value=response) as fn:
        client.request(method, full_path, params)
        fn.assert_called_with(
            method=method,
            url=full_path,
            params=params,
            verify=CERT_PATH,
            stream=True,
            **kwargs
        )


@pytest.mark.parametrize("status_code,err_msg", [
    (401, {"detail":
           "You do not have permission to perform this action."}),
    (403, {"detail":
           "You do not have permission to perform this action."}),
    (404, {"detail":
           "Not found."}), ])
def test_client_request_errors(client, status_code, err_msg):
    """Test Client request method handles errors as intended"""
    method = 'GET'
    path = random_string()
    full_path = urljoin(client.api_url, path)
    params = {'teset_param': 1}
    kwargs = {'headers': {'Authorization': 'Token {}'.format(client.token)}}
    response = Response()
    response._content = json.dumps(err_msg)
    response.status_code = status_code
    with mock.patch.object(requests, 'request', return_value=response) as fn:
        with pytest.raises(BulkAPIError) as err:
            client.request(method, full_path, params)
        fn.assert_called_with(
            method=method,
            url=full_path,
            params=params,
            verify=CERT_PATH,
            stream=True,
            **kwargs
        )
    assert str(err.value) == str(err_msg)


def test_download_swagger_yaml():
    token = random_string()
    url = random_string()
    yaml_data = {'definitions': ['some_definitions'],
                 'paths': ['some_paths']}
    data = BytesIO(yaml.dump(yaml_data).encode())
    method = 'GET'
    url = 'http://localhost:8000/bulk/api/swagger.yaml'
    params = {}
    kwargs = {'headers': {'Authorization': 'Token {}'.format(token)}}
    response = Response()
    response._content = b''
    response.status_code = 200
    response.raw = data
    with mock.patch.object(requests, 'request', return_value=response) as fn:
        client = Client(token, api_url=url)
        fn.assert_called_with(
            method=method,
            url=url,
            params=params,
            verify=CERT_PATH,
            stream=True,
            **kwargs
        )
    assert client.definitions == yaml_data['definitions']
    assert client.paths == yaml_data['paths']


def test_request_caching(client):
    method = 'GET'
    path = random_string()
    full_path = urljoin(client.api_url, path)
    response = Response()
    response.status_code = 200
    response.from_cache = True
    with mock.patch.object(requests, 'request', return_value=response):
        res = client.request(method, full_path, {})
    res.from_cache = True


def test_client_app_method(client):
    """Test Client class works as intented"""
    test_app_name = random_string()
    with mock.patch.object(AppAPI, '__init__', return_value=None) as fn:
        test_app_obj = client.app(test_app_name)
        fn.assert_called_with(client, test_app_name)
    assert isinstance(test_app_obj, AppAPI)


def test_client_clear_cache(client):
    """Test cache clearing method clears request cache"""
    with mock.patch.object(requests_cache, 'clear') as fn:
        client.clear_cache()
        fn.called = True


def test_app_api(client):
    """Test AppAPI class works as intented"""
    test_app_label = random_string()
    url = client.api_url
    params = {}
    data = {
        'app_label_1': "rgrg",
        'app_label_2': "rgrg",
        'app_label_3': "rgrg",
        test_app_label: url
    }

    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200

    with mock.patch.object(Client, 'request', return_value=response) as fn:
        test_app_obj = AppAPI(client, test_app_label)
    fn.assert_called_with('GET', url, params)
    assert test_app_obj.app_label == test_app_label


def test_app_api_invalid_app(client):
    data = {
        'app_label_1': "rgrg",
        'app_label_2': "rgrg",
        'app_label_3': "rgrg"
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, 'request', return_value=response):
        with pytest.raises(BulkAPIError):
            AppAPI(client, 'invalid_label')


def test_app_api_model_method(app_api):
    """Test AppAPI class works as intented"""

    test_model_name = 'test_model_name'
    with mock.patch.object(ModelAPI, '__init__', return_value=None) as fn:
        test_model_obj = app_api.model(test_model_name)
        fn.assert_called_with(app_api, test_model_name)
    assert isinstance(test_model_obj, ModelAPI)


def test_model_api(app_api):
    """Test ModelAPI class works as intented"""
    test_model_name = random_string()
    path = app_api.client.api_url
    url = urljoin(path, app_api.app_label)
    params = {}
    data = {
        'model_name_1': "rgrg",
        'model_name_2': "rgrg",
        'model_name_3': "rgrg",
        test_model_name.lower(): url,
    }

    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200

    with mock.patch.object(Client, 'request', return_value=response) as fn:
        test_model_obj = ModelAPI(app_api, test_model_name)
    fn.assert_called_with('GET', url, params)
    assert test_model_obj.model_name == test_model_name.lower()


def test_model_api_invalid_model(app_api):
    """Test ModelAPI class init with invalid model name works as intented"""
    data = {
        'model_name_1': "rgrg",
        'model_name_2': "rgrg",
        'model_name_3': "rgrg",
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, 'request', return_value=response):
        with pytest.raises(BulkAPIError):
            ModelAPI(app_api, 'invalid_model')


def test_model_api_query(model_api):
    """Test ModelAPI query_request method works as intented"""

    test_fields = ['id', 'text']
    test_filter = 'key=value|key=value&key=value'
    test_order = 'text'
    test_page = [1, 2]
    test_page_size = 1

    dataframes = [read_csv(BytesIO(b'col1,col2\n1,2')),
                  read_csv(BytesIO(b'col1,col2\n3,4'))]

    with mock.patch.object(ModelAPI, '_query',) as fn:
        fn.side_effect = [(dataframes[0], 1), (dataframes[1], 0)]
        test_model_data_frame = model_api.query(
            fields=test_fields,
            filter=test_filter,
            order=test_order,
            page_size=test_page_size
        )
        fn.assert_called_with(
            fields=test_fields,
            filter=test_filter,
            order=test_order,
            page=test_page.pop(),
            page_size=test_page_size
        )
    assert isinstance(test_model_data_frame, DataFrame)
    assert test_model_data_frame.columns.to_list() == ['col1', 'col2']
    assert test_model_data_frame.values.tolist() == [[1, 2], [3, 4]]
    assert test_model_data_frame.shape == (2, 2)


def test_model_api_query_request(model_api):
    """Test ModelAPI query_request method works as intented"""
    path = model_api.app.client.app_api_urls[model_api.app.app_label]
    url = urljoin(path, os.path.join(model_api.model_name, 'query'))

    test_fields = ['id', 'text']
    test_filter = 'key=value|key=value&key=value'
    test_order = 'text'
    test_page = 1
    test_page_size = 1

    params = {
        'fields': ','.join(test_fields),
        'filter': test_filter,
        'ordering': test_order,
        'page': test_page,
        'page_size': test_page_size
    }

    response = Response()
    response._content = b'col1,col2\n1,2\n3,4'
    response.status_code = 200
    response.headers['page_count'] = '1'
    response.headers['current_page'] = '1'
    response.raw = BytesIO(b'col1,col2\n1,2\n3,4')
    with mock.patch.object(Client, 'request', return_value=response) as fn:
        test_model_data_frame, pages_left = model_api._query(
            fields=test_fields, filter=test_filter, order=test_order,
            page=test_page, page_size=test_page_size)
        fn.assert_called_with('GET', url, params=params)
    assert isinstance(test_model_data_frame, DataFrame)
    assert test_model_data_frame.columns.to_list() == ['col1', 'col2']
    assert test_model_data_frame.values.tolist() == [[1, 2], [3, 4]]
    assert test_model_data_frame.shape == (2, 2)
    assert pages_left == 0


def test_model_api_query_request_null_params(model_api):
    """Test ModelAPI query_request method with null parameters works as intented
    """
    path = model_api.app.client.app_api_urls[model_api.app.app_label]
    url = urljoin(path, os.path.join(model_api.model_name, 'query'))

    params = {
        'fields': None,
        'filter': None,
        'ordering': None,
        'page': 1,
        'page_size': None,
    }

    response = Response()
    response.status_code = 200
    response.headers['page_count'] = '1'
    response.headers['current_page'] = '1'
    response._content = b'col1,col2\n1,2'
    response.raw = BytesIO(b'col1,col2\n1,2')
    with mock.patch.object(Client, 'request',
                           return_value=response) as fn:
        test_model_data_frame, pages_left = model_api._query()
        fn.assert_called_with('GET', url, params=params)
    assert isinstance(test_model_data_frame, DataFrame)
    assert test_model_data_frame.columns.to_list() == ['col1', 'col2']
    assert test_model_data_frame.values.tolist() == [[1, 2]]
    assert test_model_data_frame.shape == (1, 2)
    assert pages_left == 0


@pytest.mark.parametrize("kwarg,val,msg", [
    ("fields", "invalid_field", {
        'detail': "fields arguement must be list"}),
    ("filter", 1, {
        'detail': "filter must be a string of form field_name=value"}),
    ("filter", "invalid", {
        'detail': "filter must be a string of form field_name=value"}),
    ("order", 1, {
        'detail': "order must be a string"}),
    ("page", "invalid_page", {
        'detail': "page must be a positive integer"}),
    ("page", 0, {
        'detail': "page must be a positive integer"}),
    ("page", -1, {
        'detail': "page must be a positive integer"}),
    ("page_size", "invalid_page_size", {
     'detail': "page size must be a positive integer"}),
    ("page_size", 0, {
        'detail': "page size must be a positive integer"}),
    ("page_size", -1, {
        'detail': "page size must be a positive integer"})])
def test_model_api_query_request_invalid_params(model_api, kwarg, val, msg):
    """Test ModelAPI class errors works as intented"""

    params = {
        kwarg: val,
    }
    response = Response()
    response.status_code = 200
    response._content = b'col1,col2\n1,2'
    with mock.patch.object(Client, 'request', return_value=response):
        with pytest.raises(TypeError) as err:
            model_api._query(**params)
    assert str(err.value) == str(msg)


def test_model_api_query_request_regression(model_api):
    """Test ModelAPI query_request regression that fails when making
    multiple query requests
    """

    response = Response()
    response._content = b'col1,col2\n1,2\n3,4'
    response.status_code = 200
    response.headers['page_count'] = '1'
    response.headers['current_page'] = '1'
    response.raw = BytesIO(b'col1,col2\n1,2\n3,4')
    with mock.patch.object(Client, 'request', return_value=response):
        test_model_data_frame, pages_left = model_api._query()
    model_api.app.client.clear_cache()
    with mock.patch.object(Client, 'request', return_value=response):
        test_model_data_frame, pages_left = model_api._query()


def test_model_api_query_request_fresh_cache(model_api):
    """Test ModelAPI query_request caches new file after 2 hours old"""

    path = model_api.app.client.app_api_urls[model_api.app.app_label]
    url = urljoin(path, os.path.join(model_api.model_name, 'query'))
    params = {
        'fields': None,
        'filter': None,
        'ordering': None,
        'page': 1,
        'page_size': None,
    }
    model_api.app.client.clear_cache()
    response = Response()
    response.status_code = 200
    response.headers['page_count'] = '1'
    response.headers['current_page'] = '1'
    response._content = b'col1,col2\n3,4'
    response.raw = BytesIO(b'col1,col2\n3,4')
    with mock.patch.object(Client, 'request',
                           return_value=response) as fn:
        test_model_data_frame, pages_left = model_api._query()
        fn.assert_called_with('GET', url, params=params)
    assert isinstance(test_model_data_frame, DataFrame)
    assert test_model_data_frame.columns.to_list() == ['col1', 'col2']
    assert test_model_data_frame.values.tolist() == [[3, 4]]
    assert test_model_data_frame.shape == (1, 2)
    assert pages_left == 0


def test_model_api_private_list(model_api):
    """Test ModelAPI list method works as intented"""

    path = model_api.app.client.app_api_urls[model_api.app.app_label]
    url = urljoin(path, model_api.model_name)
    content = b'{"count":406,"next":"http://localhost:8000/bulk/api/bulk_'\
        b'importer/examplefortesting/?page=2","previous":null,"results":'\
        b'[{"id": 1016, "created_at": "2019-11-01T19:17:50.415922Z",'\
        b'"updated_at": "2019-11-01T19:17:50.416090Z", "text":'\
        b'"EYdVWVxempVwBpqMENtuYmGZJskLE", "date_time":'\
        b'"2019-11-10T07:28:34.088291Z",'\
        b'"integer": 5, "imported_from": null}]}'
    results = [{'id': 1016, 'created_at': '2019-11-01T19:17:50.415922Z',
                'updated_at': '2019-11-01T19:17:50.416090Z',
                'text': 'EYdVWVxempVwBpqMENtuYmGZJskLE',
                'date_time': '2019-11-10T07:28:34.088291Z',
                'integer': 5, 'imported_from': None}]
    response = Response()
    response.status_code = 200
    response._content = content
    response.headers['page'] = '1'
    with mock.patch.object(Client, 'request', return_value=response) as fn:
        obj_list = model_api._list(page=1)
        fn.assert_called_with('GET', url, params={'page': 1})
    assert obj_list == results


def test_model_api_list(model_api):
    obj_data_list = [{
        'id': 1016,
        'created_at': '2019-11-01T19:17:50.415922Z',
        'updated_at': '2019-11-01T19:17:50.416090Z',
        'text': 'EYdVWVxempVwBpqMENtuYmGZJskLE',
        'date_time': '2019-11-10T07:28:34.088291Z',
        'integer': 5,
        'imported_from': None
    }, {
        'id': 1017,
        'created_at': '2017-07-01T19:17:50.415922Z',
        'updated_at': '2017-07-01T19:17:50.416090Z',
        'text': 'eisjgntignuseitguIUNIUFNE',
        'date_time': '2017-07-10T07:28:34.088291Z',
        'integer': 7,
        'imported_from': None
    }]
    with mock.patch.object(ModelAPI, '_list', return_value=obj_data_list) as fn:
        obj_list = model_api.list(page=1)
        fn.assert_called_with(page=1)
    assert all(isinstance(obj, ModelObj) for obj in obj_list)
    assert all(x.data == y for x, y in zip(obj_list, obj_data_list))


def test_model_api_private_create(model_api):
    """Test ModelAPI private create method with POST request makes a request
    with the correct parameters, and that the data returned is consistent
    """

    path = model_api.app.client.app_api_urls[model_api.app.app_label]
    url = urljoin(path, model_api.model_name)
    content = b'[{"id": 1016, "created_at": "2019-11-01T19:17:50.415922Z",'\
        b'"updated_at": "2019-11-01T19:17:50.416090Z", "text":'\
        b'"EYdVWVxempVwBpqMENtuYmGZJskLE", "date_time":'\
        b'"2019-11-10T07:28:34.088291Z",'\
        b'"integer": 5, "imported_from": null}]'
    obj_data = {
        'text': 'EYdVWVxempVwBpqMENtuYmGZJskLE',
        'date_time': '2019-11-01T19:17:50.416090Z',
        'integer': 5
    }
    data = json.dumps(obj_data)
    kwargs = {
        'data': data,
        'headers': {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    }
    response = Response()
    response.status_code = 200
    response._content = content
    with mock.patch.object(Client, 'request', return_value=response) as fn:
        obj = model_api._create(obj_data)
        fn.assert_called_with('POST', url, params={}, **kwargs)
    assert obj == json.loads(content)


def test_model_api_create(model_api):
    """Test ModelAPI create method with POST request calls the private create
    method correct parameters, and that the data returned is consistent
    """
    obj_data = {
        'text': 'EYdVWVxempVwBpqMENtuYmGZJskLE',
        'date_time': '2019-11-10T07:28:34.088291Z',
        'integer': 5,
    }
    data = {
        'id': 1016,
        'created_at': '2019-11-01T19:17:50.415922Z',
        'updated_at': '2019-11-01T19:17:50.416090Z',
        'text': 'EYdVWVxempVwBpqMENtuYmGZJskLE',
        'date_time': '2019-11-10T07:28:34.088291Z',
        'integer': 5,
        'imported_from': None
    }
    with mock.patch.object(ModelAPI, '_create', return_value=data) as fn:
        obj = model_api.create(obj_data)
        fn.assert_called_with(obj_data)
    assert isinstance(obj, ModelObj)
    assert obj.data == data


def test_model_api_private_get(model_api):
    """Test ModelAPI private get method with GET request makes a request with
    the correct parameters, and that the data returned is consistent"""

    path = model_api.app.client.app_api_urls[model_api.app.app_label]
    uri = '/api/bulk_importer/examplefortesting/1016'
    url = urljoin(path, os.path.join(model_api.model_name, uri))
    content = b'[{"id": 1016, "created_at": "2019-11-01T19:17:50.415922Z",'\
        b'"updated_at": "2019-11-01T19:17:50.416090Z", "text":'\
        b'"EYdVWVxempVwBpqMENtuYmGZJskLE", "date_time":'\
        b'"2019-11-10T07:28:34.088291Z",'\
        b'"integer": 5, "imported_from": null}]'
    response = Response()
    response.status_code = 200
    response._content = content
    with mock.patch.object(Client, 'request', return_value=response) as fn:
        obj = model_api._get(uri)
        fn.assert_called_with('GET', url, params={})
    assert obj == json.loads(content)


def test_model_api_get(model_api):
    """Test ModelAPI get method with POST request calls the private get
    method correct parameters, and that the data returned is consistent
    """
    obj_data = {
        'id': 1016,
        'created_at': '2019-11-01T19:17:50.415922Z',
        'updated_at': '2019-11-01T19:17:50.416090Z',
        'text': 'EYdVWVxempVwBpqMENtuYmGZJskLE',
        'date_time': '2019-11-10T07:28:34.088291Z',
        'integer': 5,
        'imported_from': None
    }
    path = model_api.app.client.model_api_urls[model_api.app.app_label][
        model_api.model_name]
    pk = obj_data['id']
    uri = os.path.join(path, str(pk))
    with mock.patch.object(ModelAPI, '_get', return_value=obj_data) as fn:
        obj = model_api.get(pk)
        fn.assert_called_with(uri)
    assert isinstance(obj, ModelObj)
    assert obj.data == obj_data


def test_model_api_update(model_api):
    """Test ModelAPI update method with PUT request makes a request with the
    correct parameters"""

    path = model_api.app.client.app_api_urls[model_api.app.app_label]
    uri = uri = '/api/bulk_importer/examplefortesting/1016'
    url = urljoin(path, os.path.join(model_api.model_name, uri))
    obj_data = {
        'text': 'EYdVWVxempVwBpqMENtuYmGZJskLE',
        'date_time': '2019-11-01T19:17:50.416090Z',
        'integer': 5
    }
    data = json.dumps(obj_data)
    kwargs = {
        'data': data,
        'headers': {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    }
    content = b'[{"id": 1016, "created_at": "2019-11-01T19:17:50.415922Z",'\
        b'"updated_at": "2019-11-01T19:17:50.416090Z", "text":'\
        b'"EYdVWVxempVwBpqMENtuYmGZJskLE", "date_time":'\
        b'"2019-11-10T07:28:34.088291Z",'\
        b'"integer": 5, "imported_from": null}]'
    response = Response()
    response.status_code = 200
    response._content = content
    with mock.patch.object(Client, 'request', return_value=response) as fn:
        model_api._update(uri, obj_data, patch=False)
        fn.assert_called_with('PUT', url, params={}, **kwargs)


def test_model_api_partial_update(model_api):
    """Test ModelAPI update method with PATCH request makes a request with the
    correct parameters"""

    path = model_api.app.client.app_api_urls[model_api.app.app_label]
    uri = '/api/bulk_importer/examplefortesting/1016'
    url = urljoin(path, os.path.join(model_api.model_name, uri))
    obj_data = {
        'text': 'EYdVWVxempVwBpqMENtuYmGZJskLE',
        'date_time': '2019-11-01T19:17:50.416090Z',
        'integer': 5
    }
    data = json.dumps(obj_data)
    kwargs = {
        'data': data,
        'headers': {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    }
    content = b'[{"id": 1016, "created_at": "2019-11-01T19:17:50.415922Z",'\
        b'"updated_at": "2019-11-01T19:17:50.416090Z", "text":'\
        b'"EYdVWVxempVwBpqMENtuYmGZJskLE", "date_time":'\
        b'"2019-11-10T07:28:34.088291Z",'\
        b'"integer": 5, "imported_from": null}]'
    response = Response()
    response.status_code = 200
    response._content = content
    with mock.patch.object(Client, 'request', return_value=response) as fn:
        model_api._update(
            uri,
            obj_data
        )
        fn.assert_called_with('PATCH', url, params={}, **kwargs)


def test_model_api_delete(model_api):
    """Test ModelAPI list method works as intented"""

    path = model_api.app.client.app_api_urls[model_api.app.app_label]
    uri = '/api/bulk_importer/examplefortesting/1016'
    url = urljoin(path, os.path.join(model_api.model_name, uri))
    response = Response()
    response.status_code = 204
    with mock.patch.object(Client, 'request', return_value=response) as fn:
        model_api._delete(uri)
        fn.assert_called_with('DELETE', url, params={})


@pytest.mark.parametrize("uri,data", [
    ('/api/bulk_importer/examplefortesting/1', None),
    ('/api/bulk_importer/examplefortesting/1',
     {'text': random_string()}),
])
def test_model_obj(model_api, uri, data):
    """Test ModelObj properties are as set when creating an instance"""
    with mock.patch.object(ModelObj, 'get_data',
                           return_value={'id': 1}) as fn_get:
        model_obj = ModelObj(model_api, uri, data)
        assert model_obj.model_api == model_api
        assert model_obj.uri == uri
        assert model_obj._data == data


def test_model_obj_get_data(model_api):
    """Tests the get_data method of the ModelObj class sets the _data property
    when data isn't set on instance creation
    """
    model_data = {'id': 1}
    uri = '/api/bulk_importer/examplefortesting/1'
    with mock.patch.object(ModelObj, 'set_data') as fn_set:
        model_obj = ModelObj(model_api, uri)
        not fn_set.called

    with mock.patch.object(ModelAPI, '_get',
                           return_value=model_data) as fn_get:
        model_obj.get_data()
        fn_get.assert_called_with(model_obj.uri)
    assert model_obj.model_api == model_api
    assert model_obj.uri == uri
    assert model_obj.data == model_data


def test_model_obj_invalid_model(app_api):
    """Test that giving an invalid model as the model_api property of a ModelObj
    instance throws a BulkAPIError
    """
    with pytest.raises(BulkAPIError):
        ModelObj(app_api, '/api/bulk_importer/examplefortesting/1')


def test_model_obj_save(model_api):
    """Tests that calling the save method on the ModelObj class calls the
    _update method on its model_api property with the correct variables (PATCH)
    """
    model_data = {'id': 1}
    model_obj = ModelObj(model_api, uri=random_string(), data=model_data)
    with mock.patch.object(ModelAPI, '_update', return_value=200) as fn:
        model_obj.save()
        fn.assert_called_with(model_obj.uri, model_obj.data, patch=False)


def test_model_obj_invalid_save(model_api):
    """Tests that calling the save method on the ModelObj class calls the
    _update method on its model_api property with a returned status code that is
    not 200 raises a BulkAPIError
    """

    model_data = {'id': 1}
    model_obj = ModelObj(model_api, uri=random_string(), data=model_data)
    response = Response()
    response.status_code = 404
    with mock.patch.object(Client, 'request', return_value=response):
        with pytest.raises(BulkAPIError):
            model_obj.save()


def test_model_obj_update(model_api):
    """Tests that calling the update method on the ModelObj class calls the
    _update method on its model_api property with the correct variables (PUT)
    """
    model_data = {'id': 1, 'text': random_string()}
    model_obj = ModelObj(model_api, uri=random_string(), data=model_data)
    update_data = {'text': random_string()}
    data = {'id': 1, **update_data}
    with mock.patch.object(ModelAPI, '_update', return_value=data) as fn:
        model_obj.update(update_data)
        assert model_obj.data == data
        fn.assert_called_with(model_obj.uri, update_data)


def test_model_obj_invalid_update(model_api):
    """Tests that calling the update method on the ModelObj class calls the
    _update method on its model_api property with a returned status code that is
    not 200 raises a BulkAPIError
    """
    model_data = {'id': 1}
    model_obj = ModelObj(model_api, uri=random_string(), data=model_data)
    response = Response()
    response.status_code = 404
    with mock.patch.object(Client, 'request', return_value=response):
        with pytest.raises(BulkAPIError):
            model_obj.update(model_data)


def test_model_obj_delete(model_api):
    """Tests that calling the delete method on the ModelObj class calls the
    _delete method on its model_api property with the correct variables
    """
    model_data = {'id': 1}
    model_obj = ModelObj(model_api, uri=random_string(), data=model_data)
    with mock.patch.object(ModelAPI, '_delete', return_value=200) as fn:
        model_obj.delete()
        fn.assert_called_with(model_obj.uri)


def test_model_obj_invalid_delete(model_api):
    """Tests that calling the delete method on the ModelObj class calls the
    _delete method on its model_api property with a returned status code that is
    not 200 raises a BulkAPIError
    """
    model_data = {'id': 1}
    model_obj = ModelObj(model_api, uri=random_string(), data=model_data)
    response = Response()
    response.status_code = 404
    with mock.patch.object(Client, 'request', return_value=response):
        with pytest.raises(BulkAPIError):
            model_obj.delete()
